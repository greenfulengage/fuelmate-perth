// ============================================
// FuelMate Perth — Backend Server (Neon Postgres)
// ============================================
const express = require('express');
const https = require('https');
const { parseStringPromise } = require('xml2js');
const path = require('path');
const { Pool } = require('@neondatabase/serverless');

const app = express();
const PORT = process.env.PORT || 3000;

// Neon Postgres — connection string from Vercel env var
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

// Lazy DB init — runs once on first request
let dbReady = false;
app.use(async (req, res, next) => {
  if (!dbReady) {
    try { await initDB(); dbReady = true; } catch(e) { console.error('DB init failed:', e.message); }
  }
  next();
});

// ---- Database Setup ----
async function initDB() {
  const client = await pool.connect();
  try {
    await client.query(`CREATE TABLE IF NOT EXISTS prices (
      id SERIAL PRIMARY KEY,
      date TEXT NOT NULL,
      region TEXT NOT NULL,
      product TEXT NOT NULL,
      station_name TEXT,
      brand TEXT,
      price REAL NOT NULL,
      location TEXT,
      address TEXT,
      latitude REAL,
      longitude REAL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(date, station_name, product)
    )`);
    await client.query(`CREATE TABLE IF NOT EXISTS daily_summary (
      id SERIAL PRIMARY KEY,
      date TEXT NOT NULL,
      region TEXT NOT NULL,
      product TEXT NOT NULL,
      min_price REAL,
      max_price REAL,
      avg_price REAL,
      station_count INTEGER,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(date, region, product)
    )`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(date)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_prices_region ON prices(date, region, product)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_summary_date ON daily_summary(date)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_summary_lookup ON daily_summary(region, product, date)`);
    console.log('DB initialised');
  } finally {
    client.release();
  }
}

// ---- FuelWatch RSS Fetcher ----
function fetchFuelWatch(product, region, day) {
  return new Promise((resolve, reject) => {
    const url = `https://www.fuelwatch.wa.gov.au/fuelwatch/fuelWatchRSS?Product=${product}&Region=${region}&Day=${day}`;
    const req = https.get(url, { timeout: 15000 }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        https.get(res.headers.location, { timeout: 15000 }, (res2) => {
          readResponse(res2, resolve, reject);
        }).on('error', reject);
        return;
      }
      readResponse(res, resolve, reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

function readResponse(res, resolve, reject) {
  let data = '';
  res.on('data', chunk => data += chunk);
  res.on('end', async () => {
    try {
      data = data.replace(/^\uFEFF/, '');
      const result = await parseStringPromise(data, { explicitArray: false });
      const channel = result?.rss?.channel;
      if (!channel?.item) { resolve([]); return; }
      const items = Array.isArray(channel.item) ? channel.item : [channel.item];
      const stations = items.map(item => ({
        name: item['trading-name'] || '',
        brand: item.brand || '',
        price: parseFloat(item.price) || 0,
        location: item.location || '',
        address: item.address || '',
        lat: parseFloat(item.latitude) || 0,
        lng: parseFloat(item.longitude) || 0,
        date: item.date || '',
      })).sort((a, b) => a.price - b.price);
      resolve(stations);
    } catch (e) { reject(e); }
  });
  res.on('error', reject);
}

// ---- Store to Postgres ----
async function storeStations(stations, region, product) {
  if (!stations.length) return 0;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Insert individual station prices
    const insertSQL = `INSERT INTO prices (date, region, product, station_name, brand, price, location, address, latitude, longitude)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      ON CONFLICT (date, station_name, product) DO NOTHING`;

    for (const s of stations) {
      await client.query(insertSQL, [s.date, region, product, s.name, s.brand, s.price, s.location, s.address, s.lat, s.lng]);
    }

    // Upsert daily summary
    const date = stations[0].date;
    const prices = stations.map(s => s.price);
    const min = Math.min(...prices);
    const max = Math.max(...prices);
    const avg = prices.reduce((a, b) => a + b, 0) / prices.length;

    await client.query(
      `INSERT INTO daily_summary (date, region, product, min_price, max_price, avg_price, station_count)
       VALUES ($1,$2,$3,$4,$5,$6,$7)
       ON CONFLICT (date, region, product)
       DO UPDATE SET min_price=$4, max_price=$5, avg_price=$6, station_count=$7`,
      [date, region, product, min, max, avg, prices.length]
    );

    await client.query('COMMIT');
    return stations.length;
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('Store error:', e.message);
    return 0;
  } finally {
    client.release();
  }
}

// ---- Collection Logic ----
const METRO_REGIONS = ['25', '26', '27'];
const PRODUCTS = ['1', '2', '4', '5', '6'];

async function collectAllData() {
  const start = Date.now();
  let totalStored = 0;
  let errors = 0;

  for (const region of METRO_REGIONS) {
    for (const product of PRODUCTS) {
      for (const day of ['today', 'yesterday']) {
        try {
          const stations = await fetchFuelWatch(product, region, day);
          if (stations.length) {
            const stored = await storeStations(stations, region, product);
            totalStored += stored;
          }
          // Small delay to be polite to FuelWatch
          await new Promise(r => setTimeout(r, 300));
        } catch (e) {
          errors++;
        }
      }
    }
  }

  const elapsed = ((Date.now() - start) / 1000).toFixed(1);
  const result = { totalStored, errors, elapsed: elapsed + 's', timestamp: new Date().toISOString() };
  console.log('Collection complete:', result);
  return result;
}

// ---- API Routes ----

// Live prices (proxied from FuelWatch RSS)
app.get('/api/prices', async (req, res) => {
  try {
    const { product = '1', region = '25', day = 'today' } = req.query;
    if (!['today', 'tomorrow', 'yesterday'].includes(day)) {
      return res.status(400).json({ error: 'Day must be today, tomorrow, or yesterday' });
    }
    const stations = await fetchFuelWatch(product, region, day);
    // Opportunistic storage on every request
    if (stations.length && day !== 'tomorrow') {
      storeStations(stations, region, product).catch(() => {});
    }
    res.json({ count: stations.length, region, product, day, stations });
  } catch (e) {
    res.status(500).json({ error: 'Failed to fetch fuel data' });
  }
});

// Historical data
app.get('/api/history', async (req, res) => {
  try {
    const { product = '1', region = '25', days = '30' } = req.query;
    const limit = Math.min(parseInt(days) || 30, 365);
    const result = await pool.query(
      `SELECT date, min_price, max_price, avg_price, station_count
       FROM daily_summary WHERE region = $1 AND product = $2
       ORDER BY date DESC LIMIT $3`,
      [region, product, limit]
    );
    res.json({ region, product, days: limit, history: result.rows.reverse() });
  } catch (e) {
    res.status(500).json({ error: 'Failed to fetch history' });
  }
});

// Stats summary
app.get('/api/stats', async (req, res) => {
  try {
    const { region = '25', product = '1' } = req.query;
    const todayResult = await pool.query(
      `SELECT date, min_price, max_price, avg_price, station_count
       FROM daily_summary WHERE region=$1 AND product=$2 ORDER BY date DESC LIMIT 1`,
      [region, product]
    );
    const weekResult = await pool.query(
      `SELECT AVG(avg_price) as week_avg, MIN(min_price) as week_min
       FROM (SELECT avg_price, min_price FROM daily_summary WHERE region=$1 AND product=$2 ORDER BY date DESC LIMIT 7) sub`,
      [region, product]
    );
    res.json({
      today: todayResult.rows[0] || null,
      weekAvg: weekResult.rows[0] || null
    });
  } catch (e) {
    res.status(500).json({ error: 'Failed to get stats' });
  }
});

// Health check
app.get('/api/health', async (req, res) => {
  try {
    const result = await pool.query('SELECT COUNT(*) as rows FROM daily_summary');
    res.json({
      status: 'ok',
      db: 'neon',
      rows: result.rows[0].rows,
      uptime: process.uptime().toFixed(0) + 's'
    });
  } catch (e) {
    res.json({ status: 'error', db: 'disconnected', error: e.message });
  }
});

// ---- CRON ENDPOINT ----
// Vercel Cron hits this endpoint on schedule
// Protected by CRON_SECRET env var
app.get('/api/cron', async (req, res) => {
  // Verify the request is from Vercel Cron
  const authHeader = req.headers.authorization;
  if (process.env.CRON_SECRET && authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    const result = await collectAllData();
    res.json({ status: 'ok', ...result });
  } catch (e) {
    res.status(500).json({ status: 'error', error: e.message });
  }
});

// ---- Serve Frontend ----
app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ---- Local dev server ----
if (!process.env.VERCEL) {
  (async () => {
    await initDB();
    dbReady = true;
    app.listen(PORT, () => console.log(`FuelMate Perth running on port ${PORT}`));
    setTimeout(collectAllData, 3000);
  })();
}

// Export for Vercel serverless
module.exports = app;

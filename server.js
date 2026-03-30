// ============================================
// FuelMate Perth — Backend Server
// ============================================
// Proxies FuelWatch RSS, stores historical data in SQLite (sql.js),
// and serves the frontend as a static PWA.
//
// sql.js is pure JavaScript — no native compilation needed.
// Works on Replit, Vercel, Railway, Render, anywhere.

const express = require('express');
const https = require('https');
const { parseStringPromise } = require('xml2js');
const path = require('path');
const fs = require('fs');
const initSqlJs = require('sql.js');

const app = express();
const PORT = process.env.PORT || 3000;
const DB_PATH = path.join(__dirname, 'fuelmate.db');

let db = null;

// ---- SQLite Setup ----
async function initDB() {
  const SQL = await initSqlJs();

  if (fs.existsSync(DB_PATH)) {
    const buffer = fs.readFileSync(DB_PATH);
    db = new SQL.Database(buffer);
    console.log('📂 Loaded existing database');
  } else {
    db = new SQL.Database();
    console.log('🆕 Created new database');
  }

  db.run(`CREATE TABLE IF NOT EXISTS prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL, region TEXT NOT NULL, product TEXT NOT NULL,
    station_name TEXT, brand TEXT, price REAL NOT NULL,
    location TEXT, address TEXT, latitude REAL, longitude REAL,
    UNIQUE(date, station_name, product)
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS daily_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL, region TEXT NOT NULL, product TEXT NOT NULL,
    min_price REAL, max_price REAL, avg_price REAL, station_count INTEGER,
    UNIQUE(date, region, product)
  )`);

  db.run(`CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(date)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_summary_date ON daily_summary(date)`);
  saveDB();
}

function saveDB() {
  if (!db) return;
  try {
    const data = db.export();
    fs.writeFileSync(DB_PATH, Buffer.from(data));
  } catch (e) {
    console.error('DB save error:', e.message);
  }
}

// Auto-save every 5 minutes
setInterval(saveDB, 5 * 60 * 1000);

// ---- FuelWatch RSS Fetcher ----
function fetchFuelWatch(product, region, day) {
  return new Promise((resolve, reject) => {
    const url = `https://www.fuelwatch.wa.gov.au/fuelwatch/fuelWatchRSS?Product=${product}&Region=${region}&Day=${day}`;

    const req = https.get(url, { timeout: 12000 }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        https.get(res.headers.location, { timeout: 12000 }, (res2) => {
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
        phone: item.phone || '',
      })).sort((a, b) => a.price - b.price);

      resolve(stations);
    } catch (e) { reject(e); }
  });
  res.on('error', reject);
}

// ---- Store prices ----
function storeStations(stations, region, product) {
  if (!db || !stations.length) return;
  try {
    db.run('BEGIN TRANSACTION');
    for (const s of stations) {
      db.run(
        `INSERT OR IGNORE INTO prices (date,region,product,station_name,brand,price,location,address,latitude,longitude) VALUES (?,?,?,?,?,?,?,?,?,?)`,
        [s.date, region, product, s.name, s.brand, s.price, s.location, s.address, s.lat, s.lng]
      );
    }
    const date = stations[0].date;
    const prices = stations.map(s => s.price);
    const min = Math.min(...prices), max = Math.max(...prices);
    const avg = prices.reduce((a, b) => a + b, 0) / prices.length;
    db.run(
      `INSERT OR REPLACE INTO daily_summary (date,region,product,min_price,max_price,avg_price,station_count) VALUES (?,?,?,?,?,?,?)`,
      [date, region, product, min, max, avg, prices.length]
    );
    db.run('COMMIT');
  } catch (e) {
    try { db.run('ROLLBACK'); } catch (_) {}
    console.error('Store error:', e.message);
  }
}

// ---- Background collection ----
const METRO_REGIONS = ['25', '26', '27'];
const PRODUCTS = ['1', '2', '4', '5', '6'];

async function collectAllData() {
  console.log(`[${new Date().toISOString()}] Collecting FuelWatch data...`);
  let count = 0;
  for (const region of METRO_REGIONS) {
    for (const product of PRODUCTS) {
      for (const day of ['today', 'yesterday']) {
        try {
          const stations = await fetchFuelWatch(product, region, day);
          if (stations.length) { storeStations(stations, region, product); count += stations.length; }
          await new Promise(r => setTimeout(r, 500));
        } catch (e) {
          // Silent fail — will retry next cycle
        }
      }
    }
  }
  console.log(`[${new Date().toISOString()}] Stored ${count} records`);
  saveDB();
}

// ---- API Routes ----

app.get('/api/prices', async (req, res) => {
  try {
    const { product = '1', region = '25', day = 'today' } = req.query;
    if (!['today', 'tomorrow', 'yesterday'].includes(day)) {
      return res.status(400).json({ error: 'Day must be today, tomorrow, or yesterday' });
    }
    const stations = await fetchFuelWatch(product, region, day);
    if (stations.length) { try { storeStations(stations, region, product); } catch (_) {} }
    res.json({ count: stations.length, region, product, day, stations });
  } catch (e) {
    console.error('API /prices:', e.message);
    res.status(500).json({ error: 'Failed to fetch fuel data' });
  }
});

app.get('/api/history', (req, res) => {
  if (!db) return res.json({ history: [] });
  try {
    const { product = '1', region = '25', days = '30' } = req.query;
    const limit = Math.min(parseInt(days) || 30, 365);
    const stmt = db.prepare(`
      SELECT date, min_price, max_price, avg_price, station_count
      FROM daily_summary WHERE region = ? AND product = ?
      ORDER BY date DESC LIMIT ?
    `);
    stmt.bind([region, product, limit]);
    const rows = [];
    while (stmt.step()) rows.push(stmt.getAsObject());
    stmt.free();
    res.json({ region, product, days: limit, history: rows.reverse() });
  } catch (e) {
    res.status(500).json({ error: 'Failed to fetch history' });
  }
});

app.get('/api/stats', (req, res) => {
  if (!db) return res.json({ today: null, weekAvg: null });
  try {
    const { region = '25', product = '1' } = req.query;
    const s1 = db.prepare(`SELECT date,min_price,max_price,avg_price,station_count FROM daily_summary WHERE region=? AND product=? ORDER BY date DESC LIMIT 1`);
    s1.bind([region, product]);
    const today = s1.step() ? s1.getAsObject() : null;
    s1.free();
    const s2 = db.prepare(`SELECT AVG(avg_price) as week_avg, MIN(min_price) as week_min FROM (SELECT avg_price,min_price FROM daily_summary WHERE region=? AND product=? ORDER BY date DESC LIMIT 7)`);
    s2.bind([region, product]);
    const weekAvg = s2.step() ? s2.getAsObject() : null;
    s2.free();
    res.json({ today, weekAvg });
  } catch (e) {
    res.status(500).json({ error: 'Failed to get stats' });
  }
});

app.get('/api/health', (req, res) => {
  const dbSize = fs.existsSync(DB_PATH) ? (fs.statSync(DB_PATH).size / 1024).toFixed(1) + 'KB' : 'none';
  res.json({ status: 'ok', db: dbSize, uptime: process.uptime().toFixed(0) + 's' });
});

// ---- Serve Frontend ----
app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ---- Start ----
async function start() {
  await initDB();
  app.listen(PORT, () => {
    console.log(`🚀 FuelMate Perth running on port ${PORT}`);
  });
  setTimeout(collectAllData, 3000);
  setInterval(collectAllData, 4 * 60 * 60 * 1000);
}

start();

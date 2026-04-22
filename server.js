// KRW Marketing Solutions — Postback Receiver
// Railway-compatible: binds to 0.0.0.0, uses process.env.PORT

const express = require('express');
const { Pool } = require('pg');
require('dotenv').config();

const app  = express();
const PORT = parseInt(process.env.PORT || '3000', 10);

// ── CORS — allow all origins (dashboard runs as local HTML file) ────
app.use(function(req, res, next) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-api-key, Authorization');
  if (req.method === 'OPTIONS') { res.sendStatus(200); return; }
  next();
});

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ── Database ───────────────────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production'
    ? { rejectUnauthorized: false }
    : false,
});

// ── API Key auth ───────────────────────────────────────────────────
function requireKey(req, res, next) {
  const provided = (req.headers['x-api-key'] || req.query.api_key || '').trim();
  const expected = (process.env.API_KEY || '').trim();

  console.log('Auth check — provided:', provided ? provided.slice(0,6)+'...' : 'EMPTY',
              '| expected starts with:', expected ? expected.slice(0,6)+'...' : 'NOT SET');

  if (!expected) {
    console.error('WARNING: API_KEY env var is not set!');
    return res.status(500).json({ error: 'Server misconfigured — API_KEY not set' });
  }
  if (provided !== expected) {
    return res.status(401).json({ error: 'Unauthorized — invalid API key' });
  }
  next();
}

// ── Init DB ────────────────────────────────────────────────────────
async function initDB() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS calls (
        id          SERIAL PRIMARY KEY,
        received_at TIMESTAMPTZ DEFAULT NOW(),
        call_date   TEXT,
        vertical    TEXT,
        buyer       TEXT,
        campaign    TEXT,
        campaign_id TEXT,
        caller_id   TEXT,
        duration    INTEGER DEFAULT 0,
        revenue     NUMERIC(10,2) DEFAULT 0,
        disposition TEXT,
        billable    BOOLEAN DEFAULT true,
        raw         JSONB
      );
    `);
    console.log('DB ready');
  } catch (err) {
    console.error('DB init error:', err.message);
  }
}

// ── ROUTES ─────────────────────────────────────────────────────────

app.get('/', (req, res) => res.json({
  ok: true, service: 'KRW Postback Server', time: new Date().toISOString()
}));

app.get('/health', (req, res) => res.json({
  ok: true, service: 'KRW Postback Server', time: new Date().toISOString()
}));

app.get('/debug', (req, res) => res.json({
  api_key_set: !!process.env.API_KEY,
  api_key_length: (process.env.API_KEY || '').length,
  node_env: process.env.NODE_ENV,
  db_url_set: !!process.env.DATABASE_URL,
}));

app.post('/postback', requireKey, async (req, res) => {
  try {
    const b = req.body;
    const record = {
      call_date:   b.call_date   || b.call_datetime || new Date().toISOString(),
      vertical:    b.vertical    || b.campaign_name || 'Unknown',
      buyer:       b.buyer       || b.buyer_name    || '',
      campaign:    b.campaign    || b.campaign_name || '',
      campaign_id: b.campaign_id || '',
      caller_id:   b.caller_id   || b.phone         || '',
      duration:    parseInt(b.duration || b.call_duration || 0),
      revenue:     parseFloat(b.revenue || b.payout || 0),
      disposition: b.disposition || '',
      billable:    b.billable !== 'false' && b.billable !== false,
      raw:         JSON.stringify(b),
    };
    await pool.query(
      `INSERT INTO calls (call_date,vertical,buyer,campaign,campaign_id,
       caller_id,duration,revenue,disposition,billable,raw)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
      [record.call_date, record.vertical, record.buyer, record.campaign,
       record.campaign_id, record.caller_id, record.duration, record.revenue,
       record.disposition, record.billable, record.raw]
    );
    console.log('Postback:', record.vertical, '$'+record.revenue, record.buyer);
    res.json({ ok: true });
  } catch (err) {
    console.error('Postback error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/summary', requireKey, async (req, res) => {
  try {
    const today      = new Date().toISOString().split('T')[0];
    const weekAgo    = new Date(Date.now() - 7*86400000).toISOString().split('T')[0];
    const monthStart = today.slice(0,7) + '-01';
    const [todayQ, weekQ, monthQ, vertQ] = await Promise.all([
      pool.query(`SELECT vertical, COUNT(*) as calls, COALESCE(SUM(revenue),0) as revenue
                  FROM calls WHERE call_date::date=$1 AND billable=true
                  GROUP BY vertical ORDER BY revenue DESC`, [today]),
      pool.query(`SELECT COALESCE(SUM(revenue),0) as total, COUNT(*) as calls
                  FROM calls WHERE call_date::date>=$1 AND billable=true`, [weekAgo]),
      pool.query(`SELECT COALESCE(SUM(revenue),0) as total, COUNT(*) as calls
                  FROM calls WHERE call_date::date>=$1 AND billable=true`, [monthStart]),
      pool.query(`SELECT vertical, COUNT(*) as total_calls,
                  COALESCE(SUM(revenue),0) as total_revenue
                  FROM calls WHERE billable=true
                  GROUP BY vertical ORDER BY total_revenue DESC`),
    ]);
    res.json({
      ok: true,
      today: {
        date: today,
        by_vertical: todayQ.rows,
        total: todayQ.rows.reduce((s,r) => s + parseFloat(r.revenue||0), 0),
        calls: todayQ.rows.reduce((s,r) => s + parseInt(r.calls||0), 0),
      },
      week:  { total: parseFloat(weekQ.rows[0]?.total||0),  calls: parseInt(weekQ.rows[0]?.calls||0) },
      month: { total: parseFloat(monthQ.rows[0]?.total||0), calls: parseInt(monthQ

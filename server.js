// ============================================================
//  KRW Marketing Solutions — Postback Receiver
//  Receives call data from Zapier (sourced from TrackDrive)
//  Stores in PostgreSQL, serves live data to dashboard
// ============================================================

const express = require('express');
const cors    = require('cors');
const { Pool } = require('pg');
require('dotenv').config();

// ── Startup banner — printed before any async work ────────
console.log('Starting KRW server...');
console.log(`   NODE_ENV     : ${process.env.NODE_ENV || '(not set)'}`);
console.log(`   DATABASE_URL : ${process.env.DATABASE_URL ? 'set' : 'NOT SET — this will cause startup failure'}`);
console.log(`   PORT         : ${process.env.PORT || '8080 (default)'}`);

// ── Startup watchdog — exits with a clear message if the
//    app hasn't finished initialising within 30 seconds ────
const startupTimer = setTimeout(() => {
  console.error('STARTUP TIMEOUT: server did not finish initialising within 30 s');
  console.error('Check DATABASE_URL, network reachability, and the logs above for errors.');
  process.exit(1);
}, 30000);
// Allow Node to exit normally even if this timer is still pending
startupTimer.unref();

const app  = express();
const port = 3000;

// ── Middleware ─────────────────────────────────────────────
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ── Database connection ────────────────────────────────────
console.log(`🔧 DATABASE_URL is ${process.env.DATABASE_URL ? 'set' : 'NOT SET — this will cause startup failure'}`);

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production'
    ? { rejectUnauthorized: false }
    : false,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

// Log any idle-client errors so they surface in Railway logs
// instead of crashing the process silently
pool.on('error', (err) => {
  console.error('Pool error (idle client):', err.message);
  console.error(err.stack);
});

// ── Auth middleware (simple API key) ──────────────────────
function requireKey(req, res, next) {
  const key = req.headers['x-api-key'] || req.query.api_key;
  if (key !== process.env.API_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}

// ── Create table on startup ───────────────────────────────
async function initDB() {
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
  console.log('✅ Database table ready');
}

// ============================================================
//  POST /postback
//  Receives call data from Zapier
//  Protected by API key
// ============================================================
app.post('/postback', requireKey, async (req, res) => {
  try {
    const body = req.body;

    // Map TrackDrive / Zapier fields — adjust field names to
    // match whatever TrackDrive sends you
    const record = {
      call_date:   body.call_date   || body.call_datetime || new Date().toISOString(),
      vertical:    body.vertical    || body.campaign_name || 'Unknown',
      buyer:       body.buyer       || body.buyer_name    || '',
      campaign:    body.campaign    || body.campaign_name || '',
      campaign_id: body.campaign_id || '',
      caller_id:   body.caller_id   || body.phone        || '',
      duration:    parseInt(body.duration || body.call_duration || 0),
      revenue:     parseFloat(body.revenue || body.payout || 0),
      disposition: body.disposition || '',
      billable:    body.billable !== 'false' && body.billable !== false,
      raw:         JSON.stringify(body),
    };

    await pool.query(`
      INSERT INTO calls
        (call_date, vertical, buyer, campaign, campaign_id, caller_id,
         duration, revenue, disposition, billable, raw)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
    `, [
      record.call_date, record.vertical, record.buyer,
      record.campaign,  record.campaign_id, record.caller_id,
      record.duration,  record.revenue, record.disposition,
      record.billable,  record.raw,
    ]);

    console.log(`📞 Postback received: ${record.vertical} | $${record.revenue} | ${record.buyer}`);
    res.json({ ok: true, message: 'Postback recorded' });

  } catch (err) {
    console.error('Postback error:', err.message);
    res.status(500).json({ error: 'Server error', details: err.message });
  }
});

// ============================================================
//  GET /calls
//  Returns recent calls — used by dashboard
//  Query params: limit, vertical, buyer, date
// ============================================================
app.get('/calls', requireKey, async (req, res) => {
  try {
    const { limit = 200, vertical, buyer, date } = req.query;
    let where = [];
    let params = [];
    let i = 1;

    if (vertical) { where.push(`vertical = $${i++}`); params.push(vertical); }
    if (buyer)    { where.push(`buyer = $${i++}`);    params.push(buyer); }
    if (date)     { where.push(`call_date::date = $${i++}`); params.push(date); }

    const whereClause = where.length ? 'WHERE ' + where.join(' AND ') : '';
    params.push(parseInt(limit));

    const result = await pool.query(
      `SELECT * FROM calls ${whereClause} ORDER BY received_at DESC LIMIT $${i}`,
      params
    );
    res.json({ ok: true, count: result.rows.length, calls: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
//  GET /summary
//  Aggregated revenue by vertical — used by live dashboard
// ============================================================
app.get('/summary', requireKey, async (req, res) => {
  try {
    const today = new Date().toISOString().split('T')[0];
    const weekAgo = new Date(Date.now() - 7 * 86400000).toISOString().split('T')[0];
    const monthStart = today.slice(0, 7) + '-01';

    // Today's revenue by vertical
    const todayQ = await pool.query(`
      SELECT vertical,
             COUNT(*) as calls,
             SUM(revenue) as revenue
      FROM calls
      WHERE call_date::date = $1 AND billable = true
      GROUP BY vertical ORDER BY revenue DESC
    `, [today]);

    // Week totals
    const weekQ = await pool.query(`
      SELECT SUM(revenue) as total, COUNT(*) as calls
      FROM calls
      WHERE call_date::date >= $1 AND billable = true
    `, [weekAgo]);

    // Month totals
    const monthQ = await pool.query(`
      SELECT SUM(revenue) as total, COUNT(*) as calls
      FROM calls
      WHERE call_date::date >= $1 AND billable = true
    `, [monthStart]);

    // All-time by vertical
    const verticalQ = await pool.query(`
      SELECT vertical,
             COUNT(*) as total_calls,
             SUM(revenue) as total_revenue,
             AVG(revenue) as avg_revenue
      FROM calls WHERE billable = true
      GROUP BY vertical ORDER BY total_revenue DESC
    `);

    res.json({
      ok: true,
      today: {
        date: today,
        by_vertical: todayQ.rows,
        total: todayQ.rows.reduce((s, r) => s + parseFloat(r.revenue || 0), 0),
        calls: todayQ.rows.reduce((s, r) => s + parseInt(r.calls || 0), 0),
      },
      week: {
        total: parseFloat(weekQ.rows[0]?.total || 0),
        calls: parseInt(weekQ.rows[0]?.calls  || 0),
      },
      month: {
        total: parseFloat(monthQ.rows[0]?.total || 0),
        calls: parseInt(monthQ.rows[0]?.calls  || 0),
      },
      by_vertical: verticalQ.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
//  GET /health  — basic health check, no auth required
// ============================================================
app.get('/health', (req, res) => {
  res.json({ ok: true, service: 'KRW Postback Server', time: new Date().toISOString() });
});

// ── Process-level error handlers ──────────────────────────
process.on('unhandledRejection', (reason, promise) => {
  console.error('🔥 Unhandled promise rejection:', reason);
  console.error('   Promise:', promise);
  process.exit(1);
});

process.on('uncaughtException', (err) => {
  console.error('🔥 Uncaught exception:', err.message);
  console.error(err.stack);
  process.exit(1);
});

// ── Start ──────────────────────────────────────────────────
try {
  console.log('⏳ Calling initDB()...');
  initDB().then(() => {
    console.log('⏳ initDB() resolved — starting HTTP listener...');
    app.listen(port, () => {
      clearTimeout(startupTimer); // disarm the watchdog — we're up
      console.log(`🚀 KRW server running on port ${port}`);
      console.log(`   Postback URL: POST /postback`);
      console.log(`   Summary URL:  GET  /summary`);
    });
  }).catch(err => {
    console.error('❌ Failed to initialise database:', err.message);
    console.error(err.stack);
    process.exit(1);
  });
} catch (err) {
  console.error('❌ Synchronous error during startup:', err.message);
  console.error(err.stack);
  process.exit(1);
}

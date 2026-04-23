// KRW Marketing Solutions — Postback Receiver
// Designed for invoicing: stores all billable call data cleanly
// Every field is preserved for future invoice generation

const express = require('express');
const { Pool } = require('pg');
require('dotenv').config();

const app  = express();
const PORT = parseInt(process.env.PORT || '3000', 10);

// ── CORS ───────────────────────────────────────────────────────────
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
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

// ── Auth ───────────────────────────────────────────────────────────
function requireKey(req, res, next) {
  const provided = (req.headers['x-api-key'] || req.query.api_key || '').trim();
  const expected = (process.env.API_KEY || '').trim();
  if (!expected) return res.status(500).json({ error: 'API_KEY not set' });
  if (provided !== expected) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

// ── Init DB ─────────────────────────────────────────────────────────
// Full schema designed for invoicing — every field stored separately
// so the invoice tab can group, filter, and total by any dimension
async function initDB() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS calls (
        -- Core identity
        id              SERIAL PRIMARY KEY,
        received_at     TIMESTAMPTZ DEFAULT NOW(),   -- when WE got it

        -- Call timing
        call_date       DATE,                        -- date of call
        call_datetime   TIMESTAMPTZ,                 -- full datetime if available
        call_duration   INTEGER DEFAULT 0,           -- seconds

        -- Deal / account info (critical for invoicing)
        vertical        TEXT NOT NULL DEFAULT 'Unknown',  -- e.g. FE Xfers, ACA, MVA
        campaign_name   TEXT,                        -- TrackDrive campaign name
        campaign_id     TEXT,                        -- TrackDrive campaign ID
        buyer_name      TEXT,                        -- who we are billing
        buyer_id        TEXT,                        -- buyer account ID if available
        supplier_name   TEXT,                        -- who supplied the call
        caller_id       TEXT,                        -- phone number of caller

        -- Financial (core invoice fields)
        payout_amount   NUMERIC(10,2) DEFAULT 0,     -- what buyer pays us per call
        revenue         NUMERIC(10,2) DEFAULT 0,     -- alias for payout_amount
        cost            NUMERIC(10,2) DEFAULT 0,     -- what we pay supplier
        profit          NUMERIC(10,2) DEFAULT 0,     -- revenue - cost

        -- Call quality / billing status
        billable        BOOLEAN DEFAULT true,
        disposition     TEXT,                        -- e.g. Converted, No Answer
        call_status     TEXT,                        -- e.g. completed, failed

        -- Invoice tracking (managed by invoice tab later)
        invoice_id      TEXT,                        -- populated when invoice is created
        invoice_status  TEXT DEFAULT 'pending',      -- pending | invoiced | paid | disputed
        invoice_date    DATE,                        -- when invoice was sent
        paid_date       DATE,                        -- when payment was received

        -- Raw data (keep everything for debugging)
        raw             JSONB                        -- full original postback payload
      );

      -- Indexes for fast invoice queries
      CREATE INDEX IF NOT EXISTS idx_calls_call_date   ON calls (call_date);
      CREATE INDEX IF NOT EXISTS idx_calls_vertical    ON calls (vertical);
      CREATE INDEX IF NOT EXISTS idx_calls_buyer       ON calls (buyer_name);
      CREATE INDEX IF NOT EXISTS idx_calls_billable    ON calls (billable);
      CREATE INDEX IF NOT EXISTS idx_calls_inv_status  ON calls (invoice_status);
      CREATE INDEX IF NOT EXISTS idx_calls_received    ON calls (received_at);
    `);

    // Add invoice columns to existing table if upgrading from old schema
    const addCols = [
      `ALTER TABLE calls ADD COLUMN IF NOT EXISTS call_datetime   TIMESTAMPTZ`,
      `ALTER TABLE calls ADD COLUMN IF NOT EXISTS call_duration   INTEGER DEFAULT 0`,
      `ALTER TABLE calls ADD COLUMN IF NOT EXISTS campaign_name   TEXT`,
      `ALTER TABLE calls ADD COLUMN IF NOT EXISTS buyer_name      TEXT`,
      `ALTER TABLE calls ADD COLUMN IF NOT EXISTS buyer_id        TEXT`,
      `ALTER TABLE calls ADD COLUMN IF NOT EXISTS supplier_name   TEXT`,
      `ALTER TABLE calls ADD COLUMN IF NOT EXISTS payout_amount   NUMERIC(10,2) DEFAULT 0`,
      `ALTER TABLE calls ADD COLUMN IF NOT EXISTS cost            NUMERIC(10,2) DEFAULT 0`,
      `ALTER TABLE calls ADD COLUMN IF NOT EXISTS profit          NUMERIC(10,2) DEFAULT 0`,
      `ALTER TABLE calls ADD COLUMN IF NOT EXISTS call_status     TEXT`,
      `ALTER TABLE calls ADD COLUMN IF NOT EXISTS invoice_id      TEXT`,
      `ALTER TABLE calls ADD COLUMN IF NOT EXISTS invoice_status  TEXT DEFAULT 'pending'`,
      `ALTER TABLE calls ADD COLUMN IF NOT EXISTS invoice_date    DATE`,
      `ALTER TABLE calls ADD COLUMN IF NOT EXISTS paid_date       DATE`,
    ];
    for (const sql of addCols) {
      try { await pool.query(sql); } catch(e) { /* column already exists */ }
    }

    console.log('✅ DB schema ready for invoicing');
  } catch (err) {
    console.error('DB init error:', err.message);
  }
}

// ── Field extractor — handles all TrackDrive/Zapier naming variants ─
function field(b, ...keys) {
  for (const k of keys) {
    const variants = [
      k,
      k.toLowerCase(),
      'Querystring ' + k,
      'querystring ' + k,
      k.replace(/_/g, ' '),
      k.replace(/ /g, '_'),
    ];
    for (const v of variants) {
      if (b[v] !== undefined && b[v] !== null && b[v] !== '') {
        return String(b[v]).trim();
      }
    }
  }
  return '';
}

function parseDate(str) {
  if (!str) return null;
  // Handle MM/DD/YY and MM/DD/YYYY formats from TrackDrive
  const parts = str.split('/');
  if (parts.length === 3) {
    let [m, d, y] = parts;
    if (y.length === 2) y = '20' + y;
    return `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`;
  }
  // ISO format fallback
  try { return new Date(str).toISOString().split('T')[0]; } catch(e) { return null; }
}

// ── ROUTES ──────────────────────────────────────────────────────────

app.get('/',       (req, res) => res.json({ ok: true, service: 'KRW Postback Server', time: new Date().toISOString() }));
app.get('/health', (req, res) => res.json({ ok: true, service: 'KRW Postback Server', time: new Date().toISOString() }));
app.get('/debug',  (req, res) => res.json({
  api_key_set:    !!process.env.API_KEY,
  api_key_length: (process.env.API_KEY || '').length,
  node_env:       process.env.NODE_ENV,
  db_url_set:     !!process.env.DATABASE_URL,
}));

// ── POST /postback ──────────────────────────────────────────────────
app.post('/postback', requireKey, async (req, res) => {
  try {
    const b = req.body;
    console.log('Postback received:', JSON.stringify(b));

    // Extract all fields — trying every TrackDrive naming variant
    const rawDate     = field(b, 'Call Date Time', 'call_date', 'call_datetime', 'call_start', 'date');
    const callDate    = parseDate(rawDate);
    const revenue     = parseFloat(field(b, 'Payout Amount', 'payout_amount', 'payout', 'revenue', 'amount') || 0);
    const cost        = parseFloat(field(b, 'cost', 'Cost', 'supplier_cost', 'buy_price') || 0);
    const buyerName   = field(b, 'Buyer Name', 'buyer_name', 'buyer', 'account') || 'Unknown buyer';
    const vertical    = field(b, 'Vertical Campaign Name', 'vertical', 'campaign_name', 'Campaign Name', 'vertical_name', 'Campaign ID', 'campaign_id') || 'Unknown';
    const duration    = parseInt(field(b, 'Duration', 'call_duration', 'duration', 'length') || 0);
    const billable    = field(b, 'Billable Flag', 'billable_flag', 'billable', 'is_billable') !== 'false';

    const record = {
      call_date:      callDate,
      call_datetime:  rawDate ? new Date(rawDate.replace(/(\d+)\/(\d+)\/(\d+)/, '20$3-$1-$2')).toISOString() : new Date().toISOString(),
      call_duration:  duration,
      vertical:       vertical,
      campaign_name:  field(b, 'Vertical Campaign Name', 'campaign_name', 'Campaign Name', 'Campaign ID') || vertical,
      campaign_id:    field(b, 'Campaign ID', 'campaign_id', 'campaign') || '',
      buyer_name:     buyerName,
      buyer_id:       field(b, 'buyer_id', 'Buyer ID', 'account_id') || '',
      supplier_name:  field(b, 'supplier', 'Supplier', 'publisher', 'Publisher', 'supplier_name') || '',
      caller_id:      field(b, 'Caller ID', 'caller_id', 'phone', 'ani', 'caller') || '',
      payout_amount:  revenue,
      revenue:        revenue,
      cost:           cost,
      profit:         revenue - cost,
      billable:       billable,
      disposition:    field(b, 'Disposition', 'disposition', 'call_disposition', 'status') || '',
      call_status:    field(b, 'call_status', 'Call Status', 'status') || '',
      invoice_status: 'pending',
      raw:            JSON.stringify(b),
    };

    await pool.query(`
      INSERT INTO calls (
        call_date, call_datetime, call_duration,
        vertical, campaign_name, campaign_id,
        buyer_name, buyer_id, supplier_name, caller_id,
        payout_amount, revenue, cost, profit,
        billable, disposition, call_status,
        invoice_status, raw
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
        $11,$12,$13,$14,$15,$16,$17,$18,$19
      )`,
      [
        record.call_date, record.call_datetime, record.call_duration,
        record.vertical, record.campaign_name, record.campaign_id,
        record.buyer_name, record.buyer_id, record.supplier_name, record.caller_id,
        record.payout_amount, record.revenue, record.cost, record.profit,
        record.billable, record.disposition, record.call_status,
        record.invoice_status, record.raw,
      ]
    );

    console.log(`✅ Stored: ${record.vertical} | ${record.buyer_name} | $${record.revenue} | ${record.caller_id}`);
    res.json({
      ok: true,
      stored: {
        vertical:   record.vertical,
        buyer:      record.buyer_name,
        revenue:    record.revenue,
        call_date:  record.call_date,
        billable:   record.billable,
      }
    });

  } catch (err) {
    console.error('Postback error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /summary ────────────────────────────────────────────────────
app.get('/summary', requireKey, async (req, res) => {
  try {
    const weekAgo    = new Date(Date.now() - 7*86400000).toISOString().split('T')[0];
    const monthStart = new Date().toISOString().slice(0,7) + '-01';

    const [todayQ, weekQ, monthQ, vertQ, buyerQ] = await Promise.all([
      // Today by vertical
      pool.query(`
        SELECT vertical, buyer_name, COUNT(*) as calls,
               COALESCE(SUM(payout_amount),0) as revenue,
               COALESCE(SUM(profit),0) as profit
        FROM calls WHERE received_at::date=CURRENT_DATE AND billable=true
        GROUP BY vertical, buyer_name ORDER BY revenue DESC
      `),
      // Week totals
      pool.query(`
        SELECT COALESCE(SUM(payout_amount),0) as total,
               COALESCE(SUM(profit),0) as profit,
               COUNT(*) as calls
        FROM calls WHERE received_at::date >= $1 AND billable=true
      `, [weekAgo]),
      // Month totals
      pool.query(`
        SELECT COALESCE(SUM(payout_amount),0) as total,
               COALESCE(SUM(profit),0) as profit,
               COUNT(*) as calls
        FROM calls WHERE received_at::date >= $1 AND billable=true
      `, [monthStart]),
      // All-time by vertical
      pool.query(`
        SELECT vertical, COUNT(*) as total_calls,
               COALESCE(SUM(payout_amount),0) as total_revenue,
               COALESCE(SUM(profit),0) as total_profit
        FROM calls WHERE billable=true
        GROUP BY vertical ORDER BY total_revenue DESC
      `),
      // All-time by buyer (for invoice grouping)
      pool.query(`
        SELECT buyer_name, COUNT(*) as total_calls,
               COALESCE(SUM(payout_amount),0) as total_revenue,
               COUNT(CASE WHEN invoice_status='pending' THEN 1 END) as pending_count,
               COALESCE(SUM(CASE WHEN invoice_status='pending' THEN payout_amount ELSE 0 END),0) as pending_amount
        FROM calls WHERE billable=true
        GROUP BY buyer_name ORDER BY total_revenue DESC
      `),
    ]);

    // Group today's results by vertical only (for chart)
    const todayByVert = {};
    todayQ.rows.forEach(r => {
      if (!todayByVert[r.vertical]) todayByVert[r.vertical] = { vertical: r.vertical, calls: 0, revenue: 0 };
      todayByVert[r.vertical].calls   += parseInt(r.calls);
      todayByVert[r.vertical].revenue += parseFloat(r.revenue);
    });

    res.json({
      ok: true,
      today: {
        date:        new Date().toISOString().split('T')[0],
        by_vertical: Object.values(todayByVert).sort((a,b) => b.revenue - a.revenue),
        by_buyer:    todayQ.rows,
        total:       todayQ.rows.reduce((s,r) => s + parseFloat(r.revenue||0), 0),
        calls:       todayQ.rows.reduce((s,r) => s + parseInt(r.calls||0), 0),
      },
      week: {
        total:  parseFloat(weekQ.rows[0]?.total||0),
        profit: parseFloat(weekQ.rows[0]?.profit||0),
        calls:  parseInt(weekQ.rows[0]?.calls||0),
      },
      month: {
        total:  parseFloat(monthQ.rows[0]?.total||0),
        profit: parseFloat(monthQ.rows[0]?.profit||0),
        calls:  parseInt(monthQ.rows[0]?.calls||0),
      },
      by_vertical: vertQ.rows,
      by_buyer:    buyerQ.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /calls ───────────────────────────────────────────────────────
app.get('/calls', requireKey, async (req, res) => {
  try {
    const { limit=100, vertical, buyer, date, invoice_status } = req.query;
    let where=[], params=[], i=1;
    if (vertical)       { where.push(`vertical=$${i++}`);        params.push(vertical); }
    if (buyer)          { where.push(`buyer_name=$${i++}`);       params.push(buyer); }
    if (date)           { where.push(`call_date=$${i++}`);        params.push(date); }
    if (invoice_status) { where.push(`invoice_status=$${i++}`);   params.push(invoice_status); }
    params.push(parseInt(limit));
    const wc = where.length ? 'WHERE '+where.join(' AND ') : '';
    const r = await pool.query(
      `SELECT * FROM calls ${wc} ORDER BY received_at DESC LIMIT $${i}`, params
    );
    res.json({ ok: true, count: r.rows.length, calls: r.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /invoice-summary ─────────────────────────────────────────────
// Pre-built for the future invoice tab
// Returns all billable calls grouped by buyer, ready for invoice generation
app.get('/invoice-summary', requireKey, async (req, res) => {
  try {
    const { buyer, vertical, from, to, status='pending' } = req.query;
    let where = [`billable=true`];
    let params = [];
    let i = 1;
    if (buyer)    { where.push(`buyer_name=$${i++}`);    params.push(buyer); }
    if (vertical) { where.push(`vertical=$${i++}`);      params.push(vertical); }
    if (from)     { where.push(`call_date>=$${i++}`);    params.push(from); }
    if (to)       { where.push(`call_date<=$${i++}`);    params.push(to); }
    if (status)   { where.push(`invoice_status=$${i++}`);params.push(status); }

    const wc = 'WHERE ' + where.join(' AND ');

    // Summary by buyer
    const byBuyer = await pool.query(`
      SELECT
        buyer_name,
        vertical,
        COUNT(*) as call_count,
        COALESCE(SUM(payout_amount),0) as total_owed,
        COALESCE(SUM(profit),0) as total_profit,
        MIN(call_date) as period_start,
        MAX(call_date) as period_end,
        invoice_status,
        ARRAY_AGG(id ORDER BY call_date) as call_ids
      FROM calls ${wc}
      GROUP BY buyer_name, vertical, invoice_status
      ORDER BY buyer_name, vertical
    `, params);

    // Individual calls for the invoice line items
    const calls = await pool.query(`
      SELECT id, call_date, call_datetime, vertical, campaign_name,
             buyer_name, caller_id, call_duration, payout_amount,
             disposition, invoice_status, invoice_id
      FROM calls ${wc}
      ORDER BY buyer_name, call_date DESC
    `, params);

    res.json({
      ok: true,
      by_buyer: byBuyer.rows,
      calls:    calls.rows,
      totals: {
        call_count:  calls.rows.length,
        total_owed:  calls.rows.reduce((s,r) => s + parseFloat(r.payout_amount||0), 0),
      }
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── PATCH /calls/:id ─────────────────────────────────────────────────
// Update invoice status on a call — used by invoice tab
app.patch('/calls/:id', requireKey, async (req, res) => {
  try {
    const { id } = req.params;
    const { invoice_status, invoice_id, invoice_date, paid_date } = req.body;
    const sets = [];
    const params = [];
    let i = 1;
    if (invoice_status) { sets.push(`invoice_status=$${i++}`); params.push(invoice_status); }
    if (invoice_id)     { sets.push(`invoice_id=$${i++}`);     params.push(invoice_id); }
    if (invoice_date)   { sets.push(`invoice_date=$${i++}`);   params.push(invoice_date); }
    if (paid_date)      { sets.push(`paid_date=$${i++}`);      params.push(paid_date); }
    if (!sets.length) return res.status(400).json({ error: 'Nothing to update' });
    params.push(id);
    await pool.query(`UPDATE calls SET ${sets.join(',')} WHERE id=$${i}`, params);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── START ────────────────────────────────────────────────────────────
initDB().then(() => {
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`KRW server on 0.0.0.0:${PORT}`);
    console.log(`API_KEY set: ${!!process.env.API_KEY}`);
    console.log(`DATABASE_URL set: ${!!process.env.DATABASE_URL}`);
  });
});

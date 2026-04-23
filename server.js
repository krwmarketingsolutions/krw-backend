// KRW Marketing Solutions — Postback Receiver v3
// Invoice-ready schema. Handles existing DB gracefully.

const express = require('express');
const { Pool } = require('pg');
require('dotenv').config();

const app  = express();
const PORT = parseInt(process.env.PORT || '3000', 10);

// ── CORS ──────────────────────────────────────────────────
app.use(function(req, res, next) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PATCH');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-api-key, Authorization');
  if (req.method === 'OPTIONS') { res.sendStatus(200); return; }
  next();
});
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ── Database ───────────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

// ── Auth ───────────────────────────────────────────────────
function requireKey(req, res, next) {
  const provided = (req.headers['x-api-key'] || req.query.api_key || '').trim();
  const expected = (process.env.API_KEY || '').trim();
  if (!expected) return res.status(500).json({ error: 'API_KEY not set' });
  if (provided !== expected) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

// ── Safe column add helper ─────────────────────────────────
async function addCol(col, type) {
  try {
    await pool.query(`ALTER TABLE calls ADD COLUMN IF NOT EXISTS ${col} ${type}`);
  } catch(e) {
    // ignore — column already exists
  }
}

// ── Init DB ────────────────────────────────────────────────
async function initDB() {
  // Step 1: Create table with minimal safe schema
  await pool.query(`
    CREATE TABLE IF NOT EXISTS calls (
      id           SERIAL PRIMARY KEY,
      received_at  TIMESTAMPTZ DEFAULT NOW(),
      vertical     TEXT DEFAULT 'Unknown',
      buyer        TEXT DEFAULT 'Unknown buyer',
      campaign     TEXT,
      campaign_id  TEXT,
      caller_id    TEXT,
      duration     INTEGER DEFAULT 0,
      revenue      NUMERIC(10,2) DEFAULT 0,
      disposition  TEXT,
      billable     BOOLEAN DEFAULT true,
      raw          JSONB
    );
  `);

  // Step 2: Add new invoice columns one at a time — safe if they already exist
  await addCol('call_date',       'DATE');
  await addCol('call_datetime',   'TIMESTAMPTZ');
  await addCol('call_duration',   'INTEGER DEFAULT 0');
  await addCol('campaign_name',   'TEXT');
  await addCol('buyer_name',      'TEXT DEFAULT \'Unknown buyer\'');
  await addCol('buyer_id',        'TEXT');
  await addCol('supplier_name',   'TEXT');
  await addCol('payout_amount',   'NUMERIC(10,2) DEFAULT 0');
  await addCol('cost',            'NUMERIC(10,2) DEFAULT 0');
  await addCol('profit',          'NUMERIC(10,2) DEFAULT 0');
  await addCol('call_status',     'TEXT');
  await addCol('invoice_id',      'TEXT');
  await addCol('invoice_status',  'TEXT DEFAULT \'pending\'');
  await addCol('invoice_date',    'DATE');
  await addCol('paid_date',       'DATE');

  // Step 3: Create indexes (safe — IF NOT EXISTS)
  const indexes = [
    'CREATE INDEX IF NOT EXISTS idx_calls_received   ON calls (received_at)',
    'CREATE INDEX IF NOT EXISTS idx_calls_call_date  ON calls (call_date)',
    'CREATE INDEX IF NOT EXISTS idx_calls_vertical   ON calls (vertical)',
    'CREATE INDEX IF NOT EXISTS idx_calls_buyer_name ON calls (buyer_name)',
    'CREATE INDEX IF NOT EXISTS idx_calls_billable   ON calls (billable)',
    'CREATE INDEX IF NOT EXISTS idx_calls_inv_status ON calls (invoice_status)',
  ];
  for (const idx of indexes) {
    try { await pool.query(idx); } catch(e) {}
  }

  console.log('✅ DB ready');
}

// ── Field extractor ────────────────────────────────────────
function field(b, ...keys) {
  for (const k of keys) {
    const variants = [k, k.toLowerCase(), 'Querystring ' + k,
                      'querystring ' + k, k.replace(/_/g,' '), k.replace(/ /g,'_')];
    for (const v of variants) {
      if (b[v] !== undefined && b[v] !== null && b[v] !== '')
        return String(b[v]).trim();
    }
  }
  return '';
}

function parseDate(str) {
  if (!str) return null;
  // MM/DD/YY or MM/DD/YYYY from TrackDrive
  const slash = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (slash) {
    let [,m,d,y] = slash;
    if (y.length === 2) y = '20' + y;
    return `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`;
  }
  try { return new Date(str).toISOString().split('T')[0]; } catch(e) { return null; }
}

// ── ROUTES ─────────────────────────────────────────────────

app.get('/',       (req, res) => res.json({ ok:true, service:'KRW Postback Server', time:new Date().toISOString() }));
app.get('/health', (req, res) => res.json({ ok:true, service:'KRW Postback Server', time:new Date().toISOString() }));
app.get('/debug',  (req, res) => res.json({
  api_key_set:    !!process.env.API_KEY,
  api_key_length: (process.env.API_KEY||'').length,
  node_env:       process.env.NODE_ENV,
  db_url_set:     !!process.env.DATABASE_URL,
}));

// ── POST /postback ─────────────────────────────────────────
app.post('/postback', requireKey, async (req, res) => {
  try {
    const b = req.body;
    console.log('Postback:', JSON.stringify(b));

    const rawDate    = field(b,'Call Date Time','call_date','call_datetime','call_start','date');
    const callDate   = parseDate(rawDate);
    const revenue    = parseFloat(field(b,'Payout Amount','payout_amount','payout','revenue','amount') || 0);
    const cost       = parseFloat(field(b,'cost','Cost','buy_price') || 0);
    const vertical   = field(b,'Vertical Campaign Name','vertical','campaign_name','Campaign Name','Campaign ID','campaign_id') || 'Unknown';
    const buyerName  = field(b,'Buyer Name','buyer_name','buyer','account') || 'Unknown buyer';
    const duration   = parseInt(field(b,'Duration','call_duration','duration','length') || 0);
    const callerId   = field(b,'Caller ID','caller_id','phone','ani','caller') || '';
    const campaignId = field(b,'Campaign ID','campaign_id','campaign') || '';
    const billable   = field(b,'Billable Flag','billable_flag','billable') !== 'false';
    const disposition= field(b,'Disposition','disposition','call_disposition') || '';

    // Write to both old columns (buyer, duration, revenue) and new columns
    // (buyer_name, call_duration, payout_amount) so everything stays in sync
    await pool.query(`
      INSERT INTO calls (
        call_date, call_duration, vertical, campaign_name, campaign_id,
        buyer, buyer_name, buyer_id, supplier_name, caller_id,
        duration, payout_amount, revenue, cost, profit,
        billable, disposition, invoice_status, raw
      ) VALUES (
        $1,$2,$3,$4,$5,
        $6,$7,$8,$9,$10,
        $11,$12,$13,$14,$15,
        $16,$17,$18,$19
      )`,
      [
        callDate, duration, vertical, vertical, campaignId,
        buyerName, buyerName, '', '', callerId,
        duration, revenue, revenue, cost, revenue - cost,
        billable, disposition, 'pending', JSON.stringify(b)
      ]
    );

    console.log(`✅ ${vertical} | ${buyerName} | $${revenue} | ${callerId}`);
    res.json({ ok:true, stored:{ vertical, buyer:buyerName, revenue, call_date:callDate, billable } });

  } catch (err) {
    console.error('Postback error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /summary ───────────────────────────────────────────
app.get('/summary', requireKey, async (req, res) => {
  try {
    const weekAgo    = new Date(Date.now()-7*86400000).toISOString().split('T')[0];
    const monthStart = new Date().toISOString().slice(0,7)+'-01';

    const [todayQ, weekQ, monthQ, vertQ, buyerQ] = await Promise.all([
      pool.query(`SELECT COALESCE(vertical,'Unknown') as vertical,
                  COALESCE(buyer_name,buyer,'Unknown buyer') as buyer_name,
                  COUNT(*) as calls, COALESCE(SUM(COALESCE(payout_amount,revenue)),0) as revenue
                  FROM calls WHERE received_at::date=CURRENT_DATE AND billable=true
                  GROUP BY vertical,buyer_name,buyer ORDER BY revenue DESC`),
      pool.query(`SELECT COALESCE(SUM(COALESCE(payout_amount,revenue)),0) as total,
                  COUNT(*) as calls FROM calls
                  WHERE received_at::date>=$1 AND billable=true`,[weekAgo]),
      pool.query(`SELECT COALESCE(SUM(COALESCE(payout_amount,revenue)),0) as total,
                  COUNT(*) as calls FROM calls
                  WHERE received_at::date>=$1 AND billable=true`,[monthStart]),
      pool.query(`SELECT COALESCE(vertical,'Unknown') as vertical,
                  COUNT(*) as total_calls,
                  COALESCE(SUM(COALESCE(payout_amount,revenue)),0) as total_revenue
                  FROM calls WHERE billable=true
                  GROUP BY vertical ORDER BY total_revenue DESC`),
      pool.query(`SELECT COALESCE(buyer_name,buyer,'Unknown buyer') as buyer_name,
                  COUNT(*) as total_calls,
                  COALESCE(SUM(COALESCE(payout_amount,revenue)),0) as total_revenue,
                  COUNT(CASE WHEN invoice_status='pending' OR invoice_status IS NULL THEN 1 END) as pending_count,
                  COALESCE(SUM(CASE WHEN invoice_status='pending' OR invoice_status IS NULL
                    THEN COALESCE(payout_amount,revenue) ELSE 0 END),0) as pending_amount
                  FROM calls WHERE billable=true GROUP BY buyer_name,buyer ORDER BY total_revenue DESC`),
    ]);

    const todayByVert = {};
    todayQ.rows.forEach(r => {
      if (!todayByVert[r.vertical]) todayByVert[r.vertical] = {vertical:r.vertical,calls:0,revenue:0};
      todayByVert[r.vertical].calls   += parseInt(r.calls);
      todayByVert[r.vertical].revenue += parseFloat(r.revenue);
    });

    res.json({
      ok: true,
      today: {
        date:        new Date().toISOString().split('T')[0],
        by_vertical: Object.values(todayByVert).sort((a,b)=>b.revenue-a.revenue),
        by_buyer:    todayQ.rows,
        total:       todayQ.rows.reduce((s,r)=>s+parseFloat(r.revenue||0),0),
        calls:       todayQ.rows.reduce((s,r)=>s+parseInt(r.calls||0),0),
      },
      week:        { total:parseFloat(weekQ.rows[0]?.total||0),  calls:parseInt(weekQ.rows[0]?.calls||0) },
      month:       { total:parseFloat(monthQ.rows[0]?.total||0), calls:parseInt(monthQ.rows[0]?.calls||0) },
      by_vertical: vertQ.rows,
      by_buyer:    buyerQ.rows,
    });
  } catch(err) {
    console.error('Summary error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /calls ─────────────────────────────────────────────
app.get('/calls', requireKey, async (req, res) => {
  try {
    const { limit=100, vertical, buyer, date, invoice_status } = req.query;
    let where=[], params=[], i=1;
    if (vertical)       { where.push(`vertical=$${i++}`);            params.push(vertical); }
    if (buyer)          { where.push(`COALESCE(buyer_name,buyer)=$${i++}`); params.push(buyer); }
    if (date)           { where.push(`received_at::date=$${i++}`);   params.push(date); }
    if (invoice_status) { where.push(`invoice_status=$${i++}`);      params.push(invoice_status); }
    params.push(parseInt(limit));
    const wc = where.length ? 'WHERE '+where.join(' AND ') : '';
    const r = await pool.query(
      `SELECT *, COALESCE(buyer_name,buyer,'Unknown buyer') as buyer_display,
              COALESCE(payout_amount,revenue,0) as payout_display
       FROM calls ${wc} ORDER BY received_at DESC LIMIT $${i}`, params
    );
    res.json({ ok:true, count:r.rows.length, calls:r.rows });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /invoice-summary ───────────────────────────────────
app.get('/invoice-summary', requireKey, async (req, res) => {
  try {
    const { buyer, vertical, from, to } = req.query;
    let where = ['billable=true', "(invoice_status='pending' OR invoice_status IS NULL)"];
    let params = [], i = 1;
    if (buyer)    { where.push(`COALESCE(buyer_name,buyer)=$${i++}`); params.push(buyer); }
    if (vertical) { where.push(`vertical=$${i++}`);                   params.push(vertical); }
    if (from)     { where.push(`received_at::date>=$${i++}`);         params.push(from); }
    if (to)       { where.push(`received_at::date<=$${i++}`);         params.push(to); }
    const wc = 'WHERE ' + where.join(' AND ');

    const [byBuyer, calls] = await Promise.all([
      pool.query(`SELECT COALESCE(buyer_name,buyer,'Unknown') as buyer_name, vertical,
                  COUNT(*) as call_count, COALESCE(SUM(COALESCE(payout_amount,revenue)),0) as total_owed,
                  MIN(received_at::date) as period_start, MAX(received_at::date) as period_end
                  FROM calls ${wc} GROUP BY buyer_name, vertical ORDER BY buyer_name, vertical`, params),
      pool.query(`SELECT id, received_at::date as call_date, vertical,
                  COALESCE(buyer_name,buyer,'Unknown') as buyer_name,
                  caller_id, duration, COALESCE(payout_amount,revenue,0) as payout_amount,
                  disposition, invoice_status, invoice_id
                  FROM calls ${wc} ORDER BY buyer_name, received_at DESC`, params),
    ]);

    res.json({
      ok: true,
      by_buyer: byBuyer.rows,
      calls:    calls.rows,
      totals: {
        call_count: calls.rows.length,
        total_owed: calls.rows.reduce((s,r)=>s+parseFloat(r.payout_amount||0),0),
      }
    });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// ── PATCH /calls/:id ───────────────────────────────────────
app.patch('/calls/:id', requireKey, async (req, res) => {
  try {
    const { id } = req.params;
    const { invoice_status, invoice_id, invoice_date, paid_date } = req.body;
    const sets=[], params=[];
    let i=1;
    if (invoice_status) { sets.push(`invoice_status=$${i++}`); params.push(invoice_status); }
    if (invoice_id)     { sets.push(`invoice_id=$${i++}`);     params.push(invoice_id); }
    if (invoice_date)   { sets.push(`invoice_date=$${i++}`);   params.push(invoice_date); }
    if (paid_date)      { sets.push(`paid_date=$${i++}`);      params.push(paid_date); }
    if (!sets.length) return res.status(400).json({ error: 'Nothing to update' });
    params.push(id);
    await pool.query(`UPDATE calls SET ${sets.join(',')} WHERE id=$${i}`, params);
    res.json({ ok: true });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// ── START ──────────────────────────────────────────────────
initDB()
  .then(() => {
    app.listen(PORT, '0.0.0.0', () => {
      console.log(`KRW server on 0.0.0.0:${PORT}`);
      console.log(`API_KEY set: ${!!process.env.API_KEY}`);
      console.log(`DB set: ${!!process.env.DATABASE_URL}`);
    });
  })
  .catch(err => {
    console.error('Failed to start:', err.message);
    process.exit(1);
  });

import http from 'k6/http';
import { check } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL        || 'http://127.0.0.1:8000';
const SEED_RECORDS = parseInt(__ENV.SEED_RECORDS    || '10000');
const SEED_SPAN_MS = parseInt(__ENV.SEED_SPAN_MS    || '120000'); // simulated history window
const READ_RECENCY_MS = parseInt(__ENV.READ_RECENCY_MS || '5000');   // mean age for point reads
const SCAN_HOT_MS = parseInt(__ENV.SCAN_HOT_MS     || '60000');  // hot scan window 
const SCAN_WARM_MS = parseInt(__ENV.SCAN_WARM_MS    || '120000'); // warm scan window 
const COLD_SCAN_RATIO = parseFloat(__ENV.COLD_SCAN_RATIO || '0.20'); // fraction of cold scans

// Value padding — 256 bytes per record
const VALUE_PAD = 'x'.repeat(256);

// Workload mix — append-heavy
const W_PUT  = 0.65;
const W_SCAN = 0.25;
// W_GET = 0.10

const appendLatency = new Trend('op_append_ms',    true);
const getLatency = new Trend('op_get_ms',       true);
const scanHotLatency = new Trend('op_scan_hot_ms',  true);
const scanWarmLatency = new Trend('op_scan_warm_ms', true);
const scanColdLatency = new Trend('op_scan_cold_ms', true);
const scanItems = new Trend('scan_items_returned');
const opErrors = new Counter('op_errors');

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(50)', 'p(90)', 'p(95)', 'p(99)'],
  stages: [
    { duration: '30s', target: 20 },  // ramp up
    { duration: '3m',  target: 20 },  // baseline
    { duration: '30s', target: 40 },  // step up
    { duration: '3m',  target: 40 },  // sustained load
    { duration: '30s', target: 0  },  // ramp down
  ],
  thresholds: {
    op_append_ms: ['p(95)<50',   'p(99)<200'  ],
    op_get_ms: ['p(95)<100',  'p(99)<500'  ],
    op_scan_hot_ms: ['p(95)<500',  'p(99)<2000' ],
    op_scan_warm_ms: ['p(95)<1000', 'p(99)<4000' ],
    op_errors: ['count<50'],
  },
};

// Key format:
// ts:{epoch_ms:014d}:{vu_id:03d}
//
// 14-digit ms timestamp guarantees lex order = time order for all practical
// purposes (covers timestamps until year 33658).
// VU suffix so that multiple VUs writing within the same ms doesn't cause a collision.
//
// Scan bounds use fmtBound (no VU suffix). Because the colon separator makes
// fmtBound(T) < fmtKey(T, 0) < fmtKey(T, 999) lexicographically, a range
// [fmtBound(start), fmtBound(end)) naturally spans all VU keys in that window.

const JSON_HEADERS = { 'Content-Type': 'application/json' };

function fmtKey(tsMs, vuId) {
  return `ts:${String(tsMs).padStart(14, '0')}:${String(vuId).padStart(3, '0')}`;
}

function fmtBound(tsMs) {
  return `ts:${String(tsMs).padStart(14, '0')}`;
}

function expSample(mean) {
  return Math.floor(-mean * Math.log(Math.random()));
}

function pickOp() {
  const r = Math.random();
  if (r < W_PUT) return 'put';
  if (r < W_PUT + W_SCAN) return 'scan';
  return 'get';
}

// Seed historical data
// Writes SEED_RECORDS records spanning [now - SEED_SPAN_MS, now] using VU id 0.
// It simulates a pre-existing timeseries store with 2 minutes of history.
// Returns the seeded time range for VUs to use as scan and read targets.
export function setup() {
  const seedEndMs = Date.now();
  const seedStartMs = seedEndMs - SEED_SPAN_MS;
  const intervalMs = SEED_SPAN_MS / SEED_RECORDS;
  const batchSz = 100;

  for (let i = 0; i < SEED_RECORDS; i += batchSz) {
    const reqs = [];
    for (let j = i; j < Math.min(i + batchSz, SEED_RECORDS); j++) {
      const tsMs = Math.floor(seedStartMs + j * intervalMs);
      reqs.push([
        'PUT', `${BASE_URL}/kv`,
        JSON.stringify({ key: fmtKey(tsMs, 0), value: { type: 'string', value: `${VALUE_PAD}seed-${j}` } }),
        { headers: JSON_HEADERS },
      ]);
    }
    http.batch(reqs);
  }

  console.log(
    `Seeded ${SEED_RECORDS} records: ` +
    `${new Date(seedStartMs).toISOString()} → ${new Date(seedEndMs).toISOString()}`
  );
  return { seedStartMs, seedEndMs };
}

// Main workload 
export default function (data) {
  const { seedStartMs, seedEndMs } = data;
  const op = pickOp();

  if (op === 'put') {
    doAppend();
  } else if (op === 'get') {
    doGet(seedStartMs);
  } else {
    doScan(seedStartMs, seedEndMs);
  }
}

// Always write a fresh key at the current wall-clock timestamp.
// Keys written here form the "hot" region that hot scans and FIFO trimming target.
function doAppend() {
  const key = fmtKey(Date.now(), __VU);
  const res = http.put(
    `${BASE_URL}/kv`,
    JSON.stringify({ key, value: { type: 'string', value: `${VALUE_PAD}vu${__VU}-${__ITER}` } }),
    { headers: JSON_HEADERS },
  );
  const ok = check(res, { 'append 200': (r) => r.status === 200 });
  if (!ok) opErrors.add(1);
  appendLatency.add(res.timings.duration);
}

// Recency-biased point read into the seed range. Using reconstructed seed keys
// guarantees the key exists — avoids 404s from cross-VU coordination gaps.
// Exponential recency: median read targets a key ~READ_RECENCY_MS before seedEnd.
function doGet(seedStartMs) {
  const intervalMs = SEED_SPAN_MS / SEED_RECORDS;
  const ageMs = Math.min(expSample(READ_RECENCY_MS), SEED_SPAN_MS - intervalMs);
  const seedIdx = Math.max(0, SEED_RECORDS - 1 - Math.floor(ageMs / intervalMs));
  const tsMs = Math.floor(seedStartMs + seedIdx * intervalMs);

  const res = http.get(`${BASE_URL}/kv?key=${encodeURIComponent(fmtKey(tsMs, 0))}`);
  const ok  = check(res, { 'get 200': (r) => r.status === 200 });
  if (!ok) {
    opErrors.add(1);
  }
  getLatency.add(res.timings.duration);
}

// Three scan tiers targeting distinct regions of the time axis:
//   cold - oldest seed data
//   hot - trailing edge of live writes
//   warm - random window across seed history and recent VU writes
function doScan(seedStartMs, seedEndMs) {
  const now = Date.now();
  let startMs, endMs, tier;

  if (Math.random() < COLD_SCAN_RATIO) {
    endMs = seedStartMs + Math.floor((seedEndMs - seedStartMs) / 4);
    startMs = seedStartMs;
    tier = 'cold';

  } else if (Math.random() < 0.5) {
    // Hot: trailing SCAN_HOT_MS of wall time. Covers keys currently being
    // written by VUs
    endMs = now;
    startMs = now - SCAN_HOT_MS;
    tier = 'hot';

  } else {
    // Warm: random SCAN_WARM_MS window uniformly spread across
    // [seedStartMs + SCAN_WARM_MS, now]. Covers both historical seed data
    // and recently written VU keys.
    const span = Math.max(0, now - seedStartMs - SCAN_WARM_MS);
    endMs = seedStartMs + SCAN_WARM_MS + Math.floor(Math.random() * span);
    startMs = endMs - SCAN_WARM_MS;
    tier = 'warm';
  }

  const res = http.get(
    `${BASE_URL}/kv/scan` +
    `?start=${encodeURIComponent(fmtBound(startMs))}` +
    `&end=${encodeURIComponent(fmtBound(endMs))}`,
  );

  const ok = check(res, { 'scan 200': (r) => r.status === 200 });
  if (!ok) {
    opErrors.add(1);
    return;
  }

  if (tier === 'hot')  scanHotLatency.add(res.timings.duration);
  else if (tier === 'warm') scanWarmLatency.add(res.timings.duration);
  else scanColdLatency.add(res.timings.duration);

  try {
    const body = res.json();
    scanItems.add(Array.isArray(body) ? body.length : 0);
  } catch (_) {}
}

export function handleSummary(data) {
  const ts = new Date().toISOString().slice(0, 19).replace('T', '-').replace(/:/g, '');
  const file = `k6-summary-fifo-${ts}.json`;

  function val(metric, key) {
    const m = data.metrics[metric];
    return (m && m.values && m.values[key] != null) ? m.values[key] : null;
  }

  function thresholdStr(metric) {
    const m = data.metrics[metric];
    if (!m || !m.thresholds) return '';
    return Object.entries(m.thresholds)
      .map(([expr, r]) => `${r.ok ? 'PASS' : 'FAIL'} ${expr}`)
      .join('  ');
  }

  function trendRow(metric, unit) {
    const p50 = val(metric, 'p(50)');
    const p95 = val(metric, 'p(95)');
    const p99 = val(metric, 'p(99)');
    const n = val(metric, 'count');
    if (p50 === null) return `  ${metric}: no data`;
    return `  ${metric.padEnd(20)}  p50=${p50.toFixed(1)}${unit}  p95=${p95.toFixed(1)}${unit}  p99=${p99.toFixed(1)}${unit}  n=${n}  ${thresholdStr(metric)}`;
  }

  const errCount = val('op_errors', 'count') || 0;
  const siAvg = val('scan_items_returned', 'avg');
  const siP50 = val('scan_items_returned', 'p(50)');

  const lines = [
    '',
    'Appends (always new key, monotonically increasing):',
    trendRow('op_append_ms', ' ms'),
    '',
    'Point reads (recency-biased, seed range):',
    trendRow('op_get_ms', ' ms'),
    '',
    'Scans by tier:',
    trendRow('op_scan_hot_ms',  ' ms'),
    trendRow('op_scan_warm_ms', ' ms'),
    trendRow('op_scan_cold_ms', ' ms'),
    siAvg !== null
      ? `  scan_items_returned      avg=${siAvg.toFixed(1)}  p50=${siP50.toFixed(1)}`
      : '',
    '',
    `Errors: ${errCount}  ${thresholdStr('op_errors')}`,
    '',
    `Full metrics written to: ${file}`,
    '',
  ];

  return {
    [file]: JSON.stringify(data, null, 2),
    stdout: lines.join('\n'),
  };
}

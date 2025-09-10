// server.js
// Node 18+ required. Starts an HTTP API to run pixel-send jobs in parallel.

import express from 'express';
import fs from 'node:fs/promises';
import fsSync from 'node:fs';
import { parse } from 'csv-parse/sync';
import crypto from 'node:crypto';
import path from 'node:path';

// Default config
const DEFAULT_ROUTE_URL = 'http://91.210.164.25:3001/api/send';
const DEFAULTS = {
  chunkSize: 5,
  reqDelay: 100,
  chunkDelay: 500,
  timeout: 30_000,
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function mapStatus(statusDefault) {
  const map = { lead: 'lead', sale: 'sale', rejected: 'install' };
  return map[String(statusDefault ?? '').trim()] ?? 'unknown';
}

function buildUrl(baseUrl, pixel, row) {
  const userAgent = encodeURIComponent(String(row['User Agent'] ?? ''));
  const fbclid = row['Sub ID 7'];
  const ip = row['IP'] ?? '';
  const subid = row['Subid'] ?? '';
  const country = row['\u0424\u043b\u0430\u0433 \u0441\u0442\u0440\u0430\u043d\u044b'] ?? row['Флаг страны'] ?? '';
  const status = mapStatus(row['\u0421\u0442\u0430\u0442\u0443\u0441'] ?? row['Статус']);

  const url =
    `${baseUrl}?pixel=${encodeURIComponent(String(pixel))}` +
    `&fbclid=${fbclid}` +
    `&ip=${ip}` +
    `&subid=${subid}` +
    `&user_agent=${userAgent}` +
    `&status=${status}` +
    `&country=${country}`;
  return url;
}

async function sendRequest({ baseUrl, pixel, row, index, reqDelay, timeout, progress, jobId }) {
  try {
    const url = buildUrl(baseUrl, pixel, row);
    // Delay between requests
    await sleep(reqDelay);

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);

    try {
      const response = await fetch(url, { method: 'GET', signal: controller.signal });
      clearTimeout(timeoutId);

      if (response.ok) {
        progress.sent += 1;
        progress.logs.push({ level: 'info', msg: `OK ${progress.sent}/${progress.total}`, url, status: response.status });
        // Increment per-type counters (lead/sale)
        try {
          const statusKind = mapStatus(row['\u0421\u0442\u0430\u0442\u0443\u0441'] ?? row['Статус']);
          if (statusKind === 'lead') progress.leads += 1;
          else if (statusKind === 'sale') progress.sales += 1;
        } catch {}
        // Single-line success log for each send
        console.log(`[job ${jobId}] OK ${progress.sent}/${progress.total} | HTTP ${response.status} | idx ${index} | ${url}`);
        return index;
      } else {
        let content = '';
        try { content = await response.text(); } catch {}
        progress.errors += 1;
        progress.logs.push({ level: 'error', msg: `HTTP ${response.status}`, url, content });
        // Log only errors to console with compact, single-line output
        console.error(`[job ${jobId}] HTTP ${response.status} | ${url} | ${content?.slice(0,200) ?? ''}`);
        return null;
      }
    } catch (err) {
      progress.errors += 1;
      progress.logs.push({ level: 'error', msg: `Fetch error: ${String(err)}` });
      console.error(`[job ${jobId}] Fetch error: ${String(err)}`);
      return null;
    }
  } catch (outerErr) {
    console.error(`[job ${jobId}] Unexpected error: ${String(outerErr)}`);
    return null;
  }
}

async function processChunk(sessionOpts, chunk) {
  const tasks = chunk.map(({ row, index }) =>
    sendRequest({ ...sessionOpts, row, index })
  );
  return Promise.all(tasks);
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function runJob(job) {
  const { id, pixel, file, baseUrl, chunkSize, reqDelay, chunkDelay, timeout } = job;
  job.status = 'running';
  job.startedAt = new Date().toISOString();

  console.log(`Attempting to read file: ${file}`);
  
  // Read CSV
  let csvContent;
  try {
    csvContent = await fs.readFile(file, 'utf-8');
    console.log('File read successfully, length:', csvContent.length);
  } catch (e) {
    const errorMsg = `Ошибка чтения файла: ${e.message}`;
    console.error(errorMsg);
    job.status = 'failed';
    job.error = errorMsg;
    job.finishedAt = new Date().toISOString();
    return;
  }

  let records;
  try {
    console.log('Parsing CSV content...');
    
    // Try different delimiters and formats
    const parseOptions = [
      { delimiter: ';', quote: '"', skip_empty_lines: true, trim: true },
      { delimiter: ',', quote: '"', skip_empty_lines: true, trim: true },
      { delimiter: '\t', quote: '"', skip_empty_lines: true, trim: true }
    ];
    
    let parseError = null;
    
    for (const options of parseOptions) {
      try {
        console.log(`Trying delimiter: ${JSON.stringify(options.delimiter)}`);
        records = parse(csvContent, {
          columns: true,
          ...options
        });
        
        if (records.length > 0 && Object.keys(records[0]).length > 1) {
          console.log(`Successfully parsed with delimiter: ${JSON.stringify(options.delimiter)}`);
          parseError = null;
          break;
        }
      } catch (e) {
        parseError = e;
        console.log(`Failed with delimiter ${options.delimiter}:`, e.message);
      }
    }
    
    if (parseError) throw parseError;
    
    console.log(`Parsed ${records.length} records`);
    if (records.length > 0) {
      console.log('First record fields:', Object.keys(records[0]));
      console.log('First 3 records:', records.slice(0, 3));
    }
  } catch (e) {
    job.status = 'failed';
    job.error = `Ошибка парсинга CSV: ${e}`;
    job.finishedAt = new Date().toISOString();
    return;
  }

  // Apply per-type limits if provided
  const hasLimits = (job.limits?.leads || 0) > 0 || (job.limits?.sales || 0) > 0;
  if (hasLimits) {
    const limited = [];
    let leadsLeft = job.limits.leads || 0;
    let salesLeft = job.limits.sales || 0;
    for (const rec of records) {
      const kind = mapStatus(rec['\u0421\u0442\u0430\u0442\u0443\u0441'] ?? rec['Статус']);
      if (kind === 'lead' && leadsLeft > 0) { limited.push(rec); leadsLeft--; }
      else if (kind === 'sale' && salesLeft > 0) { limited.push(rec); salesLeft--; }
      if (leadsLeft <= 0 && salesLeft <= 0) break;
    }
    records = limited;
  }

  job.progress.total = records.length;

  const indexed = records.map((row, index) => ({ row, index }));
  const chunks = chunkArray(indexed, chunkSize);
  const successfulIndices = [];

  const sessionOpts = { baseUrl, pixel, reqDelay, timeout, progress: job.progress, jobId: id };

  for (const chunk of chunks) {
    if (job.cancelled) {
      job.status = 'cancelled';
      job.finishedAt = new Date().toISOString();
      return;
    }
    const results = await processChunk(sessionOpts, chunk);
    for (const idx of results) if (idx !== null && idx !== undefined) successfulIndices.push(idx);
    await sleep(chunkDelay);
  }

  // Note: we no longer modify the source CSV file. Successful rows are NOT removed.

  job.status = job.error ? 'completed_with_errors' : 'completed';
  job.finishedAt = new Date().toISOString();
}

// In-memory job store
const jobs = new Map();

function createJob({ pixel, file, baseUrl, chunkSize, reqDelay, chunkDelay, timeout, leadsCount = 0, salesCount = 0, clientId = null }) {
  const id = crypto.randomUUID();
  const job = {
    id,
    status: 'queued',
    createdAt: new Date().toISOString(),
    startedAt: null,
    finishedAt: null,
    pixel,
    file,
    baseUrl,
    chunkSize,
    reqDelay,
    chunkDelay,
    timeout,
    progress: { sent: 0, total: 0, errors: 0, leads: 0, sales: 0, logs: [] },
    error: null,
    cancelled: false,
    limits: { leads: Number(leadsCount) || 0, sales: Number(salesCount) || 0 },
    clientId: clientId || crypto.randomUUID(),
  };
  jobs.set(id, job);
  // Fire and forget
  runJob(job).catch((e) => {
    job.status = 'failed';
    job.error = `Непредвиденная ошибка: ${e}`;
    job.finishedAt = new Date().toISOString();
  });
  return job;
}

const app = express();
app.use(express.json());
// Optional base path for mounting the entire app (UI + API) under a subpath
const BASE_PATH = process.env.BASE_PATH || '/';
const router = express.Router();
// Assign or read per-user clientId via cookie for isolation
function parseCookies(header) {
  const out = {};
  if (!header) return out;
  for (const part of header.split(';')) {
    const idx = part.indexOf('=');
    if (idx === -1) continue;
    const k = part.slice(0, idx).trim();
    const v = part.slice(idx + 1).trim();
    out[k] = decodeURIComponent(v);
  }
  return out;
}

function ensureClientId(req, res, next) {
  const cookies = parseCookies(req.headers.cookie || '');
  let clientId = cookies.clientId || req.get('x-client-id');
  if (!clientId) {
    clientId = crypto.randomUUID();
    res.setHeader('Set-Cookie', `clientId=${encodeURIComponent(clientId)}; Path=/; Max-Age=${60 * 60 * 24 * 365}; SameSite=Lax`);
  }
  req.clientId = clientId;
  next();
}

app.use(ensureClientId);
// Serve static UI under base path
router.use(express.static(path.resolve(process.cwd(), 'public')));

router.get('/health', (_req, res) => {
  res.json({ ok: true, serverTime: new Date().toISOString() });
});

// Root UI under base path
router.get('/', (_req, res) => {
  res.sendFile(path.resolve(process.cwd(), 'public', 'index.html'));
});

// List available country CSV files from countries/ directory
router.get('/countries', async (_req, res) => {
  try {
    const dir = path.resolve(process.cwd(), 'countries');
    const entries = await fs.readdir(dir, { withFileTypes: true });
    const items = entries
      .filter((e) => e.isFile() && e.name.toLowerCase().endsWith('.csv'))
      .map((e) => {
        const file = e.name;
        const code = file.replace(/\.csv$/i, '');
        return { code, file: path.join('countries', file) };
      });

    // Attach Russian display name if possible
    let display;
    try {
      display = new Intl.DisplayNames(['ru'], { type: 'region' });
    } catch {
      display = null;
    }
    const result = items.map((it) => {
      const nameRu = display ? (display.of(it.code.toUpperCase()) || null) : null;
      return { ...it, nameRu };
    });
    res.json(result);
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// Create and start a new job
router.post('/send', (req, res) => {
  const {
    pixel,
    file,
    url,
    chunkSize = DEFAULTS.chunkSize,
    reqDelay = DEFAULTS.reqDelay,
    chunkDelay = DEFAULTS.chunkDelay,
    timeout = DEFAULTS.timeout,
    leadsCount = 0,
    salesCount = 0,
    clientId: clientIdBody = null,
  } = req.body || {};

  if (!pixel || !file) {
    return res.status(400).json({ error: 'Fields "pixel" and "file" are required' });
  }

  // Resolve file: if a bare name like "in" is provided, use countries/in.csv
  const resolveFilePath = (input) => {
    try {
      console.log('Resolving file path for input:', input);
      const hasSeparator = /[\\/]/.test(input);
      const hasCsvExt = input.toLowerCase().endsWith('.csv');
      
      let resolvedPath;
      if (hasSeparator) {
        // Treat as path (relative or absolute) as-is
        resolvedPath = path.resolve(process.cwd(), input);
      } else {
        // No separator: treat as countries/<name>[.csv]
        const baseName = hasCsvExt ? input : `${input}.csv`;
        resolvedPath = path.resolve(process.cwd(), 'countries', baseName);
      }
      
      console.log('Resolved path:', resolvedPath);
      
      // Verify file exists
      if (!fsSync.existsSync(resolvedPath)) {
        throw new Error(`File not found: ${resolvedPath}`);
      }
      
      return resolvedPath;
    } catch (error) {
      console.error('Error resolving file path:', error);
      throw error;
    }
  };

  const resolvedFile = resolveFilePath(String(file));

  const clientIdHeader = req.get('x-client-id');
  const job = createJob({
    pixel,
    file: resolvedFile,
    baseUrl: url || DEFAULT_ROUTE_URL,
    chunkSize: Number(chunkSize),
    reqDelay: Number(reqDelay),
    chunkDelay: Number(chunkDelay),
    timeout: Number(timeout),
    leadsCount: Number(leadsCount) || 0,
    salesCount: Number(salesCount) || 0,
    clientId: String(clientIdHeader || clientIdBody || req.clientId || ''),
  });

  // Include both id and jobId for compatibility with UI expecting `id`
  res.status(202).json({ id: job.id, jobId: job.id, status: job.status, file: resolvedFile, clientId: job.clientId });
});

// Get job status
router.get('/jobs/:id', (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: 'Job not found' });
  const clientId = req.get('x-client-id') || req.clientId;
  if (!clientId || (job.clientId && job.clientId !== clientId)) {
    return res.status(404).json({ error: 'Job not found' });
  }
  res.json(job);
});

// List jobs (recent first)
router.get('/jobs', (req, res) => {
  const clientId = req.get('x-client-id') || req.clientId;
  if (!clientId) return res.json([]);
  let list = Array.from(jobs.values()).filter((j) => j.clientId === clientId);
  list = list.sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));
  res.json(list);
});

// Cancel a job
router.post('/jobs/:id/cancel', (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: 'Job not found' });
  const clientId = req.get('x-client-id') || req.clientId;
  if (!clientId || (job.clientId && job.clientId !== clientId)) {
    return res.status(404).json({ error: 'Job not found' });
  }
  if (job.status === 'completed' || job.status === 'failed' || job.status === 'completed_with_errors') {
    return res.status(400).json({ error: 'Job already finished' });
  }
  job.cancelled = true;
  res.json({ ok: true });
});

// Mount router at base path
app.use(BASE_PATH, router);

const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || '0.0.0.0';
app.listen(PORT, HOST, () => {
  console.log(`Pixel server listening on http://${HOST}:${PORT}${BASE_PATH === '/' ? '' : BASE_PATH}`);
});

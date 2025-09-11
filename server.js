// server.js
// Node 18+ required. Starts an HTTP API to run pixel-send jobs in parallel.

import express from 'express';
import dotenv from 'dotenv';
import fs from 'node:fs/promises';
import fsSync from 'node:fs';
import { parse } from 'csv-parse/sync';
import crypto from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Agent, setGlobalDispatcher } from 'undici';
import Redis from 'ioredis';
import { EventEmitter } from 'node:events';

// Load env once at startup
dotenv.config();

// Redis client
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379/0';
const redis = new Redis(REDIS_URL);

// Default config
const DEFAULT_ROUTE_URL = process.env.DEFAULT_ROUTE_URL || '';
const DEFAULTS = {
	chunkSize: 5,            // количество запросов в пачке
	reqDelay: 100,            // задержка между запросами (мс)
	chunkDelay: 500,          // задержка между пачками (мс)
	reqTimeout: 30_000,       // таймаут на один запрос (мс)
	sessionTimeout: 3_600_000, // общий таймаут сессии (мс)
	connLimit: 10             // максимальное число соединений (параллельных запросов)
};

// HTTP keep-alive for better throughput
const KEEPALIVE_CONNECTIONS = Number(process.env.KEEPALIVE_CONNECTIONS) || DEFAULTS.connLimit;
const keepAliveAgent = new Agent({
	keepAliveTimeout: 10_000,
	keepAliveMaxTimeout: 60_000,
	connections: KEEPALIVE_CONNECTIONS,
});
setGlobalDispatcher(keepAliveAgent);

// Global concurrency limiter
const MAX_CONCURRENCY = Math.max(1, Number(process.env.MAX_CONCURRENCY) || DEFAULTS.connLimit);
let concurrencyInUse = 0;
const concurrencyWaiters = [];
async function acquireConcurrency() {
	if (concurrencyInUse < MAX_CONCURRENCY) {
		concurrencyInUse += 1;
		return;
	}
	await new Promise((resolve) => concurrencyWaiters.push(resolve));
	concurrencyInUse += 1;
}
function releaseConcurrency() {
	concurrencyInUse -= 1;
	const next = concurrencyWaiters.shift();
	if (next) next();
}
async function withConcurrency(fn) {
	await acquireConcurrency();
	try {
		return await fn();
	} finally {
		releaseConcurrency();
	}
}

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function mapStatus(statusDefault) {
	const map = { lead: 'lead', sale: 'sale', rejected: 'install' };
	return map[String(statusDefault ?? '').trim()] ?? 'unknown';
}

// ---- Redis helpers for jobs and cancellation ----
function cancelKey(jobId) { return `job:${jobId}:cancelled`; }
async function setCancelled(jobId) { await redis.set(cancelKey(jobId), '1'); }
async function clearCancelled(jobId) { await redis.del(cancelKey(jobId)); }
async function isCancelled(jobId) { return (await redis.get(cancelKey(jobId))) === '1'; }

function buildUrl(baseUrl, pixel, row) {
	const userAgent = encodeURIComponent(String(row['User Agent'] ?? ''));
	const fbclid = row['Sub ID 7'];
	const ip = row['IP'] ?? '';
	const subid = row['Subid'] ?? '';
	const country = row['\u0424\u043b\u0430\u0433 \u0441\u0442\u0440\u0430\u043d\u044b'] ?? row['Флаг страны'] ?? '';
	const status = mapStatus(row['\u0421\u0442\u0430\u0441\u0442\u0443\u0441'] ?? row['Статус']);

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

async function sendRequest({ baseUrl, pixel, row, index, reqDelay, reqTimeout, progress, jobId }) {
	try {
		// Fast cancel check before doing anything
		if (await isCancelled(jobId)) return null;

		const url = buildUrl(baseUrl, pixel, row);
		// Delay between requests
		await sleep(reqDelay);

		// Cancel after delay as well
		if (await isCancelled(jobId)) return null;

		return await withConcurrency(async () => {
			const controller = new AbortController();
			const timeoutId = setTimeout(() => controller.abort(), reqTimeout);
			try {
				const response = await fetch(url, { method: 'GET', signal: controller.signal });
				clearTimeout(timeoutId);

				if (response.ok) {
					progress.sent += 1;
					progress.logs.push({ level: 'info', msg: `OK ${progress.sent}/${progress.total}`, url, status: response.status });
					// Increment per-type counters (lead/sale)
					try {
						const statusKind = mapStatus(row['\u0421\u0442\u0430\u0441\u0442\u0443\u0441'] ?? row['Статус']);
						if (statusKind === 'lead') progress.leads += 1;
						else if (statusKind === 'sale') progress.sales += 1;
					} catch {}
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
				clearTimeout(timeoutId);
				progress.errors += 1;
				const isAbort = (err && (err.name === 'AbortError' || String(err).includes('AbortError')));
				if (isAbort) {
					progress.logs.push({ level: 'error', msg: `Timeout after ${reqTimeout}ms`, url });
					console.error(`[job ${jobId}] Timeout after ${reqTimeout}ms | ${url}`);
				} else {
					progress.logs.push({ level: 'error', msg: `Fetch error: ${String(err)}` });
					console.error(`[job ${jobId}] Fetch error: ${String(err)}`);
				}
				return null;
			}
		});
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

// Redis helpers for jobs storage
async function redisSaveJob(job) {
	await redis.set(`job:${job.id}`, JSON.stringify(job));
	if (job.clientId) {
		await redis.zadd(`jobs:byClient:${job.clientId}`, Date.parse(job.createdAt) || Date.now(), job.id);
	}
}
async function redisGetJob(id) {
	const data = await redis.get(`job:${id}`);
	return data ? JSON.parse(data) : null;
}
async function redisListJobsByClient(clientId, limit = 100) {
	const ids = await redis.zrevrange(`jobs:byClient:${clientId}`, 0, limit - 1);
	if (!ids.length) return [];
	const pipeline = redis.pipeline();
	for (const id of ids) pipeline.get(`job:${id}`);
	const res = await pipeline.exec();
	return res
		.map(([, val]) => (val ? JSON.parse(val) : null))
		.filter(Boolean)
		.sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));
}
async function redisUpdateJob(job) {
	await redis.set(`job:${job.id}`, JSON.stringify(job));
	// Notify SSE listeners
	notifyJobUpdate(job);
}

async function runJob(job) {
	const { id, pixel, file, baseUrl, chunkSize, reqDelay, chunkDelay, reqTimeout, sessionTimeout } = job;
	job.status = 'running';
	job.startedAt = new Date().toISOString();
	await redisUpdateJob(job);

	const startedAtMs = Date.now();
	const sessionDeadline = startedAtMs + Number(sessionTimeout || DEFAULTS.sessionTimeout);

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
		await redisUpdateJob(job);
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
		await redisUpdateJob(job);
		return;
	}

	// Apply per-type limits if provided
	const hasLimits = (job.limits?.leads || 0) > 0 || (job.limits?.sales || 0) > 0;
	if (hasLimits) {
		const limited = [];
		let leadsLeft = job.limits.leads || 0;
		let salesLeft = job.limits.sales || 0;
		for (const rec of records) {
			const kind = mapStatus(rec['\u0421\u0442\u0430\u0441\u0442\u0443\u0441'] ?? rec['Статус']);
			if (kind === 'lead' && leadsLeft > 0) { limited.push(rec); leadsLeft--; }
			else if (kind === 'sale' && salesLeft > 0) { limited.push(rec); salesLeft--; }
			if (leadsLeft <= 0 && salesLeft <= 0) break;
		}
		records = limited;
	}

	job.progress.total = records.length;
	await redisUpdateJob(job);

	const indexed = records.map((row, index) => ({ row, index }));
	const chunks = chunkArray(indexed, chunkSize);
	const successfulIndices = [];

	const sessionOpts = { baseUrl, pixel, reqDelay, reqTimeout, progress: job.progress, jobId: id };

	for (const chunk of chunks) {
		// Session timeout check
		if (Date.now() > sessionDeadline) {
			job.status = 'failed';
			job.error = `Session timeout after ${Number(sessionTimeout || DEFAULTS.sessionTimeout)}ms`;
			job.finishedAt = new Date().toISOString();
			await setCancelled(id);
			await redisUpdateJob(job);
			console.error(`[job ${id}] session timeout`);
			return;
		}
		// Fast cancel check from Redis before each chunk
		if (await isCancelled(id)) {
			job.cancelled = true;
			job.status = 'cancelled';
			job.finishedAt = new Date().toISOString();
			await redisUpdateJob(job);
			console.log(`[job ${id}] cancelled | ok=${job.progress.sent}/${job.progress.total} errors=${job.progress.errors} leads=${job.progress.leads} sales=${job.progress.sales}`);
			return;
		}
		const results = await processChunk(sessionOpts, chunk);
		for (const idx of results) if (idx !== null && idx !== undefined) successfulIndices.push(idx);
		await redisUpdateJob(job);
		await sleep(chunkDelay);
	}

	// Note: we no longer modify the source CSV file. Successful rows are NOT removed.

	job.status = job.error ? 'completed_with_errors' : 'completed';
	job.finishedAt = new Date().toISOString();
	await redisUpdateJob(job);
	await clearCancelled(id);
	console.log(`[job ${id}] ${job.status} | ok=${job.progress.sent}/${job.progress.total} errors=${job.progress.errors} leads=${job.progress.leads} sales=${job.progress.sales} | maxConcurrency=${MAX_CONCURRENCY} keepAliveConns=${KEEPALIVE_CONNECTIONS}`);
}

// Redis-backed job store API
async function createJob({ pixel, file, baseUrl, chunkSize, reqDelay, chunkDelay, reqTimeout, sessionTimeout, connLimit, leadsCount = 0, salesCount = 0, clientId = null }) {
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
		reqTimeout,
		sessionTimeout,
		connLimit,
		progress: { sent: 0, total: 0, errors: 0, leads: 0, sales: 0, logs: [] },
		error: null,
		cancelled: false,
		limits: { leads: Number(leadsCount) || 0, sales: Number(salesCount) || 0 },
		clientId: clientId || crypto.randomUUID(),
	};
	await redisSaveJob(job);
	// Fire and forget
	runJob(job).catch(async (e) => {
		job.status = 'failed';
		job.error = `Непредвиденная ошибка: ${e}`;
		job.finishedAt = new Date().toISOString();
		await redisUpdateJob(job);
	});
	return job;
}

const app = express();
app.use(express.json());
// Optional base path for mounting the entire app (UI + API) under a subpath
const BASE_PATH = process.env.BASE_PATH || '/pixel';
const router = express.Router();

// --- SSE infrastructure: per-job subscribers ---
const sseClientsByJob = new Map(); // jobId -> Set<res>

function notifyJobUpdate(job) {
	try {
		const clients = sseClientsByJob.get(job.id);
		if (!clients || clients.size === 0) return;
		const payload = `data: ${JSON.stringify(job)}\n\n`;
		for (const res of clients) {
			try { res.write(payload); } catch {}
		}
	} catch {}
}

// Resolve project directories independent of process.cwd()
const __FILENAME = fileURLToPath(import.meta.url);
const __DIRNAME = path.dirname(__FILENAME);
const PUBLIC_DIR = path.resolve(__DIRNAME, 'public');
const COUNTRIES_DIR = process.env.COUNTRIES_DIR || path.resolve(__DIRNAME, 'countries');
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
router.use(express.static(PUBLIC_DIR));

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
		const entries = await fs.readdir(COUNTRIES_DIR, { withFileTypes: true });
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
router.post('/send', async (req, res) => {
	const {
		pixel,
		file,
		url,
		chunkSize = DEFAULTS.chunkSize,
		reqDelay = DEFAULTS.reqDelay,
		chunkDelay = DEFAULTS.chunkDelay,
		reqTimeout = DEFAULTS.reqTimeout,
		sessionTimeout = DEFAULTS.sessionTimeout,
		connLimit = DEFAULTS.connLimit,
		leadsCount = 0,
		salesCount = 0,
		clientId: clientIdBody = null,
		// Backward compat: allow "timeout" to set reqTimeout if provided
		timeout,
	} = req.body || {};

	const effectiveReqTimeout = Number(timeout) || Number(reqTimeout) || DEFAULTS.reqTimeout;

	if (!pixel || !file) {
		return res.status(400).json({ error: 'Fields "pixel" and "file" are required' });
	}
	if (!/^\d+$/.test(String(pixel))) {
		return res.status(400).json({ error: 'Field "pixel" must contain digits only' });
	}

	// Resolve file: if a bare name like "in" is provided, use countries/in.csv (under COUNTRIES_DIR)
	const resolveFilePath = (input) => {
		try {
			console.log('Resolving file path for input:', input);
			const hasSeparator = /[\\/]/.test(input);
			const hasCsvExt = input.toLowerCase().endsWith('.csv');
			
			let resolvedPath;
			if (hasSeparator) {
				// Treat as path (relative or absolute)
				resolvedPath = path.resolve(input);
			} else {
				// No separator: treat as countries/<name>[.csv]
				const baseName = hasCsvExt ? input : `${input}.csv`;
				resolvedPath = path.resolve(COUNTRIES_DIR, baseName);
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
	const job = await createJob({
		pixel,
		file: resolvedFile,
		baseUrl: url || DEFAULT_ROUTE_URL,
		chunkSize: Number(chunkSize),
		reqDelay: Number(reqDelay),
		chunkDelay: Number(chunkDelay),
		reqTimeout: Number(effectiveReqTimeout),
		sessionTimeout: Number(sessionTimeout) || DEFAULTS.sessionTimeout,
		connLimit: Number(connLimit) || DEFAULTS.connLimit,
		leadsCount: Number(leadsCount) || 0,
		salesCount: Number(salesCount) || 0,
		clientId: String(clientIdHeader || clientIdBody || req.clientId || ''),
	});

	// Include both id and jobId for compatibility with UI expecting `id`
	res.status(202).json({ id: job.id, jobId: job.id, status: job.status, file: resolvedFile, clientId: job.clientId });
});

// Get job status
router.get('/jobs/:id', async (req, res) => {
	const job = await redisGetJob(req.params.id);
	if (!job) return res.status(404).json({ error: 'Job not found' });
	const clientId = req.get('x-client-id') || req.clientId;
	if (!clientId || (job.clientId && job.clientId !== clientId)) {
		return res.status(404).json({ error: 'Job not found' });
	}
	res.json(job);
});

// Server-Sent Events stream for a single job
router.get('/jobs/:id/stream', async (req, res) => {
	const job = await redisGetJob(req.params.id);
	if (!job) return res.status(404).json({ error: 'Job not found' });
	const clientId = req.get('x-client-id') || req.clientId;
	if (!clientId || (job.clientId && job.clientId !== clientId)) {
		return res.status(404).json({ error: 'Job not found' });
	}

	res.setHeader('Content-Type', 'text/event-stream');
	res.setHeader('Cache-Control', 'no-cache');
	res.setHeader('Connection', 'keep-alive');
	res.flushHeaders?.();

	// Send initial state immediately
	res.write(`data: ${JSON.stringify(job)}\n\n`);

	// Register client
	let set = sseClientsByJob.get(job.id);
	if (!set) { set = new Set(); sseClientsByJob.set(job.id, set); }
	set.add(res);

	// Heartbeat to keep connection alive (some proxies time out idle streams)
	const heartbeat = setInterval(() => {
		try { res.write(': ping\n\n'); } catch {}
	}, 15000);

	// Cleanup on close
	req.on('close', () => {
		clearInterval(heartbeat);
		const clients = sseClientsByJob.get(job.id);
		if (clients) {
			clients.delete(res);
			if (clients.size === 0) sseClientsByJob.delete(job.id);
		}
	});
});

// List jobs (recent first)
router.get('/jobs', async (req, res) => {
	const clientId = req.get('x-client-id') || req.clientId;
	if (!clientId) return res.json([]);
	const list = await redisListJobsByClient(clientId, 200);
	res.json(list);
});

// Cancel a job
router.post('/jobs/:id/cancel', async (req, res) => {
	const id = req.params.id;
	const job = await redisGetJob(id);
	if (!job) return res.status(404).json({ error: 'Job not found' });
	const clientId = req.get('x-client-id') || req.clientId;
	if (!clientId || (job.clientId && job.clientId !== clientId)) {
		return res.status(404).json({ error: 'Job not found' });
	}
	if (job.status === 'completed' || job.status === 'failed' || job.status === 'completed_with_errors') {
		return res.status(400).json({ error: 'Job already finished' });
	}
	job.cancelled = true;
	await redisUpdateJob(job);
	await setCancelled(id);
	res.json({ ok: true });
});

// Mount router at base path
app.use(BASE_PATH, router);

const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || '0.0.0.0';
app.listen(PORT, HOST, () => {
	console.log(`Pixel server listening on http://${HOST}:${PORT}${BASE_PATH === '/' ? '' : BASE_PATH}`);
});

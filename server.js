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
import { MongoClient } from 'mongodb';

// Load env once at startup
dotenv.config();

// Redis client
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379/0';
const redis = new Redis(REDIS_URL);

// Default config
const DEFAULT_ROUTE_URL = process.env.DEFAULT_ROUTE_URL || '';
const DEFAULTS = {
	reqDelay: 100,            // задержка между запросами (мс)
	reqTimeout: 30_000,       // таймаут на один запрос (мс)
	connLimit: 10,            // максимальное число соединений (параллельных запросов)
	chunkSize: 5,             // количество одновременных запросов в чанке
	chunkDelay: 500           // задержка между чанками (мс)
};

// TTLs for Redis entries
const JOB_TTL_SECONDS = Number(process.env.JOB_TTL_SECONDS) > 0 ? Number(process.env.JOB_TTL_SECONDS) : 60 * 60 * 24 * 3; // 3 days
const CANCEL_TTL_SECONDS = Number(process.env.CANCEL_TTL_SECONDS) > 0 ? Number(process.env.CANCEL_TTL_SECONDS) : 60 * 60 * 24; // 1 day
// Debounce settings for Redis job updates
const JOB_UPDATE_DEBOUNCE_MS = Number(process.env.JOB_UPDATE_DEBOUNCE_MS) >= 0 ? Number(process.env.JOB_UPDATE_DEBOUNCE_MS) : 300;
const JOB_UPDATE_MAX_MS = Number(process.env.JOB_UPDATE_MAX_MS) > 0 ? Number(process.env.JOB_UPDATE_MAX_MS) : 2000;

// HTTP keep-alive for better throughput
const KEEPALIVE_CONNECTIONS = Number(process.env.KEEPALIVE_CONNECTIONS) || DEFAULTS.connLimit;
const keepAliveAgent = new Agent({
	keepAliveTimeout: 10_000,
	keepAliveMaxTimeout: 60_000,
	connections: KEEPALIVE_CONNECTIONS,
});
setGlobalDispatcher(keepAliveAgent);

// Global concurrency limiter for HTTP requests
const MAX_CONCURRENCY = Math.max(1, Number(process.env.MAX_CONCURRENCY) || DEFAULTS.connLimit);
// How often to emit a short success log to stdout (to avoid log spam)
const SUCCESS_LOG_EVERY = Math.max(1, Number(process.env.SUCCESS_LOG_EVERY) || 50);
let concurrencyInUse = 0;
const concurrencyWaiters = [];

// Job queue management to prevent server overload
const MAX_CONCURRENT_JOBS = Math.max(1, Number(process.env.MAX_CONCURRENT_JOBS) || 3);
const MAX_QUEUE_SIZE = Math.max(10, Number(process.env.MAX_QUEUE_SIZE) || 50);
let runningJobs = 0;
const jobQueue = [];

// Memory and resource monitoring
const MEMORY_CHECK_INTERVAL = 30000; // 30 seconds
let lastMemoryCheck = 0;

function checkMemoryUsage() {
	const now = Date.now();
	if (now - lastMemoryCheck < MEMORY_CHECK_INTERVAL) return;
	lastMemoryCheck = now;
	
	const usage = process.memoryUsage();
	const heapUsedMB = Math.round(usage.heapUsed / 1024 / 1024);
	const heapTotalMB = Math.round(usage.heapTotal / 1024 / 1024);
	const rssMB = Math.round(usage.rss / 1024 / 1024);
	
	console.log(`[MEMORY] RSS: ${rssMB}MB, Heap: ${heapUsedMB}/${heapTotalMB}MB, Jobs: ${runningJobs}/${MAX_CONCURRENT_JOBS}, Queue: ${jobQueue.length}`);
	
	// Warning if memory usage is high
	if (heapUsedMB > 1000) {
		console.warn(`[WARNING] High memory usage: ${heapUsedMB}MB heap used`);
	}
}

async function acquireJobSlot() {
	checkMemoryUsage();
	
	if (runningJobs < MAX_CONCURRENT_JOBS) {
		runningJobs += 1;
		return;
	}
	
	// Check queue size limit
	if (jobQueue.length >= MAX_QUEUE_SIZE) {
		throw new Error(`Job queue is full (${jobQueue.length}/${MAX_QUEUE_SIZE}). Server is overloaded.`);
	}
	
	await new Promise((resolve) => jobQueue.push(resolve));
	runningJobs += 1;
}

function releaseJobSlot() {
	runningJobs -= 1;
	const next = jobQueue.shift();
	if (next) next();
}

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

// ---- MongoDB (for pixel validation) ----
const MONGO_URL = process.env.MONGO_URL || process.env.DB_URL || '';
let mongoClient = null;
let mongoDb = null; // Use default DB from connection string
async function getMongoDb() {
    if (!MONGO_URL) {
        throw new Error('DB connection string not configured (set MONGO_URL in env/PM2 ecosystem config)');
    }
    if (mongoDb && mongoClient) return mongoDb;
    mongoClient = new MongoClient(MONGO_URL, {
        // modern unified topology by default in v5
        maxPoolSize: 10,
    });
    await mongoClient.connect();
    mongoDb = mongoClient.db();
    return mongoDb;
}
async function pixelExists(pixel) {
    try {
        const db = await getMongoDb();
        const col = db.collection('pixel2');
        const numeric = Number(pixel);
        const query = Number.isFinite(numeric)
            ? { $or: [ { number: pixel }, { number: numeric } ] }
            : { number: pixel };
        const doc = await col.findOne(query, { projection: { _id: 1 } });
        return !!doc;
    } catch (e) {
        // Surface errors to the caller for proper 5xx response
        throw e;
    }
}

// ---- Redis helpers for jobs and cancellation ----
function cancelKey(jobId) { return `job:${jobId}:cancelled`; }
async function setCancelled(jobId) { await redis.set(cancelKey(jobId), '1', 'EX', CANCEL_TTL_SECONDS); }
async function clearCancelled(jobId) { await redis.del(cancelKey(jobId)); }
async function isCancelled(jobId) { return (await redis.get(cancelKey(jobId))) === '1'; }

function buildUrl(baseUrl, pixel, row) {
    // Validate base URL to avoid fetch errors like "Failed to parse URL from ?pixel=..."
    if (!baseUrl || !/^https?:\/\//i.test(String(baseUrl))) {
        throw new Error('Base URL is empty or not absolute (must start with http:// or https://)');
    }

    // Extract parameters like in Python version
    const pixelValue = String(pixel).replace('.0', ''); // Remove .0 like in Python
    const fbclid = row['Sub ID 28'] ?? row['Sub ID 7'] ?? '';
    const ip = row['IP'] ?? '';
    const subid = row['Subid'] ?? '';
    const userAgent = encodeURIComponent(String(row['User Agent'] ?? ''));
    const country = row['\u0424\u043b\u0430\u0433 \u0441\u0442\u0440\u0430\u043d\u044b'] ?? row['Флаг страны'] ?? '';
    const status = mapStatus(row['\u0421\u0442\u0430\u0441\u0442\u0443\u0441'] ?? row['Статус']);

    const sep = String(baseUrl).includes('?') ? '&' : '?';
    const url =
        `${baseUrl}${sep}pixel=${encodeURIComponent(pixelValue)}` +
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
		
		// Python-style detailed logging
		console.log(`[job ${jobId}] URL: ${url}`);
		
		// Delay between requests (like Python asyncio.sleep(0.1))
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
					
					// Python-style success logging
					console.log(`[job ${jobId}] Отправлен запрос ${progress.sent} из ${progress.total}`);
					console.log(`[job ${jobId}] Статус ответа: ${response.status}`);
					console.log(`[job ${jobId}] ---`);
					
					// cap logs to avoid unbounded growth
					progress.logs.push({ level: 'info', msg: `OK ${progress.sent}/${progress.total}`, url, status: response.status });
					if (progress.logs.length > 1000) progress.logs.splice(0, progress.logs.length - 1000);
					
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
					
					// Python-style error logging
					console.error(`[job ${jobId}] Ошибка при отправке запроса. Статус: ${response.status}`);
					console.error(`[job ${jobId}] Ответ сервера: ${content?.slice(0,200) ?? ''}`);
					
					progress.logs.push({ level: 'error', msg: `HTTP ${response.status}`, url, content });
					if (progress.logs.length > 1000) progress.logs.splice(0, progress.logs.length - 1000);
					
					return null;
				}
			} catch (err) {
				clearTimeout(timeoutId);
				progress.errors += 1;
				const isAbort = (err && (err.name === 'AbortError' || String(err).includes('AbortError')));
				
				if (isAbort) {
					console.error(`[job ${jobId}] Timeout after ${reqTimeout}ms | ${url}`);
					progress.logs.push({ level: 'error', msg: `Timeout after ${reqTimeout}ms`, url });
				} else {
					console.error(`[job ${jobId}] Ошибка при отправке запроса: ${String(err)}`);
					progress.logs.push({ level: 'error', msg: `Fetch error: ${String(err)}` });
				}
				
				if (progress.logs.length > 1000) progress.logs.splice(0, progress.logs.length - 1000);
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
	await redis.set(`job:${job.id}`, JSON.stringify(job), 'EX', JOB_TTL_SECONDS);
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
	await redis.set(`job:${job.id}`, JSON.stringify(job), 'EX', JOB_TTL_SECONDS);
	// Notify SSE listeners
	notifyJobUpdate(job);
}

// Debounced/batched updater
const pendingJobUpdates = new Map(); // jobId -> { timer, firstAt, snapshot }
async function flushPendingJobUpdate(jobId) {
	const entry = pendingJobUpdates.get(jobId);
	if (!entry) return;
	pendingJobUpdates.delete(jobId);
	try { await redisUpdateJob(entry.snapshot); } catch {}
}
function redisUpdateJobBatched(job, { force = false } = {}) {
	if (force || JOB_UPDATE_DEBOUNCE_MS <= 0) {
		pendingJobUpdates.delete(job.id);
		return redisUpdateJob(job);
	}
	const now = Date.now();
	let entry = pendingJobUpdates.get(job.id);
	if (!entry) {
		entry = { timer: null, firstAt: now, snapshot: job };
	} else {
		if (entry.timer) clearTimeout(entry.timer);
		entry.snapshot = job;
	}
	const maxDelayLeft = Math.max(0, (entry.firstAt + JOB_UPDATE_MAX_MS) - now);
	const delay = Math.min(JOB_UPDATE_DEBOUNCE_MS, maxDelayLeft);
	entry.timer = setTimeout(() => {
		flushPendingJobUpdate(job.id);
	}, delay);
	pendingJobUpdates.set(job.id, entry);
}

async function runJob(job) {
	// Acquire job slot to limit concurrent jobs
	await acquireJobSlot();
	
	try {
		const { id, pixel, file, baseUrl, chunkSize, reqDelay, chunkDelay, reqTimeout, sessionTimeout } = job;
		job.status = 'running';
		job.startedAt = new Date().toISOString();
		await redisUpdateJobBatched(job, { force: true });

		const startedAtMs = Date.now();
		const sessionDeadline = (Number(sessionTimeout) > 0) ? (startedAtMs + Number(sessionTimeout)) : null;

		console.log(`[job ${id}] Starting job (${runningJobs}/${MAX_CONCURRENT_JOBS} slots used) - file: ${file}`);
	
    // Sequential CSV processing to reduce memory load
    let csvFiles = [];
    let totalRecords = 0;
    
    try {
        const stat = await fs.stat(file);
        
        if (stat.isDirectory()) {
            console.log(`Input is a directory. Scanning CSV files in: ${file}`);
            const dirEntries = await fs.readdir(file, { withFileTypes: true });
            csvFiles = dirEntries
                .filter((e) => e.isFile() && e.name.toLowerCase().endsWith('.csv'))
                .map((e) => path.resolve(file, e.name));
            console.log(`Found ${csvFiles.length} CSV files for sequential processing`);
        } else {
            console.log(`Single CSV file: ${file}`);
            csvFiles = [file];
        }

        if (csvFiles.length === 0) {
            throw new Error('No CSV files found');
        }
    } catch (e) {
        const errorMsg = `Ошибка доступа к файлам: ${e?.message || e}`;
        job.status = 'failed';
        job.error = errorMsg;
        job.finishedAt = new Date().toISOString();
        await redisUpdateJobBatched(job, { force: true });
        return;
    }

    // Helper function to parse CSV with fallback delimiters
    const parseWithFallbacks = (csvContent) => {
        const parseOptions = [
            { delimiter: ';', quote: '"', skip_empty_lines: true, trim: true },
            { delimiter: ',', quote: '"', skip_empty_lines: true, trim: true },
            { delimiter: '\t', quote: '"', skip_empty_lines: true, trim: true }
        ];
        let lastErr = null;
        for (const options of parseOptions) {
            try {
                const recs = parse(csvContent, { columns: true, ...options });
                if (recs.length > 0 && Object.keys(recs[0]).length > 1) return recs;
            } catch (e) {
                lastErr = e;
            }
        }
        if (lastErr) throw lastErr;
        return [];
    };

    // Process CSV files sequentially to reduce memory usage
    const sessionOpts = { baseUrl, pixel, chunkSize, reqDelay, chunkDelay, reqTimeout, progress: job.progress, jobId: id };
    const hasLimits = (job.limits?.leads || 0) > 0 || (job.limits?.sales || 0) > 0;
    let leadsLeft = job.limits?.leads || 0;
    let salesLeft = job.limits?.sales || 0;
    let processedFiles = 0;

    for (const csvPath of csvFiles) {
        // Check cancellation before each file
        if (await isCancelled(id)) {
            job.cancelled = true;
            job.status = 'cancelled';
            job.finishedAt = new Date().toISOString();
            await redisUpdateJobBatched(job, { force: true });
            console.log(`[job ${id}] cancelled after ${processedFiles}/${csvFiles.length} files | ok=${job.progress.sent}/${job.progress.total} errors=${job.progress.errors}`);
            return;
        }

        // Session timeout check
        if (sessionDeadline && Date.now() > sessionDeadline) {
            job.status = 'failed';
            job.error = `Session timeout after ${Number(sessionTimeout)}ms`;
            job.finishedAt = new Date().toISOString();
            await setCancelled(id);
            await redisUpdateJobBatched(job, { force: true });
            console.error(`[job ${id}] session timeout after ${processedFiles}/${csvFiles.length} files`);
            return;
        }

        try {
            console.log(`Processing file ${processedFiles + 1}/${csvFiles.length}: ${path.basename(csvPath)}`);
            const csvContent = await fs.readFile(csvPath, 'utf-8');
            let records = parseWithFallbacks(csvContent);
            console.log(`Parsed ${records.length} records from ${path.basename(csvPath)}`);

            // Apply limits if specified
            if (hasLimits && (leadsLeft <= 0 && salesLeft <= 0)) {
                console.log(`Limits reached, skipping remaining files`);
                break;
            }

            if (hasLimits) {
                const limited = [];
                for (const rec of records) {
                    const kind = mapStatus(rec['\u0421\u0442\u0430\u0441\u0442\u0443\u0441'] ?? rec['Статус']);
                    if (kind === 'lead' && leadsLeft > 0) { 
                        limited.push(rec); 
                        leadsLeft--; 
                    } else if (kind === 'sale' && salesLeft > 0) { 
                        limited.push(rec); 
                        salesLeft--; 
                    }
                    if (leadsLeft <= 0 && salesLeft <= 0) break;
                }
                records = limited;
                console.log(`Applied limits: ${records.length} records selected (leads left: ${leadsLeft}, sales left: ${salesLeft})`);
            }

            // Update total count and process records one by one
            job.progress.total += records.length;
            await redisUpdateJobBatched(job);

            // Process records in chunks like Python version
            const chunkSize = sessionOpts.chunkSize || DEFAULTS.chunkSize;
            const chunkDelay = sessionOpts.chunkDelay || DEFAULTS.chunkDelay;
            
            console.log(`[job ${id}] Processing ${records.length} records in chunks of ${chunkSize}`);
            
            for (let i = 0; i < records.length; i += chunkSize) {
                // Check cancellation before each chunk
                if (await isCancelled(id)) {
                    job.cancelled = true;
                    job.status = 'cancelled';
                    job.finishedAt = new Date().toISOString();
                    await redisUpdateJobBatched(job, { force: true });
                    console.log(`[job ${id}] cancelled during processing | ok=${job.progress.sent}/${job.progress.total} errors=${job.progress.errors}`);
                    return;
                }

                // Session timeout check
                if (sessionDeadline && Date.now() > sessionDeadline) {
                    job.status = 'failed';
                    job.error = `Session timeout after ${Number(sessionTimeout)}ms`;
                    job.finishedAt = new Date().toISOString();
                    await setCancelled(id);
                    await redisUpdateJobBatched(job, { force: true });
                    console.error(`[job ${id}] session timeout during processing`);
                    return;
                }

                // Process chunk of records concurrently (like Python asyncio.gather)
                const chunk = records.slice(i, i + chunkSize);
                const chunkTasks = chunk.map((row, chunkIndex) => {
                    const index = totalRecords + i + chunkIndex;
                    return sendRequest({ ...sessionOpts, row, index });
                });
                
                console.log(`[job ${id}] Processing chunk ${Math.floor(i/chunkSize) + 1}/${Math.ceil(records.length/chunkSize)} (${chunk.length} requests)`);
                
                // Wait for all requests in chunk to complete
                const results = await Promise.allSettled(chunkTasks);
                
                // Count successful requests
                const successful = results.filter(r => r.status === 'fulfilled' && r.value !== null).length;
                console.log(`[job ${id}] Chunk completed: ${successful}/${chunk.length} successful`);
                
                // Update progress after each chunk
                await redisUpdateJobBatched(job);
                
                // Delay between chunks (like Python asyncio.sleep(0.5))
                if (i + chunkSize < records.length) {
                    await sleep(chunkDelay);
                }
            }

            processedFiles++;
            console.log(`Completed file ${processedFiles}/${csvFiles.length}: ${path.basename(csvPath)}`);

        } catch (e) {
            console.error(`Failed to process ${csvPath}: ${e?.message || e}`);
            // Continue with next file instead of failing the entire job
        }
    }

	// Note: we no longer modify the source CSV file. Successful rows are NOT removed.

	job.status = job.error ? 'completed_with_errors' : 'completed';
	job.finishedAt = new Date().toISOString();
	await redisUpdateJobBatched(job, { force: true });
	await clearCancelled(id);
	console.log(`[job ${id}] ${job.status} | ok=${job.progress.sent}/${job.progress.total} errors=${job.progress.errors} leads=${job.progress.leads} sales=${job.progress.sales} | maxConcurrency=${MAX_CONCURRENCY} keepAliveConns=${KEEPALIVE_CONNECTIONS}`);
	
	} catch (jobError) {
		// Handle any unexpected errors in job execution
		console.error(`[job ${job.id}] Unexpected job error:`, jobError);
		job.status = 'failed';
		job.error = `Непредвиденная ошибка: ${jobError?.message || jobError}`;
		job.finishedAt = new Date().toISOString();
		await redisUpdateJobBatched(job, { force: true });
	} finally {
		// Always release the job slot
		releaseJobSlot();
		console.log(`[job ${job.id}] Released job slot (${runningJobs}/${MAX_CONCURRENT_JOBS} slots now used)`);
	}
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
		sessionTimeout: (Number(sessionTimeout) > 0 ? Number(sessionTimeout) : null),
		connLimit,
		progress: { sent: 0, total: 0, errors: 0, leads: 0, sales: 0, logs: [] },
		error: null,
		cancelled: false,
		limits: { leads: Number(leadsCount) || 0, sales: Number(salesCount) || 0 },
		clientId: clientId || crypto.randomUUID(),
	};
	await redisSaveJob(job);
	// Fire and forget with proper error handling
	runJob(job).catch(async (e) => {
		console.error(`[job ${job.id}] Job creation error:`, e);
		job.status = 'failed';
		job.error = `Непредвиденная ошибка: ${e?.message || e}`;
		job.finishedAt = new Date().toISOString();
		await redisUpdateJobBatched(job, { force: true });
		// Make sure to release job slot if job fails to start
		releaseJobSlot();
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
        // New behavior: return country folders (geo codes) that contain CSV files
        const dirItems = [];
        for (const e of entries) {
            if (!e.isDirectory()) continue;
            const code = e.name;
            try {
                const dirPath = path.resolve(COUNTRIES_DIR, code);
                const inner = await fs.readdir(dirPath, { withFileTypes: true });
                const csvFiles = inner.filter((f) => f.isFile() && f.name.toLowerCase().endsWith('.csv'));
                if (csvFiles.length > 0) {
                    dirItems.push({ code, dir: path.join('countries', code), filesCount: csvFiles.length });
                }
            } catch {}
        }

        // Attach Russian display name if possible
        let display;
        try {
            display = new Intl.DisplayNames(['ru'], { type: 'region' });
        } catch {
            display = null;
        }
        const result = dirItems.map((it) => {
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
        reqDelay = DEFAULTS.reqDelay,
        reqTimeout = DEFAULTS.reqTimeout,
        sessionTimeout,
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

    // Validate base URL early to avoid runtime fetch errors
    const effectiveBaseUrl = String(url || DEFAULT_ROUTE_URL || '').trim();
    if (!/^https?:\/\//i.test(effectiveBaseUrl)) {
        return res.status(400).json({
            error: 'Field "url" is required and must start with http:// or https:// (or set DEFAULT_ROUTE_URL env)'
        });
    }

    // Pixel validation against DB (collection: pixel2, field: number)
    try {
        const exists = await pixelExists(String(pixel));
        if (!exists) {
            return res.status(400).json({ error: 'Пиксель не найден в базе' });
        }
    } catch (e) {
        console.error('DB validation error:', e);
        return res.status(500).json({ error: `Ошибка проверки пикселя в БД: ${e?.message || e}` });
    }

    // Resolve file or directory: if a bare name like "in" is provided and a folder exists, use countries/in/ (under COUNTRIES_DIR);
    // otherwise fallback to countries/in.csv. If a path is provided, allow directory or file.
    const resolveFilePath = (input) => {
        try {
            console.log('Resolving file path for input:', input);
            const hasSeparator = /[\\/]/.test(input);
            const hasCsvExt = input.toLowerCase().endsWith('.csv');

            let resolvedPath;
            if (hasSeparator) {
                // Treat as path (relative or absolute) and allow directory
                const tentative = path.resolve(input);
                if (fsSync.existsSync(tentative)) {
                    resolvedPath = tentative;
                } else {
                    throw new Error(`Path not found: ${tentative}`);
                }
            } else {
                // No separator: first try a directory countries/<code>/, then countries/<code>.csv
                const dirCandidate = path.resolve(COUNTRIES_DIR, input);
                if (fsSync.existsSync(dirCandidate) && fsSync.statSync(dirCandidate).isDirectory()) {
                    resolvedPath = dirCandidate;
                } else {
                    const baseName = hasCsvExt ? input : `${input}.csv`;
                    resolvedPath = path.resolve(COUNTRIES_DIR, baseName);
                }
            }

            console.log('Resolved path:', resolvedPath);

            // Verify path exists
            if (!fsSync.existsSync(resolvedPath)) {
                throw new Error(`File or directory not found: ${resolvedPath}`);
            }

            return resolvedPath;
        } catch (error) {
            console.error('Error resolving file path:', error);
            throw error;
        }
    };

    const resolvedFile = resolveFilePath(String(file));

    // Check server load before creating job
    if (runningJobs >= MAX_CONCURRENT_JOBS && jobQueue.length >= MAX_QUEUE_SIZE) {
        return res.status(503).json({ 
            error: `Сервер перегружен. Активных задач: ${runningJobs}/${MAX_CONCURRENT_JOBS}, в очереди: ${jobQueue.length}/${MAX_QUEUE_SIZE}. Попробуйте позже.` 
        });
    }

    try {
        const clientIdHeader = req.get('x-client-id');
        const job = await createJob({
            pixel,
            file: resolvedFile,
            baseUrl: effectiveBaseUrl,
            chunkSize: Number(req.body.chunkSize) || DEFAULTS.chunkSize,
            reqDelay: Number(reqDelay),
            chunkDelay: Number(req.body.chunkDelay) || DEFAULTS.chunkDelay,
            reqTimeout: Number(effectiveReqTimeout),
            sessionTimeout: Number(sessionTimeout) > 0 ? Number(sessionTimeout) : null,
            connLimit: Number(connLimit) || DEFAULTS.connLimit,
            leadsCount: Number(leadsCount) || 0,
            salesCount: Number(salesCount) || 0,
            clientId: String(clientIdHeader || clientIdBody || req.clientId || ''),
        });

        // Include both id and jobId for compatibility with UI expecting `id`
        res.status(202).json({ 
            id: job.id, 
            jobId: job.id, 
            status: job.status, 
            file: resolvedFile, 
            clientId: job.clientId,
            queuePosition: jobQueue.length > 0 ? jobQueue.length : 0
        });
    } catch (e) {
        console.error('Job creation failed:', e);
        return res.status(503).json({ error: `Не удалось создать задачу: ${e?.message || e}` });
    }
});

// Get job status
router.get('/jobs/:id', async (req, res) => {
    const job = await redisGetJob(req.params.id);
    // ... (rest of the code remains the same)
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
	await redisUpdateJobBatched(job, { force: true });
	await setCancelled(id);
	res.json(job);
});

// Mount router at base path
app.use(BASE_PATH, router);

const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || '0.0.0.0';
app.listen(PORT, HOST, () => {
	console.log(`Pixel server listening on http://${HOST}:${PORT}${BASE_PATH === '/' ? '' : BASE_PATH}`);
});

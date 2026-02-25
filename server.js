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

// ---- Global process-level error handlers to prevent crashes ----
process.on('unhandledRejection', (reason, p) => {
    try {
        console.error('[unhandledRejection] at:', p, 'reason:', reason);
    } catch {}
});
process.on('uncaughtException', (err) => {
    try {
        console.error('[uncaughtException]', err);
    } catch {}
});

// Default config
const DEFAULT_ROUTE_URL = process.env.DEFAULT_ROUTE_URL || '';
const DEFAULTS = {
	reqDelay: 100,            // задержка между запросами (мс)
	reqTimeout: 30_000,       // таймаут на один запрос (мс)
	connLimit: 10,            // максимальное число соединений (параллельных запросов)
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
// Use Redis for global job counting across all cluster processes
const MAX_CONCURRENT_JOBS = Math.max(1, Number(process.env.MAX_CONCURRENT_JOBS) || 3);
const MAX_QUEUE_SIZE = Math.max(10, Number(process.env.MAX_QUEUE_SIZE) || 50);
const REDIS_JOBS_KEY = 'pixel:global:running_jobs';
const REDIS_QUEUE_KEY = 'pixel:global:job_queue';
let runningJobs = 0; // Local counter for logging

// Memory and resource monitoring
const MEMORY_CHECK_INTERVAL = 30000; // 30 seconds
let lastMemoryCheck = 0;

async function checkMemoryUsage() {
	const now = Date.now();
	if (now - lastMemoryCheck < MEMORY_CHECK_INTERVAL) return;
	lastMemoryCheck = now;
	
	const usage = process.memoryUsage();
	const heapUsedMB = Math.round(usage.heapUsed / 1024 / 1024);
	const heapTotalMB = Math.round(usage.heapTotal / 1024 / 1024);
	const rssMB = Math.round(usage.rss / 1024 / 1024);
	
	// Get global job count from Redis
	let globalJobs = 0;
	let queueLength = 0;
	try {
		globalJobs = parseInt(await redis.get(REDIS_JOBS_KEY)) || 0;
		queueLength = await redis.llen(REDIS_QUEUE_KEY) || 0;
	} catch (e) {
		// Fallback to local count if Redis fails
		globalJobs = runningJobs;
	}
	
	console.log(`[MEMORY] RSS: ${rssMB}MB, Heap: ${heapUsedMB}/${heapTotalMB}MB, Local jobs: ${runningJobs}, Global jobs: ${globalJobs}/${MAX_CONCURRENT_JOBS}, Queue: ${queueLength}`);
	
	// Warning if memory usage is high
	if (heapUsedMB > 1000) {
		console.warn(`[WARNING] High memory usage: ${heapUsedMB}MB heap used`);
	}
}

// ---- Queue helpers ----
async function enqueueJobId(jobId) {
    try {
        await redis.rpush(REDIS_QUEUE_KEY, jobId);
    } catch (e) {
        console.error('[enqueueJobId] Redis error:', e);
        throw e;
    }
}
async function dequeueJobId() {
    try {
        return await redis.lpop(REDIS_QUEUE_KEY);
    } catch (e) {
        console.error('[dequeueJobId] Redis error:', e);
        return null;
    }
}
async function getQueueLength() {
    try {
        return await redis.llen(REDIS_QUEUE_KEY) || 0;
    } catch {
        return 0;
    }
}

async function acquireJobSlot() {
	checkMemoryUsage();
	const waitStartedAt = Date.now();
	// Try to atomically increment global job counter in Redis
	// Wait indefinitely until a slot becomes available
	while (true) {
		try {
			// Use Redis WATCH/MULTI/EXEC for atomic check-and-increment
			const currentJobs = parseInt(await redis.get(REDIS_JOBS_KEY)) || 0;
			
			if (currentJobs < MAX_CONCURRENT_JOBS) {
				// Atomically increment if still below limit
				const newCount = await redis.incr(REDIS_JOBS_KEY);
				
				// Double-check after increment (race condition protection)
				if (newCount <= MAX_CONCURRENT_JOBS) {
					runningJobs += 1; // Update local counter
					console.log(`[acquireJobSlot] Acquired slot (${newCount}/${MAX_CONCURRENT_JOBS} in use)`);
					return;
				} else {
					// We went over limit, decrement back and retry
					await redis.decr(REDIS_JOBS_KEY);
				}
			}
			// Stale-counter recovery: if waited >30s, no local jobs running, and global counter still full
			if ((Date.now() - waitStartedAt) > 30000 && runningJobs === 0 && currentJobs >= MAX_CONCURRENT_JOBS) {
				console.warn('[acquireJobSlot] Detected possible stale global counter. Resetting to 0');
				try { await redis.set(REDIS_JOBS_KEY, 0); } catch {}
			}
			
			// Wait 1 second before retrying (reduce Redis load)
			await new Promise(resolve => setTimeout(resolve, 1000));
			
		} catch (err) {
			console.error('[acquireJobSlot] Redis error:', err);
			// On Redis error, fall back to waiting
			await new Promise(resolve => setTimeout(resolve, 1000));
		}
	}
}

async function releaseJobSlot() {
	runningJobs -= 1; // Update local counter
	
	// Decrement global job counter in Redis
	try {
		await redis.decr(REDIS_JOBS_KEY);
		// Ensure it doesn't go below 0
		const count = parseInt(await redis.get(REDIS_JOBS_KEY)) || 0;
		if (count < 0) {
			await redis.set(REDIS_JOBS_KEY, 0);
		}
	} catch (err) {
		console.error('[releaseJobSlot] Redis error:', err);
	}
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

function buildUrl(baseUrl, pixel, row, folderEventType = null) {
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
    const status = folderEventType || mapStatus(row['\u0421\u0442\u0430\u0441\u0442\u0443\u0441'] ?? row['Статус']);

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

async function sendRequest({ baseUrl, pixel, row, index, reqDelay, reqTimeout, progress, jobId, fireAndForget = false, folderEventType = null }) {
	try {
		// Fast cancel check before doing anything
		if (await isCancelled(jobId)) return null;

		// Determine status to update counters (folder event type has priority)
		const status = folderEventType || mapStatus(row['\u0421\u0442\u0430\u0441\u0442\u0443\u0441'] ?? row['Статус']);
		const url = buildUrl(baseUrl, pixel, row, folderEventType);
		
		// Python-style detailed logging
		console.log(`[job ${jobId}] URL: ${url}`);
		
		// Delay between requests (like Python asyncio.sleep(0.1))
		await sleep(reqDelay);

		// Cancel after delay as well
		if (await isCancelled(jobId)) return null;

		return await withConcurrency(async () => {
			const controller = new AbortController();
			const timeoutId = setTimeout(() => controller.abort(), fireAndForget ? 5000 : reqTimeout); // Shorter timeout for fire-and-forget
			
			try {
				const fetchPromise = fetch(url, { 
					method: 'GET', 
					signal: controller.signal,
					// For RabbitMQ/queue services, we don't need to wait for full response
					...(fireAndForget && { 
						headers: { 'Connection': 'close' },
						keepalive: false 
					})
				});

				if (fireAndForget) {
					// For RabbitMQ: just check that request was sent successfully
					const response = await Promise.race([
						fetchPromise,
						new Promise((_, reject) => setTimeout(() => reject(new Error('Quick timeout')), 1000))
					]);
					
					clearTimeout(timeoutId);
					
					// If we get here, request was sent successfully
					progress.sent += 1;
					console.log(`[job ${jobId}] Отправлен запрос ${progress.sent} из ${progress.total} (fire-and-forget)`);
					console.log(`[job ${jobId}] Статус: ${response.status} (не ждем полного ответа)`);
					console.log(`[job ${jobId}] ---`);
					
					// Don't wait for response body for RabbitMQ
					return index;
				} else {
					// Normal processing - wait for full response
					const response = await fetchPromise;
					clearTimeout(timeoutId);

					if (response.ok) {
						progress.sent += 1;
						if (status === 'lead') progress.leads += 1;
						if (status === 'sale') progress.sales += 1;
						if (status === 'install') progress.installs = (progress.installs || 0) + 1;
						console.log(`[job ${jobId}] Отправлен запрос ${progress.sent} из ${progress.total}`);
						console.log(`[job ${jobId}] Статус ответа: ${response.status}`);
						console.log(`[job ${jobId}] ---`);
						return index;
					} else {
						let content = '';
						try { content = await response.text(); } catch {}
						progress.errors += 1;
						console.error(`[job ${jobId}] Ошибка при отправке запроса. Статус: ${response.status}`);
						console.error(`[job ${jobId}] Ответ сервера: ${content?.slice(0,200) ?? ''}`);
						console.error(`[job ${jobId}] ---`);
						return null;
					}
				}
			} catch (err) {
				clearTimeout(timeoutId);
				
				if (fireAndForget && (err.message === 'Quick timeout' || err.name === 'AbortError')) {
					// For fire-and-forget, timeout after sending is OK
					progress.sent += 1;
					console.log(`[job ${jobId}] Запрос отправлен ${progress.sent} из ${progress.total} (таймаут после отправки - OK для RabbitMQ)`);
					console.log(`[job ${jobId}] ---`);
					return index;
				}
				
				progress.errors += 1;
				if (err.name === 'AbortError') {
					console.error(`[job ${jobId}] Запрос отменен по таймауту (${fireAndForget ? '5000' : reqTimeout}ms)`);
				} else {
					console.error(`[job ${jobId}] Ошибка сети: ${String(err)}`);
				}
				console.error(`[job ${jobId}] ---`);
				return null;
			}
		});
	} catch (outerErr) {
		console.error(`[job ${jobId}] Unexpected error: ${String(outerErr)}`);
		return null;
	}
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
		const { id, pixel, file, baseUrl, reqDelay, reqTimeout, sessionTimeout, fireAndForget } = job;
		job.status = 'running';
		job.startedAt = new Date().toISOString();
		await redisUpdateJobBatched(job, { force: true });

		const startedAtMs = Date.now();
		const sessionDeadline = (Number(sessionTimeout) > 0) ? (startedAtMs + Number(sessionTimeout)) : null;

		console.log(`[job ${id}] Starting job (${runningJobs}/${MAX_CONCURRENT_JOBS} slots used) - file: ${file}`);
	
    // Sequential CSV processing with optional event-type subfolders (lead/sale/install)
    let csvFiles = []; // elements: { path, eventType: 'lead'|'sale'|'install'|null }
    let totalRecords = 0;
    
    try {
        const stat = await fs.stat(file);
        
        if (stat.isDirectory()) {
            console.log(`Input is a directory. Scanning CSV files in: ${file}`);
            const dirEntries = await fs.readdir(file, { withFileTypes: true });
            const eventFolders = ['lead', 'sale', 'install'];
            const hasEventFolders = dirEntries.some(e => e.isDirectory() && eventFolders.includes(e.name.toLowerCase()));

            if (hasEventFolders) {
                const wanted = job.eventType ? [String(job.eventType).toLowerCase()] : eventFolders;
                for (const entry of dirEntries) {
                    if (!entry.isDirectory()) continue;
                    const ev = entry.name.toLowerCase();
                    if (!wanted.includes(ev)) continue;
                    const subdir = path.resolve(file, entry.name);
                    const files = await fs.readdir(subdir, { withFileTypes: true });
                    for (const f of files) {
                        if (f.isFile() && f.name.toLowerCase().endsWith('.csv')) {
                            csvFiles.push({ path: path.resolve(subdir, f.name), eventType: ev });
                        }
                    }
                }
                console.log(`Found ${csvFiles.length} CSV files in event folders${job.eventType ? ` for ${job.eventType}` : ''}`);
            } else {
                csvFiles = dirEntries
                    .filter((e) => e.isFile() && e.name.toLowerCase().endsWith('.csv'))
                    .map((e) => ({ path: path.resolve(file, e.name), eventType: null }));
                console.log(`Found ${csvFiles.length} CSV files for sequential processing`);
            }
        } else {
            console.log(`Single CSV file: ${file}`);
            csvFiles = [{ path: file, eventType: null }];
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
    const sessionOpts = { baseUrl, pixel, reqDelay, reqTimeout, fireAndForget, progress: job.progress, jobId: id };
    const hasLimits = (job.limits?.leads || 0) > 0 || (job.limits?.sales || 0) > 0 || (job.limits?.installs || 0) > 0;
    let leadsLeft = job.limits?.leads || 0;
    let salesLeft = job.limits?.sales || 0;
    let installsLeft = job.limits?.installs || 0;
    let processedFiles = 0;

    for (const fileInfo of csvFiles) {
        const csvPath = fileInfo.path;
        const folderEventType = fileInfo.eventType;
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
            const etLabel = folderEventType ? ` [${folderEventType}]` : '';
            console.log(`Processing file ${processedFiles + 1}/${csvFiles.length}${etLabel}: ${path.basename(csvPath)}`);
            const csvContent = await fs.readFile(csvPath, 'utf-8');
            let records = parseWithFallbacks(csvContent);
            console.log(`Parsed ${records.length} records from ${path.basename(csvPath)}`);

            // Apply limits if specified
            if (hasLimits && (leadsLeft <= 0 && salesLeft <= 0 && installsLeft <= 0)) {
                console.log(`Limits reached, skipping remaining files`);
                break;
            }

            // Reorder records by event priority: install -> lead -> sale
            const installs = [];
            const leads = [];
            const sales = [];
            for (const rec of records) {
                const kind = folderEventType || mapStatus(rec['\u0421\u0442\u0430\u0441\u0442\u0443\u0441'] ?? rec['Статус']);
                if (kind === 'install') installs.push(rec);
                else if (kind === 'lead') leads.push(rec);
                else if (kind === 'sale') sales.push(rec);
                else leads.push(rec); // unknown -> treat as lead to not block
            }

            if (hasLimits) {
                const limited = [];
                // Respect priority when applying limits
                const prioritized = [...installs, ...leads, ...sales];
                for (const rec of prioritized) {
                    const kind = folderEventType || mapStatus(rec['\u0421\u0442\u0430\u0441\u0442\u0443\u0441'] ?? rec['Статус']);
                    if (kind === 'install' && installsLeft > 0) { limited.push(rec); installsLeft--; }
                    else if (kind === 'lead' && leadsLeft > 0) { limited.push(rec); leadsLeft--; }
                    else if (kind === 'sale' && salesLeft > 0) { limited.push(rec); salesLeft--; }
                    if (leadsLeft <= 0 && salesLeft <= 0 && installsLeft <= 0) break;
                }
                records = limited;
                console.log(`Applied limits (priority install->lead->sale): ${records.length} records selected${folderEventType ? ' (from folder: ' + folderEventType + ')' : ''} (leads left: ${leadsLeft}, sales left: ${salesLeft}, installs left: ${installsLeft})`);
            } else {
                // No limits - just process in priority order
                records = [...installs, ...leads, ...sales];
            }

            // Update total count and process records one by one
            job.progress.total += records.length;
            await redisUpdateJobBatched(job);

            // Simplified sequential processing (no chunks)
            console.log(`[job ${id}] Processing ${records.length} records sequentially...`);

            let reqCounter = 0;
            for (const row of records) {
                // Check for cancellation before each request
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

                // Skip rows without fbclid (from 'Sub ID 28' or 'Sub ID 7')
                const fbclidVal = row['Sub ID 28'] ?? row['Sub ID 7'] ?? '';
                if (!String(fbclidVal).trim()) {
                    console.log(`[job ${id}] Пропуск записи без fbclid`);
                    if (job.progress.total > 0) job.progress.total -= 1; // keep total for sendable rows only
                    continue;
                }

                // Send request and wait for it to complete
                await sendRequest({ ...sessionOpts, row, index: totalRecords + reqCounter, folderEventType });

                reqCounter++;

                // Update progress in Redis periodically to reduce load
                if (reqCounter % 10 === 0) {
                    await redisUpdateJobBatched(job);
                }

                // Delay between requests is now handled inside sendRequest
            }

            // Final progress update for the file
            await redisUpdateJobBatched(job);

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
	console.log(`[job ${id}] ${job.status} | ok=${job.progress.sent}/${job.progress.total} errors=${job.progress.errors} leads=${job.progress.leads} sales=${job.progress.sales} installs=${job.progress.installs || 0} | maxConcurrency=${MAX_CONCURRENCY} keepAliveConns=${KEEPALIVE_CONNECTIONS}`);
	
	} catch (jobError) {
		// Handle any unexpected errors in job execution
		console.error(`[job ${job.id}] Unexpected job error:`, jobError);
		job.status = 'failed';
		job.error = `Непредвиденная ошибка: ${jobError?.message || jobError}`;
		job.finishedAt = new Date().toISOString();
		await redisUpdateJobBatched(job, { force: true });
	} finally {
		// Always release the job slot
		await releaseJobSlot();
		console.log(`[job ${job.id}] Released job slot (${runningJobs}/${MAX_CONCURRENT_JOBS} slots now used)`);
	}
}

// Redis-backed job store API
async function createJob({ pixel, file, baseUrl, reqDelay, reqTimeout, sessionTimeout, connLimit, fireAndForget = false, leadsCount = 0, salesCount = 0, installsCount = 0, eventType = null, clientId = null, pushSet = null, note = null }) {
	const id = crypto.randomUUID();
	
	// Extract file/folder name for pushSet (e.g., "countries/in" -> "in", "countries/in/lead/file.csv" -> "in")
	let filePushSet = 'default';
	if (file) {
		const filePath = String(file);
		const parts = filePath.split(/[\\/]/);
		// Find "countries" folder and take the next part (geo code)
		const countriesIdx = parts.findIndex(p => p === 'countries');
		if (countriesIdx >= 0 && countriesIdx + 1 < parts.length) {
			filePushSet = parts[countriesIdx + 1];
		} else {
			// Fallback: use last directory or filename without extension
			const lastPart = parts[parts.length - 1];
			filePushSet = lastPart.replace(/\.csv$/i, '');
		}
	}
	
	const job = {
		id,
		status: 'queued',
		createdAt: new Date().toISOString(),
		startedAt: null,
		finishedAt: null,
		pixel,
		file,
		baseUrl,
		pushSet: pushSet || filePushSet,
		note: note || '',
		reqDelay,
		reqTimeout,
		sessionTimeout: (Number(sessionTimeout) > 0 ? Number(sessionTimeout) : null),
		connLimit,
		fireAndForget,
		progress: { sent: 0, total: 0, errors: 0, leads: 0, sales: 0, installs: 0, logs: [] },
		error: null,
		cancelled: false,
		limits: { leads: Number(leadsCount) || 0, sales: Number(salesCount) || 0, installs: Number(installsCount) || 0 },
		eventType: eventType ? String(eventType).toLowerCase() : null,
		clientId: clientId || crypto.randomUUID(),
	};
	await redisSaveJob(job);
	// Enqueue for later processing; files will be read only when job actually starts
	await enqueueJobId(job.id);
	return job;
}

// Background queue processor: periodically pulls jobs from Redis queue and starts them
let queueProcessorBusy = false;
async function processQueueOnce() {
	if (queueProcessorBusy) return;
	queueProcessorBusy = true;
	try {
		// Determine available capacity
		let current = 0;
		try { current = parseInt(await redis.get(REDIS_JOBS_KEY)) || 0; } catch {}
		let available = Math.max(0, MAX_CONCURRENT_JOBS - current);
		// Normal path: start up to available jobs
		while (available > 0) {
			const nextId = await dequeueJobId();
			if (!nextId) break;
			const job = await redisGetJob(nextId);
			if (!job) { available--; continue; }
			if (job.cancelled) {
				if (!['cancelled','completed','failed','completed_with_errors'].includes(job.status)) {
					job.status = 'cancelled';
					job.finishedAt = new Date().toISOString();
					await redisUpdateJobBatched(job, { force: true });
				}
				available--;
				continue;
			}
			if (job.status === 'queued') {
				job.status = 'starting';
				await redisUpdateJobBatched(job, { force: true });
			}
			// Start the job; runJob will acquire and release the slot
			runJob(job).catch(async (e) => {
				console.error(`[job ${job.id}] Unexpected error while starting:`, e);
				job.status = 'failed';
				job.error = `Непредвиденная ошибка: ${e?.message || e}`;
				job.finishedAt = new Date().toISOString();
				await redisUpdateJobBatched(job, { force: true });
				try { await releaseJobSlot(); } catch {}
			});
			available--;
		}

		// Stuck safeguard: if no available slots detected but queue has items and locally nothing runs,
		// start one job to let acquireJobSlot handle gating. This helps recover from stale global counters.
		if (available === 0) {
			let qlen = 0;
			try { qlen = await getQueueLength(); } catch {}
			if (qlen > 0 && runningJobs === 0) {
				const nextId = await dequeueJobId();
				if (nextId) {
					const job = await redisGetJob(nextId);
					if (job && !job.cancelled) {
						console.warn('[queue] No slots reported but queue is non-empty and no local jobs – starting one job as safeguard');
						if (job.status === 'queued') {
							job.status = 'starting';
							await redisUpdateJobBatched(job, { force: true });
						}
						runJob(job).catch(async (e) => {
							console.error(`[job ${job.id}] Unexpected error while starting (safeguard):`, e);
							job.status = 'failed';
							job.error = `Непредвиденная ошибка: ${e?.message || e}`;
							job.finishedAt = new Date().toISOString();
							await redisUpdateJobBatched(job, { force: true });
							try { await releaseJobSlot(); } catch {}
						});
					}
				}
			}
		}
	} finally {
		queueProcessorBusy = false;
	}
}
setInterval(processQueueOnce, 1000);
// Initialize Redis counters safely and kick the queue once on boot
async function initQueueSystem() {
	try {
		await redis.setnx(REDIS_JOBS_KEY, 0);
	} catch (e) {
		console.warn('[initQueueSystem] Failed to init global counters:', e?.message || e);
	}
	processQueueOnce().catch(() => {});
}
initQueueSystem();

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

// Small helper to avoid hanging endpoints when a backend dependency is slow
function withBackendTimeout(promise, ms = 3000) {
    return Promise.race([
        promise,
        new Promise((_, reject) => setTimeout(() => reject(new Error(`BackendTimeout ${ms}ms`)), ms)),
    ]);
}

app.use(ensureClientId);
// Serve static UI under base path
router.use(express.static(PUBLIC_DIR));

router.get('/health', (_req, res) => {
	res.json({ ok: true, serverTime: new Date().toISOString() });
});

// --- Admin helpers (guarded by ADMIN_TOKEN) ---
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
function requireAdmin(req, res, next) {
    if (!ADMIN_TOKEN) return res.status(403).json({ error: 'ADMIN_TOKEN not configured' });
    const tok = req.get('x-admin-token') || '';
    if (tok !== ADMIN_TOKEN) return res.status(403).json({ error: 'Forbidden' });
    next();
}

async function deleteByPattern(pattern) {
    let cursor = '0';
    let total = 0;
    do {
        const [next, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 1000);
        cursor = next;
        if (keys && keys.length) {
            const pipe = redis.pipeline();
            for (const k of keys) pipe.del(k);
            const res = await pipe.exec();
            for (const [, val] of res) { if (val) total += Number(val) || 0; }
        }
    } while (cursor !== '0');
    return total;
}

// Clear queue, counters and all job keys
router.post('/admin/clear-all', requireAdmin, async (_req, res) => {
    try {
        const before = {
            globalJobs: Number(await redis.get(REDIS_JOBS_KEY)) || 0,
            queueLen: await getQueueLength(),
        };
        // Reset counters and queue
        await redis.set(REDIS_JOBS_KEY, 0);
        await redis.del(REDIS_QUEUE_KEY);
        // Delete jobs and indices
        const delJobs = await deleteByPattern('job:*');
        const delIdx = await deleteByPattern('jobs:byClient:*');
        res.json({
            ok: true,
            before,
            after: { globalJobs: 0, queueLen: 0 },
            deleted: { jobs: delJobs, indices: delIdx },
        });
    } catch (e) {
        res.status(500).json({ error: String(e) });
    }
});

// Reset only counters and queue
router.post('/admin/reset-counters', requireAdmin, async (_req, res) => {
    try {
        await redis.set(REDIS_JOBS_KEY, 0);
        const q = await redis.del(REDIS_QUEUE_KEY);
        res.json({ ok: true, queueCleared: Boolean(q) });
    } catch (e) {
        res.status(500).json({ error: String(e) });
    }
});

// Root UI under base path
router.get('/', (_req, res) => {
    res.sendFile(path.resolve(process.cwd(), 'public', 'index.html'));
});

// List available country CSV files from countries/ directory
router.get('/countries', async (_req, res) => {
    try {
        const entries = await fs.readdir(COUNTRIES_DIR, { withFileTypes: true });
        // Return geo folders that contain CSV files either directly or inside event subfolders (lead/sale/install)
        const dirItems = [];
        for (const e of entries) {
            if (!e.isDirectory()) continue;
            const code = e.name;
            try {
                const dirPath = path.resolve(COUNTRIES_DIR, code);
                const inner = await fs.readdir(dirPath, { withFileTypes: true });
                const directCsv = inner.filter((f) => f.isFile() && f.name.toLowerCase().endsWith('.csv'));
                const eventFolders = ['lead', 'sale', 'install'];
                const breakdown = { lead: 0, sale: 0, install: 0 };
                let structure = 'old';
                for (const sub of inner) {
                    if (!sub.isDirectory()) continue;
                    const name = sub.name.toLowerCase();
                    if (!eventFolders.includes(name)) continue;
                    structure = 'new';
                    const subdir = path.resolve(dirPath, sub.name);
                    try {
                        const files = await fs.readdir(subdir, { withFileTypes: true });
                        breakdown[name] += files.filter(ff => ff.isFile() && ff.name.toLowerCase().endsWith('.csv')).length;
                    } catch {}
                }
                const totalCsv = directCsv.length + breakdown.lead + breakdown.sale + breakdown.install;
                if (totalCsv > 0) {
                    dirItems.push({ code, dir: path.join('countries', code), filesCount: totalCsv, eventTypes: breakdown, structure });
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
            // Use base geo code before hyphen for country display (e.g., "in-avi" -> "IN")
            const baseCode = String(it.code || '').split('-')[0].toUpperCase();
            const nameRu = display ? (display.of(baseCode) || null) : null;
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
        installsCount = 0,
        eventType = '',
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

    // Infer event type if not provided and only one counter is non-zero
    const lc = Number(leadsCount) || 0;
    const sc = Number(salesCount) || 0;
    const ic = Number(installsCount) || 0;
    let effectiveEventType = String(eventType || '').trim().toLowerCase();
    if (!effectiveEventType) {
        const nonZero = [lc > 0 ? 'lead' : null, sc > 0 ? 'sale' : null, ic > 0 ? 'install' : null].filter(Boolean);
        if (nonZero.length === 1) effectiveEventType = nonZero[0];
    }

    // Check server load before creating job (use global Redis counters)
    try {
        const globalJobs = parseInt(await redis.get(REDIS_JOBS_KEY)) || 0;
        const queueLength = await redis.llen(REDIS_QUEUE_KEY) || 0;
        
        if (globalJobs >= MAX_CONCURRENT_JOBS && queueLength >= MAX_QUEUE_SIZE) {
            return res.status(503).json({ 
                error: `Сервер перегружен. Активных задач: ${globalJobs}/${MAX_CONCURRENT_JOBS}, в очереди: ${queueLength}/${MAX_QUEUE_SIZE}. Попробуйте позже.` 
            });
        }
    } catch (redisErr) {
        console.error('[POST /send] Redis check error:', redisErr);
        // Continue on Redis error - let acquireJobSlot handle it
    }

    try {
        const clientIdHeader = req.get('x-client-id');
        const job = await createJob({
            pixel,
            file: resolvedFile,
            baseUrl: effectiveBaseUrl,
            reqDelay: Number(reqDelay),
            reqTimeout: Number(effectiveReqTimeout),
            sessionTimeout: Number(sessionTimeout) > 0 ? Number(sessionTimeout) : null,
            connLimit: Number(connLimit) || DEFAULTS.connLimit,
            fireAndForget: Boolean(req.body.fireAndForget) || false,
            leadsCount: Number(leadsCount) || 0,
            salesCount: Number(salesCount) || 0,
            installsCount: Number(installsCount) || 0,
            eventType: effectiveEventType || null,
            clientId: String(clientIdHeader || clientIdBody || req.clientId || ''),
            pushSet: req.body.pushSet || effectiveBaseUrl,
            note: req.body.note || '',
        });

        // Include both id and jobId for compatibility with UI expecting `id`
        // Compute queue position: if capacity free -> 0, else current queue length minus 1 (since our job was just enqueued at tail)
        let queuePosition = 0;
        try {
            const current = parseInt(await redis.get(REDIS_JOBS_KEY)) || 0;
            if (current >= MAX_CONCURRENT_JOBS) {
                const qlen = await getQueueLength();
                queuePosition = Math.max(0, qlen - 1);
            }
        } catch (e) {
            // Ignore Redis errors for queue position
        }
        
        res.status(202).json({ 
            id: job.id, 
            jobId: job.id, 
            status: job.status, 
            file: resolvedFile, 
            clientId: job.clientId,
            queuePosition
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

// ---- Express error handling middleware (must be after routes) ----
router.use((err, _req, res, _next) => {
    try {
        console.error('[ExpressError]', err?.stack || err);
    } catch {}
    if (res.headersSent) return;
    res.status(500).json({ error: 'Internal Server Error' });
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
	try {
		const list = await withBackendTimeout(redisListJobsByClient(clientId, 200), 7000);
		res.json(list);
	} catch (e) {
		console.error('[GET /jobs] Fallback empty list due to:', e?.message || e);
		res.json([]);
	}
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

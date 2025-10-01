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

// Redis keys for centralized job queue management
const REDIS_RUNNING_JOBS_KEY = 'global:runningJobs';
const REDIS_QUEUE_KEY = 'global:jobQueue';
const REDIS_WORKER_PREFIX = 'worker:';
const WORKER_ID = `${process.pid}-${Date.now()}`; // Unique worker identifier

// Local state - only for this worker's monitoring
let localRunningJobs = 0;

// Memory and resource monitoring
const MEMORY_CHECK_INTERVAL = 30000; // 30 seconds
let lastMemoryCheck = 0;

async function getGlobalRunningJobs() {
	const count = await redis.get(REDIS_RUNNING_JOBS_KEY);
	return parseInt(count) || 0;
}

async function getGlobalQueueLength() {
	return await redis.llen(REDIS_QUEUE_KEY);
}

function checkMemoryUsage() {
	const now = Date.now();
	if (now - lastMemoryCheck < MEMORY_CHECK_INTERVAL) return;
	lastMemoryCheck = now;
	
	const usage = process.memoryUsage();
	const heapUsedMB = Math.round(usage.heapUsed / 1024 / 1024);
	const heapTotalMB = Math.round(usage.heapTotal / 1024 / 1024);
	const rssMB = Math.round(usage.rss / 1024 / 1024);
	
	// Async fetch of global stats (non-blocking)
	Promise.all([getGlobalRunningJobs(), getGlobalQueueLength()])
		.then(([globalJobs, queueLen]) => {
			console.log(`[MEMORY] Worker ${WORKER_ID} | RSS: ${rssMB}MB, Heap: ${heapUsedMB}/${heapTotalMB}MB | Local jobs: ${localRunningJobs}, Global jobs: ${globalJobs}/${MAX_CONCURRENT_JOBS}, Queue: ${queueLen}`);
			
			// Warning if memory usage is high
			if (heapUsedMB > 1000) {
				console.warn(`[WARNING] High memory usage: ${heapUsedMB}MB heap used`);
			}
		})
		.catch(() => {
			console.log(`[MEMORY] Worker ${WORKER_ID} | RSS: ${rssMB}MB, Heap: ${heapUsedMB}/${heapTotalMB}MB | Local jobs: ${localRunningJobs}`);
		});
}

async function acquireJobSlot() {
	checkMemoryUsage();
	
	// Try to acquire a slot using Redis atomic operations
	const maxAttempts = 60; // Max 60 seconds wait
	const pollInterval = 1000; // Check every second
	
	for (let attempt = 0; attempt < maxAttempts; attempt++) {
		try {
			// Use Lua script for atomic check-and-increment
			const luaScript = `
				local key = KEYS[1]
				local max = tonumber(ARGV[1])
				local current = tonumber(redis.call('GET', key) or '0')
				if current < max then
					redis.call('INCR', key)
					return 1
				else
					return 0
				end
			`;
			
			const result = await redis.eval(luaScript, 1, REDIS_RUNNING_JOBS_KEY, MAX_CONCURRENT_JOBS);
			
			if (result === 1) {
				localRunningJobs += 1;
				console.log(`[WORKER ${WORKER_ID}] Acquired job slot (local: ${localRunningJobs}, global: ${await getGlobalRunningJobs()}/${MAX_CONCURRENT_JOBS})`);
				return;
			}
			
			// Check queue size limit
			const queueLen = await redis.llen(REDIS_QUEUE_KEY);
			if (queueLen >= MAX_QUEUE_SIZE) {
				throw new Error(`Job queue is full (${queueLen}/${MAX_QUEUE_SIZE}). Server is overloaded.`);
			}
			
			// Wait before retry
			if (attempt < maxAttempts - 1) {
				await sleep(pollInterval);
			}
		} catch (e) {
			if (e.message.includes('queue is full')) throw e;
			console.error(`[WORKER ${WORKER_ID}] Error acquiring job slot:`, e);
			await sleep(pollInterval);
		}
	}
	
	// If we couldn't acquire after max attempts, throw error
	throw new Error(`Could not acquire job slot after ${maxAttempts} attempts. Server is overloaded.`);
}

async function releaseJobSlot() {
	try {
		// Atomic decrement
		const newCount = await redis.decr(REDIS_RUNNING_JOBS_KEY);
		
		// Ensure it doesn't go below 0
		if (newCount < 0) {
			await redis.set(REDIS_RUNNING_JOBS_KEY, '0');
		}
		
		localRunningJobs = Math.max(0, localRunningJobs - 1);
		console.log(`[WORKER ${WORKER_ID}] Released job slot (local: ${localRunningJobs}, global: ${Math.max(0, newCount)}/${MAX_CONCURRENT_JOBS})`);
	} catch (e) {
		console.error(`[WORKER ${WORKER_ID}] Error releasing job slot:`, e);
	}
}

// Cleanup on worker shutdown
process.on('SIGTERM', async () => {
	console.log(`[WORKER ${WORKER_ID}] Received SIGTERM, cleaning up...`);
	// Release any slots this worker was holding
	if (localRunningJobs > 0) {
		try {
			await redis.decrby(REDIS_RUNNING_JOBS_KEY, localRunningJobs);
		} catch (e) {
			console.error(`[WORKER ${WORKER_ID}] Error during cleanup:`, e);
		}
	}
	process.exit(0);
});

process.on('SIGINT', async () => {
	console.log(`[WORKER ${WORKER_ID}] Received SIGINT, cleaning up...`);
	if (localRunningJobs > 0) {
		try {
			await redis.decrby(REDIS_RUNNING_JOBS_KEY, localRunningJobs);
		} catch (e) {
			console.error(`[WORKER ${WORKER_ID}] Error during cleanup:`, e);
		}
	}
	process.exit(0);
});

// ... (rest of the code remains the same)

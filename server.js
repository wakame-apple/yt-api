import express from "express";
import { Innertube } from "youtubei.js";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { PassThrough, Readable } from "stream";
import IORedis from "ioredis";

const app = express();
const port = process.env.PORT || 3000;

const CACHE_DIR = path.resolve(process.cwd(), "cache");
const CACHE_TTL_MS = 60 * 60 * 1000;
const MAX_CACHE_BYTES = 5 * 1024 * 1024 * 1024;
const REDIS_URL = process.env.REDIS_URL;
const FORWARD_HEADER_KEYS = new Set([
  "content-type",
  "content-length",
  "accept-ranges",
  "content-range",
  "content-disposition",
  "cache-control",
  "etag",
  "last-modified"
]);

if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });

if (!REDIS_URL) {
  console.error("REDIS_URL is required");
  process.exit(1);
}

const redis = new IORedis(REDIS_URL);
const redisSub = new IORedis(REDIS_URL);

redis.on("end", () => {
  console.error("redis disconnect");
  process.exit(1);
});
redisSub.on("end", () => {
  console.error("redis sub disconnect");
  process.exit(1);
});
redis.on("error", (e) => console.error("redis error:", e));
redisSub.on("error", (e) => console.error("redis sub error:", e));

await (async function ensureRedis() {
  try {
    await redis.ping();
  } catch (e) {
    console.error("cannot connect to redis:", e);
    process.exit(1);
  }
})();

let ytPromise = Innertube.create({ client_type: "ANDROID", generate_session_locally: true });
let yt;
async function getYT() {
  if (!yt) yt = await ytPromise;
  return yt;
}

function cacheKeyForMedia(id, itag) {
  return crypto.createHash("sha1").update(`${id}:${itag}`).digest("hex");
}
function cacheFilePath(key) {
  return path.join(CACHE_DIR, key);
}
function tmpFilePath(key) {
  return cacheFilePath(key) + ".tmp";
}
function redisInfoKey(id) {
  return `info:${id}`;
}
function redisLockName(key) {
  return `cache:lock:${key}`;
}
function redisStatusKey(key) {
  return `cache:status:${key}`;
}
function redisMetaKey(key) {
  return `cache:meta:${key}`;
}
function redisChannel(key) {
  return `cache:event:${key}`;
}

function extractUrlFromFormat(format) {
  if (!format) return null;
  if (format.url) return format.url;
  const sc = format.signatureCipher || format.signature_cipher || format.cipher || format.signatureCipher;
  if (!sc) return null;
  try {
    const params = new URLSearchParams(sc);
    const u = params.get("url") || params.get("u");
    if (!u) return null;
    return decodeURIComponent(u);
  } catch (e) {
    return null;
  }
}

async function getInfoCached(id) {
  const key = redisInfoKey(id);
  const cached = await redis.get(key);
  if (cached) {
    if (cached === "NULL") throw new Error("video not found");
    try { return JSON.parse(cached); } catch (e) {}
  }
  const ytClient = await getYT();

  try {
    const info = await ytClient.getInfo(id);
    if (info && info.streaming_data) {
      const result = { streaming_data: info.streaming_data };
      await redis.set(key, JSON.stringify(result), "PX", CACHE_TTL_MS);
      return result;
    }
  } catch (err) {
    console.error("getInfo failed:", err && err.message ? err.message : err);
  }

  try {
    const sd = await ytClient.getStreamingData(id);
    if (sd) {
      const streaming_data = sd.formats || sd.adaptive_formats ? sd : { formats: Array.isArray(sd) ? sd : [sd], adaptive_formats: [] };
      const result = { streaming_data };
      await redis.set(key, JSON.stringify(result), "PX", CACHE_TTL_MS);
      return result;
    }
  } catch (err) {
    console.error("getStreamingData fallback failed:", err && err.message ? err.message : err);
  }

  await redis.set(key, "NULL", "PX", 5 * 60 * 1000);
  throw new Error("Streaming data not available");
}

async function acquireLock(key, ttlMs = 600000) {
  const lockKey = redisLockName(key);
  const val = crypto.randomBytes(8).toString("hex");
  const ok = await redis.set(lockKey, val, "PX", ttlMs, "NX");
  return ok === "OK" ? val : null;
}
async function releaseLock(key, val) {
  const lockKey = redisLockName(key);
  const lua = `
    if redis.call("get",KEYS[1]) == ARGV[1] then
      return redis.call("del",KEYS[1])
    else
      return 0
    end
  `;
  try { await redis.eval(lua, 1, lockKey, val); } catch (e) {}
}

function waitForReadyFromRedis(key, timeoutMs = 30000) {
  const ch = redisChannel(key);
  return new Promise((resolve) => {
    let finished = false;
    function cleanup() {
      try { redisSub.unsubscribe(ch); } catch (e) {}
      redisSub.removeListener("message", onMessage);
    }
    function done(v) {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      cleanup();
      resolve(v);
    }
    function onMessage(channel, message) {
      if (channel !== ch) return;
      if (message === "ready") done(true);
      else if (message === "error") done(false);
    }
    redisSub.on("message", onMessage);
    redisSub.subscribe(ch).then(() => {
      timer = setTimeout(() => done(false), timeoutMs);
    }).catch(() => done(false));
    let timer = setTimeout(() => done(false), timeoutMs);
  });
}

async function publishReady(key, meta = {}) {
  const statusKey = redisStatusKey(key);
  const metaKey = redisMetaKey(key);
  try {
    await redis.set(statusKey, "ready", "PX", 24 * 60 * 60 * 1000);
    if (meta && typeof meta === "object") {
      await redis.hset(metaKey, {
        headers: JSON.stringify(meta.headers || {}),
        size: String(meta.size || ""),
        created: String(Date.now())
      });
    }
    await redis.publish(redisChannel(key), "ready");
  } catch (e) {}
}
async function publishError(key) {
  try {
    await redis.set(redisStatusKey(key), "error", "PX", 5 * 60 * 1000);
    await redis.publish(redisChannel(key), "error");
  } catch (e) {}
}

function parseRange(rangeHeader, size) {
  if (!rangeHeader) return null;
  const m = rangeHeader.match(/bytes=(\d*)-(\d*)/);
  if (!m) return null;
  let start = m[1] === "" ? null : parseInt(m[1], 10);
  let end = m[2] === "" ? null : parseInt(m[2], 10);
  if (start === null) {
    start = Math.max(0, size - end);
    end = size - 1;
  } else if (end === null) {
    end = size - 1;
  }
  if (Number.isNaN(start) || Number.isNaN(end) || start > end || start < 0 || end >= size) return null;
  return { start, end };
}

async function cleanCacheIfNeeded() {
  try {
    const files = fs.readdirSync(CACHE_DIR)
      .filter(f => !f.endsWith(".tmp"))
      .map(f => {
        const p = path.join(CACHE_DIR, f);
        const stat = fs.statSync(p);
        return { p, mtime: stat.mtimeMs, size: stat.size };
      })
      .sort((a, b) => a.mtime - b.mtime);
    let total = files.reduce((s, f) => s + f.size, 0);
    if (total <= MAX_CACHE_BYTES) return;
    for (const f of files) {
      try {
        fs.unlinkSync(f.p);
        total -= f.size;
        if (total <= MAX_CACHE_BYTES) break;
      } catch (e) {}
    }
  } catch (e) {}
}

const localInProgress = new Map();

app.get("/api/stream", async (req, res) => {
  try {
    const id = req.query.id;
    let itag = req.query.itag ? Number(req.query.itag) : null;
    if (!id) return res.status(400).json({ error: "id required" });

    const info = await getInfoCached(id);
    const sd = info.streaming_data || {};
    const formats = [...(sd.formats || []), ...(sd.adaptive_formats || [])].filter(Boolean);
    if (!formats.length) return res.status(404).json({ error: "no formats" });

    if (!itag) {
      const progressive = formats.find(f => (f.mime_type || f.mimeType || "").includes("video") && (f.audio_quality || f.audioBitrate || f.mime_type?.includes("audio") || f.has_audio));
      const nonDash = formats.find(f => f.itag === 22) || formats.find(f => f.itag === 18);
      if (nonDash) itag = nonDash.itag;
      else if (progressive) itag = progressive.itag;
      else {
        const dashVideo = formats.find(f => f.mime_type && f.mime_type.includes("video")) || formats[0];
        itag = dashVideo.itag;
      }
    }

    const format = formats.find(f => f.itag === itag);
    if (!format) return res.status(404).json({ error: "itag not found" });

    const url = extractUrlFromFormat(format);
    const range = req.headers.range;
    const key = cacheKeyForMedia(id, format.itag);
    const finalPath = cacheFilePath(key);
    const tmpPathFile = tmpFilePath(key);

    if (fs.existsSync(finalPath)) {
      const stat = fs.statSync(finalPath);
      const size = stat.size;
      res.setHeader("Accept-Ranges", "bytes");
      const contentType = (format.mime_type || format.mimeType || "").split(";")[0] || "application/octet-stream";
      res.setHeader("Content-Type", contentType);
      if (range) {
        const r = parseRange(range, size);
        if (!r) return res.status(416).setHeader("Content-Range", `bytes */${size}`).end();
        const { start, end } = r;
        res.status(206);
        res.setHeader("Content-Range", `bytes ${start}-${end}/${size}`);
        res.setHeader("Content-Length", String(end - start + 1));
        const stream = fs.createReadStream(finalPath, { start, end });
        stream.pipe(res);
        return;
      } else {
        res.setHeader("Content-Length", String(size));
        const stream = fs.createReadStream(finalPath);
        stream.pipe(res);
        return;
      }
    }

    if (!url) return res.status(422).json({ error: "format has no direct url" });

    if (!range) {
      const existingLocal = localInProgress.get(key);
      if (existingLocal) {
        try {
          await existingLocal.ready;
        } catch (e) {
          return res.status(502).json({ error: "upstream error" });
        }
        res.status(existingLocal.status || 200);
        for (const [k, v] of Object.entries(existingLocal.headers || {})) {
          try { res.setHeader(k, v); } catch (e) {}
        }
        existingLocal.clients.add(res);
        existingLocal.pass.pipe(res);
        const onClose = () => {
          existingLocal.clients.delete(res);
          try { existingLocal.pass.unpipe(res); } catch (e) {}
        };
        res.once("close", onClose);
        res.once("finish", onClose);
        return;
      }
    }

    const status = await redis.get(redisStatusKey(key));
    if (status === "ready") {
      await new Promise(r => setTimeout(r, 200));
      if (fs.existsSync(finalPath)) {
        const stat = fs.statSync(finalPath);
        const size = stat.size;
        res.setHeader("Accept-Ranges", "bytes");
        const contentType = (format.mime_type || format.mimeType || "").split(";")[0] || "application/octet-stream";
        res.setHeader("Content-Type", contentType);
        if (range) {
          const r = parseRange(range, size);
          if (!r) return res.status(416).setHeader("Content-Range", `bytes */${size}`).end();
          const { start, end } = r;
          res.status(206);
          res.setHeader("Content-Range", `bytes ${start}-${end}/${size}`);
          res.setHeader("Content-Length", String(end - start + 1));
          const stream = fs.createReadStream(finalPath, { start, end });
          stream.pipe(res);
          return;
        } else {
          res.setHeader("Content-Length", String(size));
          const stream = fs.createReadStream(finalPath);
          stream.pipe(res);
          return;
        }
      }
    } else if (status === "downloading") {
      const waited = await waitForReadyFromRedis(key, 30000);
      if (waited && fs.existsSync(finalPath)) {
        const stat = fs.statSync(finalPath);
        const size = stat.size;
        res.setHeader("Accept-Ranges", "bytes");
        const contentType = (format.mime_type || format.mimeType || "").split(";")[0] || "application/octet-stream";
        res.setHeader("Content-Type", contentType);
        if (range) {
          const r = parseRange(range, size);
          if (!r) return res.status(416).setHeader("Content-Range", `bytes */${size}`).end();
          const { start, end } = r;
          res.status(206);
          res.setHeader("Content-Range", `bytes ${start}-${end}/${size}`);
          res.setHeader("Content-Length", String(end - start + 1));
          const stream = fs.createReadStream(finalPath, { start, end });
          stream.pipe(res);
          return;
        } else {
          res.setHeader("Content-Length", String(size));
          const stream = fs.createReadStream(finalPath);
          stream.pipe(res);
          return;
        }
      }
    }

    if (range) {
      const upstreamHeaders = { Range: range };
      const gvRes = await fetch(url, { headers: upstreamHeaders });
      res.status(gvRes.status);
      const forwardHeaders = {};
      for (const [k, v] of gvRes.headers) {
        try {
          const lk = k.toLowerCase();
          if (FORWARD_HEADER_KEYS.has(lk)) {
            forwardHeaders[lk] = v;
            res.setHeader(lk, v);
          }
        } catch (e) {}
      }
      if (!gvRes.body) return res.status(502).json({ error: "no upstream body" });
      const upstreamStream = typeof gvRes.body.pipe === "function" ? gvRes.body : Readable.fromWeb(gvRes.body);
      upstreamStream.on("error", () => { try { res.destroy(); } catch (e) {} });
      upstreamStream.pipe(res);

      (async () => {
        const lockVal = await acquireLock(key, 10 * 60 * 1000);
        if (!lockVal) return;
        try {
          if (fs.existsSync(finalPath)) return;
          await redis.set(redisStatusKey(key), "downloading", "PX", 10 * 60 * 1000);
          const gvFull = await fetch(url);
          if (!gvFull.body) {
            await publishError(key);
            return;
          }
          const ws = fs.createWriteStream(tmpPathFile);
          const nodeStream = typeof gvFull.body.pipe === "function" ? gvFull.body : Readable.fromWeb(gvFull.body);
          const pass = new PassThrough();
          nodeStream.pipe(pass);
          pass.pipe(ws);
          await Promise.all([
            new Promise((r, j) => { nodeStream.on("end", r); nodeStream.on("error", j); }),
            new Promise((r, j) => { ws.on("finish", r); ws.on("error", j); })
          ]);
          try { ws.end(); } catch (e) {}
          if (fs.existsSync(tmpPathFile)) {
            try { fs.renameSync(tmpPathFile, finalPath); } catch (e) {}
            const size = fs.existsSync(finalPath) ? fs.statSync(finalPath).size : 0;
            await publishReady(key, { headers: forwardHeaders, size });
            await cleanCacheIfNeeded();
          }
        } catch (e) {
          await publishError(key);
        } finally {
          try { await releaseLock(key, lockVal); } catch (e) {}
        }
      })();

      return;
    }

    const lockVal = await acquireLock(key, 10 * 60 * 1000);
    if (!lockVal) {
      const waited = await waitForReadyFromRedis(key, 30000);
      if (waited && fs.existsSync(finalPath)) {
        const stat = fs.statSync(finalPath);
        const size = stat.size;
        res.setHeader("Accept-Ranges", "bytes");
        const contentType = (format.mime_type || format.mimeType || "").split(";")[0] || "application/octet-stream";
        res.setHeader("Content-Type", contentType);
        res.setHeader("Content-Length", String(size));
        const stream = fs.createReadStream(finalPath);
        stream.pipe(res);
        return;
      }
      const gvResFallback = await fetch(url);
      if (!gvResFallback.body) return res.status(502).json({ error: "no upstream body" });
      res.status(gvResFallback.status);
      for (const [k, v] of gvResFallback.headers) {
        try {
          const lk = k.toLowerCase();
          if (FORWARD_HEADER_KEYS.has(lk)) res.setHeader(lk, v);
        } catch (e) {}
      }
      const upstreamStreamFallback = typeof gvResFallback.body.pipe === "function" ? gvResFallback.body : Readable.fromWeb(gvResFallback.body);
      upstreamStreamFallback.pipe(res);
      return;
    }

    const entry = {
      pass: new PassThrough(),
      tmpPath: tmpPathFile,
      finalPath,
      headers: null,
      status: null,
      ready: null,
      resolveReady: null,
      rejectReady: null,
      clients: new Set(),
      aborted: false
    };
    let resolveR, rejectR;
    entry.ready = new Promise((r, j) => { resolveR = r; rejectR = j; });
    entry.resolveReady = resolveR;
    entry.rejectReady = rejectR;
    localInProgress.set(key, entry);

    try {
      await redis.set(redisStatusKey(key), "downloading", "PX", 10 * 60 * 1000);
    } catch (e) {}

    try {
      const gvRes = await fetch(url);
      if (!gvRes.body) {
        await publishError(key);
        localInProgress.delete(key);
        return res.status(502).json({ error: "no upstream body" });
      }
      const headersObj = {};
      for (const [k, v] of gvRes.headers) {
        try {
          const lk = k.toLowerCase();
          if (FORWARD_HEADER_KEYS.has(lk)) headersObj[lk] = v;
        } catch (e) {}
      }
      entry.headers = headersObj;
      entry.status = gvRes.status;

      const writeStream = fs.createWriteStream(tmpPathFile);
      res.status(gvRes.status);
      for (const [k, v] of Object.entries(headersObj)) {
        try { res.setHeader(k, v); } catch (e) {}
      }
      entry.clients.add(res);

      const nodeStream = typeof gvRes.body.pipe === "function" ? gvRes.body : Readable.fromWeb(gvRes.body);
      nodeStream.pipe(entry.pass);
      entry.pass.pipe(res);
      nodeStream.pipe(writeStream);

      const onClientClose = () => {
        entry.clients.delete(res);
        try { entry.pass.unpipe(res); } catch (e) {}
      };
      res.once("close", onClientClose);
      res.once("finish", onClientClose);

      await Promise.all([
        new Promise((r, j) => { nodeStream.on("end", r); nodeStream.on("error", j); }),
        new Promise((r, j) => { writeStream.on("finish", r); writeStream.on("error", j); })
      ]);

      try { writeStream.end(); } catch (e) {}
      if (fs.existsSync(tmpPathFile)) {
        try { fs.renameSync(tmpPathFile, finalPath); } catch (e) {}
        const size = fs.existsSync(finalPath) ? fs.statSync(finalPath).size : 0;
        await publishReady(key, { headers: headersObj, size });
        await cleanCacheIfNeeded();
      }
      entry.resolveReady();
      localInProgress.delete(key);
      try { await releaseLock(key, lockVal); } catch (e) {}
      return;
    } catch (e) {
      await publishError(key);
      localInProgress.delete(key);
      try { await releaseLock(key, lockVal); } catch (er) {}
      throw e;
    }
  } catch (e) {
    try { res.status(500).json({ error: String(e?.message || e) }); } catch {}
  }
});

app.listen(port, () => {
  console.log(`Server listening on ${port}`);
});
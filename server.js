import express from "express";
import { Innertube } from "youtubei.js";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import stream from "stream";
import { Readable, PassThrough } from "stream";

const { pipeline } = stream.promises;

// ---------- Configuration ----------
const PORT = process.env.PORT || 3000;
const WORKER_SECRET = process.env.WORKER_SECRET;
const UPSTREAM_TIMEOUT_MS = 30000; // 30s
const INSTANCE_BAN_MS = 5 * 60 * 1000;
const ALLOWED_WINDOW = 300;
const CACHE_DIR = path.join(process.cwd(), "cache");
const DEFAULT_PROVIDER_URL_TTL_SECONDS = 300;

if (!WORKER_SECRET) {
  console.error("WORKER_SECRET is required");
  process.exit(1);
}
if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });

const app = express();
app.use(express.json());

// ---------- Instance lists (default, override with env variables if desired) ----------
let INVIDIOUS_INSTANCES = [
  "https://inv.nadeko.net",
  "https://invidious.f5.si",
  "https://invidious.lunivers.trade",
  "https://iv.melmac.space",
  "https://yt.omada.cafe",
  "https://invidious.nerdvpn.de",
  "https://invidious.tiekoetter.com",
  "https://yewtu.be",
].join(",").split(",").map(s => s.trim()).filter(Boolean);

let PIPED_INSTANCES = [
  "https://pipedapi.kavin.rocks",
  "https://pipedapi.leptons.xyz",
  "https://pipedapi.nosebs.ru",
  "https://pipedapi-libre.kavin.rocks",
  "https://piped-api.privacy.com.de",
  "https://pipedapi.adminforge.de",
  "https://api.piped.yt",
  "https://pipedapi.drgns.space",
  "https://pipedapi.owo.si",
  "https://pipedapi.ducks.party",
  "https://piped-api.codespace.cz",
  "https://pipedapi.reallyaweso.me",
  "https://api.piped.private.coffee",
  "https://pipedapi.darkness.services",
  "https://pipedapi.orangenet.cc",
].join(",").split(",").map(s => s.trim()).filter(Boolean);

// ---------- Innertube client ----------
let ytPromise = Innertube.create({ client_type: "ANDROID", generate_session_locally: true });
let ytClient;
async function getYtClient() {
  if (!ytClient) ytClient = await ytPromise;
  return ytClient;
}

// ---------- Instance management ----------
const badInstances = new Map(); // instance -> timestamp when marked bad
const nextIndex = { invidious: 0, piped: 0 };

function markBad(instance) {
  try { badInstances.set(instance, Date.now()); console.warn("[INST] mark bad", instance); } catch {}
}
function isBad(instance) {
  const t = badInstances.get(instance);
  if (!t) return false;
  if (Date.now() - t > INSTANCE_BAN_MS) { badInstances.delete(instance); return false; }
  return true;
}

/*
  Return rotated list starting at pointer; no duplicates within returned array.
  Advance pointer so subsequent calls rotate.
*/
function getInstancesForProvider(list, providerKey) {
  if (!Array.isArray(list) || list.length === 0) return [];
  const len = list.length;
  const start = nextIndex[providerKey] % len;
  const rotated = [];
  for (let i = 0; i < len; i++) rotated.push(list[(start + i) % len]);
  nextIndex[providerKey] = (start + 1) % len;
  const good = rotated.filter(i => !isBad(i));
  return good.length ? good : rotated;
}

// ---------- Utilities ----------
function parseSignatureUrl(format) {
  if (!format) return null;
  if (format.url) return format.url;
  const sc = format.signatureCipher || format.signature_cipher || format.cipher;
  if (!sc) return null;
  try { const params = new URLSearchParams(sc); return params.get("url") || params.get("u") || null; } catch { return null; }
}

function selectBestProgressive(formats) {
  if (!Array.isArray(formats) || formats.length === 0) return null;
  const norm = formats.map(f => ({
    original: f,
    itag: f.itag,
    url: parseSignatureUrl(f) || f.url || null,
    mime: (f.mime_type || f.mimeType || f.type || "").toLowerCase(),
    has_audio: Boolean(f.has_audio || f.audioBitrate || f.audioQuality || /mp4a|aac|vorbis|opus|audio/.test((f.mime_type||"") + (f.codecs||""))),
    height: Number(f.height || (f.qualityLabel && parseInt((f.qualityLabel||"").replace(/[^0-9]/g,""),10)) || f.resolution || 0) || 0,
    bitrate: Number(f.bitrate || f.audioBitrate || 0) || 0
  }));

  const combined = norm.filter(f => f.url && f.has_audio && /video/.test(f.mime || "video"));
  if (combined.length) { combined.sort((a,b) => (b.height - a.height) || (b.bitrate - a.bitrate)); return combined[0].original; }

  const codecsCombined = norm.filter(f => f.url && /mp4a|aac|opus|vorbis/.test(f.mime));
  if (codecsCombined.length) { codecsCombined.sort((a,b) => (b.height - a.height) || (b.bitrate - a.bitrate)); return codecsCombined[0].original; }

  const videos = norm.filter(f => f.url && /video/.test(f.mime || ""));
  if (videos.length) { videos.sort((a,b) => (b.height - a.height) || (b.bitrate - a.bitrate)); return videos[0].original; }

  const any = norm.find(f => f.url);
  return any ? any.original : null;
}

async function fetchWithTimeout(url, opts = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS);
  try {
    const res = await fetch(url, { method: opts.method || "GET", headers: opts.headers, signal: controller.signal });
    clearTimeout(timeout);
    return res;
  } catch (e) { clearTimeout(timeout); throw e; }
}

// ---------- Provider fetchers (Invidious -> Piped -> Innertube) ----------
async function fetchFromInvidious(id) {
  const instances = getInstancesForProvider(INVIDIOUS_INSTANCES, "invidious");
  if (!instances.length) throw new Error("no invidious instances configured");
  let lastErr = null;
  for (const base of instances) {
    try {
      const url = `${base.replace(/\/$/, "")}/api/v1/videos/${id}`;
      console.info("[INV] trying", base);
      let resp;
      try { resp = await fetchWithTimeout(url, { headers: { "user-agent": "node-fetch" } }); } catch (e) { console.warn("[INV] fetch fail", base, e?.message || e); markBad(base); lastErr = e; continue; }
      console.info("[INV] status", base, resp.status);
      if (!resp.ok) { markBad(base); lastErr = new Error(`status ${resp.status}`); continue; }
      const data = await resp.json();
      const formats = [];
      if (Array.isArray(data.formatStreams)) for (const f of data.formatStreams) formats.push({ itag: f.itag, url: f.url, mime_type: f.type, ...f });
      if (Array.isArray(data.adaptiveFormats)) for (const f of data.adaptiveFormats) formats.push({ itag: f.itag, url: f.url, mime_type: f.type, ...f });
      if (Array.isArray(data.formats)) for (const f of data.formats) formats.push({ itag: f.itag, url: f.url, mime_type: f.mimeType || f.type, ...f });
      console.info("[INV] formats found", base, formats.length);
      if (formats.length) return { provider: "invidious", instance: base, streaming_data: { formats, adaptive_formats: [] }, rawProviderData: data };
      console.warn("[INV] no formats from", base);
      markBad(base);
    } catch (e) {
      console.warn("[INV] exception for", base, e?.message || e);
      markBad(base);
      lastErr = e;
    }
  }
  throw lastErr || new Error("invidious all instances failed");
}

async function fetchFromPiped(id) {
  const instances = getInstancesForProvider(PIPED_INSTANCES, "piped");
  if (!instances.length) throw new Error("no piped instances configured");
  let lastErr = null;
  for (const base of instances) {
    try {
      const url = `${base.replace(/\/$/, "")}/streams/${id}`;
      console.info("[PIPED] trying", base);
      let resp;
      try { resp = await fetchWithTimeout(url, { headers: { "user-agent": "node-fetch" } }); } catch (e) { console.warn("[PIPED] fetch fail", base, e?.message || e); markBad(base); lastErr = e; continue; }
      console.info("[PIPED] status", base, resp.status);
      if (!resp.ok) { markBad(base); lastErr = new Error(`status ${resp.status}`); continue; }
      const data = await resp.json();
      const formats = [];
      if (Array.isArray(data.videoStreams)) for (const v of data.videoStreams) formats.push({ itag: v.itag, url: v.url, mime_type: v.mimeType || v.type, ...v });
      if (Array.isArray(data.audioStreams)) for (const a of data.audioStreams) formats.push({ itag: a.itag, url: a.url, mime_type: a.mimeType || a.type, ...a });
      if (Array.isArray(data.formats)) for (const f of data.formats) formats.push({ itag: f.itag, url: f.url, mime_type: f.mimeType || f.type, ...f });
      console.info("[PIPED] formats found", base, formats.length);
      if (formats.length) return { provider: "piped", instance: base, streaming_data: { formats, adaptive_formats: [] }, rawProviderData: data };
      console.warn("[PIPED] no formats from", base);
      markBad(base);
    } catch (e) {
      console.warn("[PIPED] exception for", base, e?.message || e);
      markBad(base);
      lastErr = e;
    }
  }
  throw lastErr || new Error("piped all instances failed");
}

async function fetchFromInnertube(id) {
  const client = await getYtClient();
  try {
    console.info("[YT] innertube getInfo", id);
    const info = await client.getInfo(id);
    if (info && info.streaming_data) return { provider: "innertube", streaming_data: info.streaming_data, rawProviderData: info };
    console.info("[YT] trying getStreamingData", id);
    const sd = await client.getStreamingData(id);
    if (sd) {
      const streaming_data = (sd.formats || sd.adaptive_formats) ? sd : { formats: Array.isArray(sd) ? sd : [sd], adaptive_formats: [] };
      return { provider: "innertube", streaming_data, rawProviderData: sd };
    }
  } catch (e) {
    console.warn("[YT] innertube error", e?.message || e);
    throw new Error("innertube failed: " + String(e?.message || e));
  }
  throw new Error("innertube streaming data unavailable");
}

async function fetchStreamingInfo(id) {
  console.info("[FLOW] fetchStreamingInfo", id);
  try {
    const r = await fetchFromInvidious(id);
    console.info("[FLOW] selected invidious", r.instance);
    return r;
  } catch (e) {
    console.warn("[FLOW] Invidious failed:", e?.message || e);
  }
  try {
    const r = await fetchFromPiped(id);
    console.info("[FLOW] selected piped", r.instance);
    return r;
  } catch (e) {
    console.warn("[FLOW] Piped failed:", e?.message || e);
  }
  console.info("[FLOW] fallback innertube");
  return await fetchFromInnertube(id);
}

// ---------- Security ----------
function timingSafeEqualHex(aHex, bHex) {
  try { const a = Buffer.from(aHex, "hex"); const b = Buffer.from(bHex, "hex"); if (a.length !== b.length) return false; return crypto.timingSafeEqual(a, b); } catch { return false; }
}
function verifyWorkerAuth(req, res, next) {
  if (req.method === "OPTIONS") return next();
  const tsHeader = req.header("x-proxy-timestamp");
  const sigHeader = req.header("x-proxy-signature");
  if (!tsHeader || !sigHeader) return res.status(401).json({ error: "unauthorized" });
  const ts = Number(tsHeader);
  if (!Number.isFinite(ts)) return res.status(401).json({ error: "unauthorized" });
  const now = Math.floor(Date.now() / 1000);
  if (Math.abs(now - ts) > ALLOWED_WINDOW) return res.status(401).json({ error: "unauthorized" });
  const payload = `${ts}:${req.originalUrl}`;
  const expected = crypto.createHmac("sha256", WORKER_SECRET).update(payload).digest("hex");
  if (!timingSafeEqualHex(expected, sigHeader)) return res.status(401).json({ error: "unauthorized" });
  next();
}

// ---------- Active stream tracking (dedupe) ----------
// key = `${id}:${itag || 'best'}`
const activeMp4Streams = new Map(); // key -> { pass: PassThrough, clients: Set(res), headers: {}, isPartial: bool }

// ---------- file helpers ----------
function makeCachePaths(id, itag) {
  const dir = path.join(CACHE_DIR, id);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, `${itag || "best"}.mp4`);
  const tmp = file + ".download";
  return { dir, file, tmp };
}

// build provider proxy endpoints when available
function buildInvidiousProxy(instance, videoId, itag) {
  return `${instance.replace(/\/$/, "")}/api/v1/proxy?v=${encodeURIComponent(videoId)}&itag=${encodeURIComponent(itag)}`;
}
function buildPipedProxyCandidates(instance, rawUrl) {
  const base = instance.replace(/\/$/, "");
  try {
    const u = new URL(rawUrl);
    const host = u.host;
    const pathAndQuery = u.pathname + u.search;
    return [
      `${base}/proxy/videoplayback?host=${encodeURIComponent(host)}&path=${encodeURIComponent(pathAndQuery)}`,
      `${base}/proxy?url=${encodeURIComponent(rawUrl)}`,
      `${base}/proxy/videoplayback?url=${encodeURIComponent(rawUrl)}`
    ];
  } catch (e) {
    return [`${base}/proxy?url=${encodeURIComponent(rawUrl)}`];
  }
}
async function chooseWorkingProxy(candidateUrls) {
  for (const p of candidateUrls) {
    try {
      const head = await fetchWithTimeout(p, { method: "HEAD", headers: { "user-agent": "node-fetch" } });
      if (head && (head.status === 200 || head.status === 206)) return p;
    } catch (e) { /* ignore */ }
  }
  return null;
}

// provider URL memory cache (optional) keyed by `${id}:${itag}`
const providerUrlCache = new Map();
function setProviderUrlCache(key, value) { providerUrlCache.set(key, value); }
function getProviderUrlCache(key) {
  const v = providerUrlCache.get(key);
  if (!v) return null;
  if (v.expiresAt && Date.now() > v.expiresAt) { providerUrlCache.delete(key); return null; }
  return v;
}
function computeExpiresAtFromUrl(url, defaultSeconds = DEFAULT_PROVIDER_URL_TTL_SECONDS) {
  try {
    const u = new URL(url);
    const expire = u.searchParams.get("expire");
    if (expire) {
      const epoch = Number(expire) * 1000;
      if (Number.isFinite(epoch) && epoch > Date.now()) return epoch - 5000;
    }
  } catch (e) {}
  return Date.now() + defaultSeconds * 1000;
}

// ---------- core: stream+cache (Web -> Node stream conversion + dedupe) ----------
async function streamAndCacheMp4(videoUrl, cacheFile, req, res, key) {
  // key = `${id}:${itag}`
  const id = path.basename(path.dirname(cacheFile));

  // If active, join existing
  if (activeMp4Streams.has(key)) {
    const state = activeMp4Streams.get(key);
    state.clients.add(res);

    // apply headers for joining client
    try {
      if (state.headers) {
        Object.entries(state.headers).forEach(([k, v]) => res.setHeader(k, v));
        if (state.isPartial && req.headers.range) res.status(206);
      }
    } catch (e) {}

    // pipe pass to this response (do not end when upstream ends - handled globally)
    try { state.pass.pipe(res, { end: false }); } catch (e) { /* ignore */ }

    req.on('close', () => state.clients.delete(res));
    console.info("[STREAM] joined existing", key, "clients", state.clients.size);
    return;
  }

  // No active stream -> start new download and cache
  const { tmp } = makeCachePaths(id, path.basename(cacheFile, ".mp4"));
  const pass = new PassThrough();
  const clients = new Set([res]);
  const state = { pass, clients, headers: null, isPartial: false, finished: false };
  activeMp4Streams.set(key, state);

  // ensure tmp dir
  try { const dir = path.dirname(cacheFile); if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true }); } catch (e) {}

  const writeStream = fs.createWriteStream(tmp, { flags: 'a' });

  const headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "accept": "*/*",
    "referer": "https://www.youtube.com/"
  };
  if (req.headers.range) headers.Range = req.headers.range;

  let upstream;
  try {
    console.info("[STREAM] fetching upstream", videoUrl);
    upstream = await fetchWithTimeout(videoUrl, { headers });
  } catch (e) {
    console.error("[STREAM] upstream fetch failed", e?.message || e);
    for (const c of clients) try { c.status(502).end(); } catch {}
    try { writeStream.close(); } catch {}
    activeMp4Streams.delete(key);
    return;
  }

  if (!upstream || (!upstream.ok && upstream.status !== 206)) {
    console.error("[STREAM] upstream bad status", upstream?.status);
    for (const c of clients) try { c.status(502).end(); } catch {}
    try { writeStream.close(); } catch {}
    activeMp4Streams.delete(key);
    return;
  }

  // convert Web ReadableStream -> Node Readable
  let upstreamNode;
  try {
    if (!upstream.body) throw new Error("upstream.body is null");
    upstreamNode = Readable.fromWeb(upstream.body);
  } catch (e) {
    console.error("[STREAM] convert upstream body failed", e?.message || e);
    for (const c of clients) try { c.status(502).end(); } catch {}
    try { writeStream.close(); } catch {}
    activeMp4Streams.delete(key);
    return;
  }

  // prepare headers for clients
  const contentType = upstream.headers.get('content-type') || 'video/mp4';
  const contentLength = upstream.headers.get('content-length');
  const contentRange = upstream.headers.get('content-range');
  const isPartial = Boolean(contentRange || req.headers.range);

  const headersToSend = {
    'Content-Type': contentType,
    'Accept-Ranges': 'bytes',
    'Cache-Control': 'public, max-age=3600'
  };
  if (contentLength) headersToSend['Content-Length'] = contentLength;
  if (contentRange) headersToSend['Content-Range'] = contentRange;
  // Only add Transfer-Encoding chunked if content-length is not present
  if (!contentLength) headersToSend['Transfer-Encoding'] = 'chunked';

  state.headers = headersToSend;
  state.isPartial = isPartial;

  // set headers for initial client(s)
  try {
    Object.entries(headersToSend).forEach(([k, v]) => res.setHeader(k, v));
    if (isPartial && req.headers.range) res.status(206);
  } catch (e) { /* ignore header set errors */ }

  // pipe upstream -> pass -> write
  upstreamNode.pipe(pass);
  // pipe to disk (do not end disk stream when pass ends - we'll close explicitly)
  pass.pipe(writeStream, { end: false });

  // pipe pass to current clients
  for (const c of clients) {
    try { pass.pipe(c, { end: false }); } catch (e) { console.warn("[STREAM] pipe to client failed", e?.message || e); }
  }

  // handle upstream end
  upstreamNode.on('end', () => {
    try {
      // ensure file flushed and atomically rename
      writeStream.close();
      try { fs.renameSync(tmp, cacheFile); } catch (err) { console.warn("[STREAM] rename tmp->final failed", err?.message || err); }
    } catch (e) { console.warn("[STREAM] finalize error", e?.message || e); }
    for (const c of clients) try { if (!c.writableEnded) c.end(); } catch {}
    state.finished = true;
    activeMp4Streams.delete(key);
    console.info("[STREAM] finished", key);
  });

  upstreamNode.on('error', (err) => {
    for (const c of clients) try { c.destroy(err); } catch {}
    try { writeStream.close(); } catch {}
    activeMp4Streams.delete(key);
    console.error("[STREAM] upstream error", err?.message || err);
  });

  // ensure we remove clients when they disconnect
  req.on('close', () => {
    clients.delete(res);
  });
}

// ---------- serve from disk with Range handling ----------
function streamFromCache(cacheFile, req, res) {
  const stat = fs.statSync(cacheFile);
  const range = req.headers.range;
  if (!range) {
    res.writeHead(200, { 'Content-Length': stat.size, 'Content-Type': 'video/mp4', 'Accept-Ranges': 'bytes', 'Cache-Control': 'public, max-age=3600' });
    fs.createReadStream(cacheFile).pipe(res);
    return;
  }
  const parts = range.replace(/bytes=/, '').split('-');
  const start = parseInt(parts[0], 10);
  const end = parts[1] ? parseInt(parts[1], 10) : stat.size - 1;
  if (isNaN(start) || isNaN(end) || start > end || start < 0) return res.status(416).end();
  const chunkSize = (end - start) + 1;
  res.writeHead(206, { 'Content-Range': `bytes ${start}-${end}/${stat.size}`, 'Accept-Ranges': 'bytes', 'Content-Length': chunkSize, 'Content-Type': 'video/mp4', 'Cache-Control': 'public, max-age=3600' });
  fs.createReadStream(cacheFile, { start, end }).pipe(res);
}

// ---------- API endpoint: /api/stream ----------
app.get('/api/stream', verifyWorkerAuth, async (req, res) => {
  try {
    const id = req.query.id;
    if (!id) return res.status(400).json({ error: 'id required' });

    // ensure cache dir
    const base = path.join(CACHE_DIR, id);
    if (!fs.existsSync(base)) fs.mkdirSync(base, { recursive: true });

    // choose file name at itag level if multiple formats present. We'll select best progressive
    let info;
    try { info = await fetchStreamingInfo(id); } catch (e) { console.error("[API] fetchStreamingInfo failed", e?.message || e); return res.status(502).json({ error: 'no streaming info' }); }
    const sd = info.streaming_data || {};
    const formats = [...(sd.formats || []), ...(sd.adaptive_formats || [])].filter(Boolean);
    if (!formats.length) return res.status(404).json({ error: 'no formats' });
    const chosen = selectBestProgressive(formats);
    if (!chosen) return res.status(404).json({ error: 'no suitable format' });

    const chosenItag = chosen.itag || chosen.itagNo || 'best';
    const cacheFile = path.join(base, `${chosenItag}.mp4`);
    const cacheKey = `${id}:${chosenItag}`;

    // 1) If file exists on disk -> serve immediately (video-file-first policy)
    if (fs.existsSync(cacheFile)) {
      console.info("[API] file cache hit", cacheFile);
      return streamFromCache(cacheFile, req, res);
    }

    // 2) Try provider-specific proxy URL (invidious/piped) if available so we avoid IP-bound googlevideo URLs where possible
    const rawUrl = parseSignatureUrl(chosen) || chosen.url;
    if (!rawUrl) return res.status(422).json({ error: 'format has no direct url' });

    let finalUrl = rawUrl;
    let finalProvider = info.provider;
    let finalInstance = info.instance || null;

    if (info.provider === 'invidious' && info.instance) {
      // prefer the invidious proxy endpoint (same instance serves the stream)
      try {
        finalUrl = buildInvidiousProxy(info.instance, id, chosenItag);
        finalProvider = 'invidious-proxy';
        finalInstance = info.instance;
        console.info("[API] built invidious proxy url", finalUrl);
      } catch (e) { console.warn("[API] failed to build invidious proxy", e?.message || e); finalUrl = rawUrl; finalProvider = info.provider || 'invidious'; }
    }

    if (finalUrl === rawUrl && info.provider === 'piped' && info.instance) {
      try {
        const candidates = buildPipedProxyCandidates(info.instance, rawUrl);
        const ok = await chooseWorkingProxy(candidates);
        if (ok) {
          finalUrl = ok;
          finalProvider = 'piped-proxy';
          finalInstance = info.instance;
          console.info("[API] chosen piped proxy", ok);
        } else {
          console.warn("[API] no working piped proxy, using raw url");
        }
      } catch (e) { console.warn("[API] piped proxy detection error", e?.message || e); }
    }

    // Log IP binding if present
    try {
      const u = new URL(finalUrl);
      if (u.searchParams.has('ip') || u.searchParams.has('ipbits')) {
        console.warn("[API] url contains ip/ipbits — may be IP-bound", { ip: u.searchParams.get('ip'), ipbits: u.searchParams.get('ipbits') });
      }
      if (u.searchParams.has('expire')) console.info("[API] url expire (epoch)", u.searchParams.get('expire'));
    } catch (e) {}

    // Store provider URL in memory as fallback (if file missing later)
    const expiresAt = computeExpiresAtFromUrl(finalUrl, DEFAULT_PROVIDER_URL_TTL_SECONDS);
    setProviderUrlCache(cacheKey, { url: finalUrl, provider: finalProvider, instance: finalInstance, expiresAt });
    console.info("[API] cached provider url", cacheKey, "expiresAt", new Date(expiresAt).toISOString());

    // 3) Stream and cache (this will create cacheFile on completion)
    await streamAndCacheMp4(finalUrl, cacheFile, req, res, cacheKey);

  } catch (e) {
    console.error("[API] unexpected", e?.message || e);
    return res.status(500).json({ error: String(e) });
  }
});

app.listen(PORT, () => console.log(`Server listening on ${PORT}`));

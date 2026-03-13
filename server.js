import express from "express";
import { Innertube } from "youtubei.js";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import stream from "stream";

// ---------- Configuration ----------
const PORT = process.env.PORT || 3000;
const WORKER_SECRET = process.env.WORKER_SECRET;
const UPSTREAM_TIMEOUT_MS = 10_000;
const INSTANCE_BAN_MS = 5 * 60 * 1000;
const ALLOWED_WINDOW = 300;
const CACHE_DIR = path.join(process.cwd(), "cache");

if (!WORKER_SECRET) {
  console.error("WORKER_SECRET is required");
  process.exit(1);
}
if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });

const app = express();
app.use(express.json());

// ---------- Instance lists ----------
const INVIDIOUS_INSTANCES = [
  "https://inv.nadeko.net",
  "https://invidious.f5.si",
  "https://invidious.lunivers.trade",
  "https://iv.melmac.space",
  "https://yt.omada.cafe",
  "https://invidious.nerdvpn.de",
  "https://invidious.tiekoetter.com",
  "https://yewtu.be",
];
const PIPED_INSTANCES = [
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
];

// ---------- Innertube client ----------
let ytPromise = Innertube.create({ client_type: "ANDROID", generate_session_locally: true });
let ytClient;
async function getYtClient() {
  if (!ytClient) ytClient = await ytPromise;
  return ytClient;
}

// ---------- Instance management ----------
const badInstances = new Map();
const nextIndex = { invidious: 0, piped: 0 };
function markBad(instance) { try { badInstances.set(instance, Date.now()); } catch {} }
function isBad(instance) {
  const t = badInstances.get(instance);
  if (!t) return false;
  if (Date.now() - t > INSTANCE_BAN_MS) { badInstances.delete(instance); return false; }
  return true;
}
function getInstancesForProvider(list, providerKey) {
  if (!Array.isArray(list) || list.length === 0) return [];
  const idx = (nextIndex[providerKey] || 0) % list.length;
  nextIndex[providerKey] = (idx + 1) % list.length;
  const rotated = [...list.slice(idx), ...list.slice(0, idx)];
  const good = rotated.filter(i => !isBad(i));
  return good.length ? good : rotated;
}

// ---------- Utilities ----------
function parseSignatureUrl(format) {
  if (!format) return null;
  if (format.url) return format.url;
  const sc = format.signatureCipher || format.signature_cipher || format.cipher || format.signatureCipher;
  if (!sc) return null;
  try { const params = new URLSearchParams(sc); return params.get("url") || params.get("u") || null; } catch { return null; }
}

function selectBestProgressive(formats) {
  if (!Array.isArray(formats) || formats.length === 0) return null;
  const norm = formats.map(f => ({
    original: f,
    itag: f.itag,
    url: parseSignatureUrl(f) || f.url || null,
    mime: (f.mime_type || f.mimeType || "").toLowerCase(),
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

// ---------- Fetching from providers ----------
async function fetchFromInvidious(id) {
  const instances = getInstancesForProvider(INVIDIOUS_INSTANCES, "invidious");
  if (!instances.length) throw new Error("no invidious instances configured");
  for (const base of instances) {
    try {
      const url = `${base.replace(/\/$/, "")}/api/v1/videos/${id}`;
      let resp;
      try { resp = await fetchWithTimeout(url); } catch (e) { markBad(base); continue; }
      if (!resp.ok) { markBad(base); continue; }
      const data = await resp.json();
      const formats = [];
      if (Array.isArray(data.formatStreams)) for (const f of data.formatStreams) formats.push({ itag: f.itag, url: f.url, mime_type: f.type, ...f });
      if (Array.isArray(data.adaptiveFormats)) for (const f of data.adaptiveFormats) formats.push({ itag: f.itag, url: f.url, mime_type: f.type, ...f });
      if (Array.isArray(data.formats)) for (const f of data.formats) formats.push({ itag: f.itag, url: f.url, mime_type: f.mimeType || f.type, ...f });
      if (formats.length) return { provider: "invidious", streaming_data: { formats, adaptive_formats: [] } };
      markBad(base);
    } catch (e) { markBad(base); continue; }
  }
  throw new Error("invidious all instances failed");
}

async function fetchFromPiped(id) {
  const instances = getInstancesForProvider(PIPED_INSTANCES, "piped");
  if (!instances.length) throw new Error("no piped instances configured");
  for (const base of instances) {
    try {
      const url = `${base.replace(/\/$/, "")}/streams/${id}`;
      let resp;
      try { resp = await fetchWithTimeout(url); } catch (e) { markBad(base); continue; }
      if (!resp.ok) { markBad(base); continue; }
      const data = await resp.json();
      const formats = [];
      if (Array.isArray(data.videoStreams)) for (const v of data.videoStreams) formats.push({ itag: v.itag, url: v.url, mime_type: v.mimeType || v.type, ...v });
      if (Array.isArray(data.audioStreams)) for (const a of data.audioStreams) formats.push({ itag: a.itag, url: a.url, mime_type: a.mimeType || a.type, ...a });
      if (Array.isArray(data.formats)) for (const f of data.formats) formats.push({ itag: f.itag, url: f.url, mime_type: f.mimeType || f.type, ...f });
      if (formats.length) return { provider: "piped", streaming_data: { formats, adaptive_formats: [] } };
      markBad(base);
    } catch (e) { markBad(base); continue; }
  }
  throw new Error("piped all instances failed");
}

async function fetchFromInnertube(id) {
  const client = await getYtClient();
  try {
    const info = await client.getInfo(id);
    if (info && info.streaming_data) return { provider: "innertube", streaming_data: info.streaming_data };
    const sd = await client.getStreamingData(id);
    if (sd) {
      const streaming_data = (sd.formats || sd.adaptive_formats) ? sd : { formats: Array.isArray(sd) ? sd : [sd], adaptive_formats: [] };
      return { provider: "innertube", streaming_data };
    }
  } catch (e) { throw new Error("innertube failed: " + String(e?.message || e)); }
  throw new Error("innertube streaming data unavailable");
}

async function fetchStreamingInfo(id) {
  try { return await fetchFromInvidious(id); } catch (e) {}
  try { return await fetchFromPiped(id); } catch (e) {}
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

// ---------- Active stream tracking ----------
const activeMp4Streams = new Map(); // id -> { pass: PassThrough, clients: Set(res) }

// ---------- MP4 streaming & caching ----------
async function streamAndCacheMp4(videoUrl, cacheFile, req, res) {
  const id = path.basename(path.dirname(cacheFile));

  if (activeMp4Streams.has(id)) {
    const state = activeMp4Streams.get(id);
    state.clients.add(res);
    req.on('close', () => state.clients.delete(res));
    return;
  }

  const dir = path.dirname(cacheFile);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const tempFile = cacheFile + '.download';

  const pass = new stream.PassThrough();
  const clients = new Set([res]);
  activeMp4Streams.set(id, { pass, clients });

  const writeStream = fs.createWriteStream(tempFile, { flags: 'a' });

  const headers = {};
  if (req.headers.range) headers.Range = req.headers.range;

  let upstream;
  try { upstream = await fetchWithTimeout(videoUrl, { headers }); } catch (e) {
    for (const c of clients) try { c.status(502).end(); } catch {};
    writeStream.close();
    activeMp4Streams.delete(id);
    return;
  }

  if (!upstream.ok && upstream.status !== 206) {
    for (const c of clients) try { c.status(502).end(); } catch {};
    writeStream.close();
    activeMp4Streams.delete(id);
    return;
  }

  const contentType = upstream.headers.get('content-type') || 'video/mp4';
  const contentLength = upstream.headers.get('content-length');
  const contentRange = upstream.headers.get('content-range');

  const setHeadersFor = (r) => {
    try {
      r.setHeader('Content-Type', contentType);
      if (contentLength) r.setHeader('Content-Length', contentLength);
      if (contentRange) r.setHeader('Content-Range', contentRange);
      r.setHeader('Accept-Ranges', 'bytes');
      if (req.headers.range) r.status(206);
    } catch (e) {}
  };

  for (const c of clients) setHeadersFor(c);

  const upstreamStream = upstream.body;
  upstreamStream.pipe(pass);
  pass.pipe(writeStream);

  const pipeToClients = () => {
    for (const c of clients) {
      if (c.writableEnded) continue;
      pass.pipe(c, { end: false });
    }
  };
  pipeToClients();

  upstreamStream.on('end', () => {
    try { writeStream.close(); fs.renameSync(tempFile, cacheFile); } catch (e) {}
    for (const c of clients) try { if (!c.writableEnded) c.end(); } catch {}
    activeMp4Streams.delete(id);
  });

  upstreamStream.on('error', (err) => {
    for (const c of clients) try { c.destroy(err); } catch {}
    try { writeStream.close(); } catch {}
    activeMp4Streams.delete(id);
  });

  req.on('close', () => clients.delete(res));
}

function streamFromCache(cacheFile, req, res) {
  const stat = fs.statSync(cacheFile);
  const range = req.headers.range;
  if (!range) {
    res.writeHead(200, { 'Content-Length': stat.size, 'Content-Type': 'video/mp4', 'Accept-Ranges': 'bytes' });
    fs.createReadStream(cacheFile).pipe(res);
    return;
  }
  const parts = range.replace(/bytes=/, '').split('-');
  const start = parseInt(parts[0], 10);
  const end = parts[1] ? parseInt(parts[1], 10) : stat.size - 1;
  if (isNaN(start) || isNaN(end) || start > end || start < 0) return res.status(416).end();
  const chunkSize = (end - start) + 1;
  res.writeHead(206, { 'Content-Range': `bytes ${start}-${end}/${stat.size}`, 'Accept-Ranges': 'bytes', 'Content-Length': chunkSize, 'Content-Type': 'video/mp4' });
  fs.createReadStream(cacheFile, { start, end }).pipe(res);
}

// ---------- API endpoint: /api/stream ----------
// GET /api/stream?id=VIDEO_ID           -> progressive MP4 streaming (Range support)
app.get('/api/stream', verifyWorkerAuth, async (req, res) => {
  try {
    const id = req.query.id;
    if (!id) return res.status(400).json({ error: 'id required' });

    const base = path.join(CACHE_DIR, id);
    if (!fs.existsSync(base)) fs.mkdirSync(base, { recursive: true });
    const cacheFile = path.join(base, `${id}.mp4`);

    if (fs.existsSync(cacheFile)) return streamFromCache(cacheFile, req, res);

    let info;
    try { info = await fetchStreamingInfo(id); } catch (e) { return res.status(502).json({ error: 'no streaming info' }); }
    const sd = info.streaming_data || {};
    const formats = [...(sd.formats || []), ...(sd.adaptive_formats || [])].filter(Boolean);
    if (!formats.length) return res.status(404).json({ error: 'no formats' });
    const chosen = selectBestProgressive(formats);
    if (!chosen) return res.status(404).json({ error: 'no suitable format' });
    const rawUrl = parseSignatureUrl(chosen) || chosen.url;
    if (!rawUrl) return res.status(422).json({ error: 'format has no direct url' });

    await streamAndCacheMp4(rawUrl, cacheFile, req, res);

  } catch (e) {
    return res.status(500).json({ error: String(e) });
  }
});

app.listen(PORT, () => console.log(`Server listening on ${PORT}`));

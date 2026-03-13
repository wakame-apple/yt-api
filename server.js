import express from "express";
import { Innertube } from "youtubei.js";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { PassThrough, Readable } from "stream";

if (!process.env.WORKER_SECRET) {
  console.error("WORKER_SECRET is required");
  process.exit(1);
}

const app = express();
const port = process.env.PORT || 3000;
const CACHE_DIR = path.resolve(process.cwd(), "cache");
const MAX_CACHE_BYTES = 5 * 1024 * 1024 * 1024;
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
const ALLOWED_WINDOW = 300;
const WORKER_SECRET = process.env.WORKER_SECRET;
const UPSTREAM_TIMEOUT_MS = 10000;
const INSTANCE_BAN_MS = 5 * 60 * 1000;

// ---------------- Instance lists ----------------
const INVIDIOUS_INSTANCES = [
  "https://inv.nadeko.net",
  "https://invidious.f5.si",
  "https://invidious.lunivers.trade",
  "https://invidious.ducks.party",
  "https://iv.melmac.space",
  "https://yt.omada.cafe",
  "https://invidious.nerdvpn.de",
  "https://invidious.privacyredirect.com",
  "https://invidious.technicalvoid.dev",
  "https://invidious.darkness.services",
  "https://invidious.nikkosphere.com",
  "https://invidious.schenkel.eti.br",
  "https://invidious.tiekoetter.com",
  "https://invidious.perennialte.ch",
  "https://invidious.reallyaweso.me",
  "https://invidious.private.coffee",
  "https://invidious.privacydev.net",
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

if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });

let ytPromise = Innertube.create({ client_type: "ANDROID", generate_session_locally: true });
let ytClient;
async function getYtClient() {
  if (!ytClient) ytClient = await ytPromise;
  return ytClient;
}

const badInstances = new Map();
const nextIndex = { invidious: 0, piped: 0 };

function markBad(instance) {
  try { badInstances.set(instance, Date.now()); } catch {}
}
function isBad(instance) {
  const t = badInstances.get(instance);
  if (!t) return false;
  if (Date.now() - t > INSTANCE_BAN_MS) {
    badInstances.delete(instance);
    return false;
  }
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

function sha1Key(id, itag) {
  return crypto.createHash("sha1").update(`${id}:${itag}`).digest("hex");
}
function filePathForKey(key) { return path.join(CACHE_DIR, key); }
function tmpPathForKey(key) { return `${filePathForKey(key)}.tmp`; }

function parseSignatureUrl(format) {
  if (!format) return null;
  if (format.url) return format.url;
  const sc = format.signatureCipher || format.signature_cipher || format.cipher || format.signatureCipher;
  if (!sc) return null;
  try {
    const params = new URLSearchParams(sc);
    const u = params.get("url") || params.get("u");
    return u || null;
  } catch (e) {
    return null;
  }
}

function parseRangeHeader(rangeHeader, size) {
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

async function enforceCacheLimit() {
  try {
    const files = fs.readdirSync(CACHE_DIR)
      .filter(f => !f.endsWith(".tmp"))
      .map(f => {
        const p = path.join(CACHE_DIR, f);
        const s = fs.statSync(p);
        return { path: p, mtime: s.mtimeMs, size: s.size };
      })
      .sort((a, b) => a.mtime - b.mtime);
    let total = files.reduce((acc, f) => acc + f.size, 0);
    if (total <= MAX_CACHE_BYTES) return;
    for (const f of files) {
      try {
        fs.unlinkSync(f.path);
        total -= f.size;
        if (total <= MAX_CACHE_BYTES) break;
      } catch {}
    }
  } catch {}
}

async function fetchWithTimeout(url, opts = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      method: opts.method || "GET",
      headers: opts.headers,
      signal: controller.signal
    });
    clearTimeout(timeout);
    return res;
  } catch (e) {
    clearTimeout(timeout);
    throw e;
  }
}

async function fetchFromInvidious(id) {
  if (!INVIDIOUS_INSTANCES.length) throw new Error("no invidious instances configured");
  const instances = getInstancesForProvider(INVIDIOUS_INSTANCES, "invidious");
  for (const base of instances) {
    try {
      const url = `${base.replace(/\/$/, "")}/api/v1/videos/${id}`;
      let resp;
      try {
        resp = await fetchWithTimeout(url);
      } catch (e) {
        markBad(base);
        continue;
      }
      if (!resp.ok) {
        markBad(base);
        continue;
      }
      const data = await resp.json();
      const formats = [];
      if (Array.isArray(data.formatStreams)) {
        for (const f of data.formatStreams) formats.push({ itag: f.itag, url: f.url, mime_type: f.type, ...f });
      }
      if (Array.isArray(data.adaptiveFormats)) {
        for (const f of data.adaptiveFormats) formats.push({ itag: f.itag, url: f.url, mime_type: f.type, ...f });
      }
      if (Array.isArray(data.formats)) {
        for (const f of data.formats) formats.push({ itag: f.itag, url: f.url, mime_type: f.mimeType || f.type, ...f });
      }
      if (formats.length) return { streaming_data: { formats, adaptive_formats: [] } };
      markBad(base);
    } catch (e) {
      markBad(base);
      continue;
    }
  }
  throw new Error("invidious all instances failed");
}

async function fetchFromPiped(id) {
  if (!PIPED_INSTANCES.length) throw new Error("no piped instances configured");
  const instances = getInstancesForProvider(PIPED_INSTANCES, "piped");
  for (const base of instances) {
    try {
      const url = `${base.replace(/\/$/, "")}/streams/${id}`;
      let resp;
      try {
        resp = await fetchWithTimeout(url);
      } catch (e) {
        markBad(base);
        continue;
      }
      if (!resp.ok) {
        markBad(base);
        continue;
      }
      const data = await resp.json();
      const formats = [];
      if (Array.isArray(data.videoStreams)) {
        for (const v of data.videoStreams) formats.push({ itag: v.itag, url: v.url, mime_type: v.mimeType || v.type, ...v });
      }
      if (Array.isArray(data.audioStreams)) {
        for (const a of data.audioStreams) formats.push({ itag: a.itag, url: a.url, mime_type: a.mimeType || a.type, ...a });
      }
      if (Array.isArray(data.formats)) {
        for (const f of data.formats) formats.push({ itag: f.itag, url: f.url, mime_type: f.mimeType || f.type, ...f });
      }
      if (formats.length) return { streaming_data: { formats, adaptive_formats: [] } };
      markBad(base);
    } catch (e) {
      markBad(base);
      continue;
    }
  }
  throw new Error("piped all instances failed");
}

async function fetchFromInnertube(id) {
  const client = await getYtClient();
  try {
    const info = await client.getInfo(id);
    if (info && info.streaming_data) return { streaming_data: info.streaming_data };
    const sd = await client.getStreamingData(id);
    if (sd) {
      const streaming_data = (sd.formats || sd.adaptive_formats) ? sd
        : { formats: Array.isArray(sd) ? sd : [sd], adaptive_formats: [] };
      return { streaming_data };
    }
  } catch (e) {
    throw new Error("innertube failed: " + String(e?.message || e));
  }
  throw new Error("innertube streaming data unavailable");
}

async function fetchStreamingInfo(id) {
  try { return await fetchFromInvidious(id); } catch (e) {}
  try { return await fetchFromPiped(id); } catch (e) {}
  return await fetchFromInnertube(id);
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
    bitrate: Number(f.bitrate || f.audioBitrate || f.bitrateKbps || 0) || 0
  })).filter(Boolean);

  const combined = norm.filter(f => f.url && f.has_audio && /video/.test(f.mime || "video"));
  if (combined.length) {
    combined.sort((a,b) => (b.height - a.height) || (b.bitrate - a.bitrate));
    return combined[0].original;
  }
  const codecsCombined = norm.filter(f => f.url && /mp4a|aac|opus|vorbis/.test(f.mime));
  if (codecsCombined.length) {
    codecsCombined.sort((a,b) => (b.height - a.height) || (b.bitrate - a.bitrate));
    return codecsCombined[0].original;
  }
  const videos = norm.filter(f => f.url && /video/.test(f.mime || ""));
  if (videos.length) {
    videos.sort((a,b) => (b.height - a.height) || (b.bitrate - a.bitrate));
    return videos[0].original;
  }
  const any = norm.find(f => f.url);
  return any ? any.original : null;
}

const inProgress = new Map();

async function startSharedDownload(key, url) {
  if (inProgress.has(key)) return inProgress.get(key);
  const tmpPath = tmpPathForKey(key);
  const finalPath = filePathForKey(key);
  const entry = {
    pass: new PassThrough(),
    tmpPath,
    finalPath,
    headers: null,
    status: null,
    ready: null,
    resolve: null,
    reject: null,
    clients: new Set()
  };
  let resolveFn, rejectFn;
  entry.ready = new Promise((r, j) => { resolveFn = r; rejectFn = j; });
  entry.resolve = resolveFn;
  entry.reject = rejectFn;
  inProgress.set(key, entry);

  (async () => {
    try {
      let resp;
      try {
        resp = await fetchWithTimeout(url);
      } catch (e) {
        try { entry.reject(e); } catch {}
        inProgress.delete(key);
        for (const r of entry.clients) {
          try { if (!r.headersSent) r.status(502).json({ error: "upstream network error" }); else r.end(); } catch {}
        }
        return;
      }

      if (!resp.ok) {
        try { entry.reject(new Error(`upstream status ${resp.status}`)); } catch {}
        inProgress.delete(key);
        for (const r of entry.clients) {
          try { if (!r.headersSent) r.status(502).json({ error: "upstream failed", status: resp.status }); else r.end(); } catch {}
        }
        return;
      }

      const headersObj = {};
      for (const [k, v] of resp.headers) {
        try {
          const lk = k.toLowerCase();
          if (FORWARD_HEADER_KEYS.has(lk)) headersObj[lk] = v;
        } catch {}
      }
      entry.headers = headersObj;
      entry.status = resp.status;

      if (!resp.body) {
        try { entry.reject(new Error("no body")); } catch {}
        inProgress.delete(key);
        for (const r of entry.clients) {
          try { if (!r.headersSent) r.status(502).json({ error: "no upstream body" }); else r.end(); } catch {}
        }
        return;
      }

      const ws = fs.createWriteStream(tmpPath);
      const stream = typeof resp.body.pipe === "function" ? resp.body : Readable.fromWeb(resp.body);

      stream.on("error", e => {
        try { ws.destroy(e); } catch {}
        try { entry.pass.destroy(e); } catch {}
        try { entry.reject(e); } catch {}
      });
      ws.on("error", e => {
        try { stream.destroy(e); } catch {}
        try { entry.pass.destroy(e); } catch {}
        try { entry.reject(e); } catch {}
      });

      stream.pipe(entry.pass);
      stream.pipe(ws);

      await Promise.all([
        new Promise((r, j) => { stream.on("end", r); stream.on("error", j); }),
        new Promise((r, j) => { ws.on("finish", r); ws.on("error", j); })
      ]);

      try { ws.end(); } catch {}
      if (fs.existsSync(tmpPath)) {
        try { fs.renameSync(tmpPath, finalPath); } catch {}
        try { entry.resolve(); } catch {}
        inProgress.delete(key);
        await enforceCacheLimit();
      } else {
        try { entry.reject(new Error("tmp missing")); } catch {}
        inProgress.delete(key);
      }
    } catch (e) {
      try { entry.reject(e); } catch {}
      inProgress.delete(key);
      for (const r of entry.clients) {
        try { if (!r.headersSent) r.status(502).json({ error: "download error" }); else r.end(); } catch {}
      }
    }
  })();

  return entry;
}

function attachClientToEntry(entry, res) {
  try {
    if (entry.headers && !res.headersSent) {
      try {
        res.status(entry.status || 200);
        for (const [k, v] of Object.entries(entry.headers)) {
          try { res.setHeader(k, v); } catch {}
        }
        if (!res.getHeader("accept-ranges")) res.setHeader("Accept-Ranges", "bytes");
      } catch {}
    } else if (!res.headersSent) {
      res.status(200);
      res.setHeader("Accept-Ranges", "bytes");
    }

    entry.clients.add(res);
    entry.pass.pipe(res);

    const onClose = () => {
      entry.clients.delete(res);
      try { entry.pass.unpipe(res); } catch {}
    };
    res.once("close", onClose);
    res.once("finish", onClose);
  } catch (e) {
    try { res.destroy(); } catch {}
  }
}

function timingSafeEqualHex(aHex, bHex) {
  try {
    const a = Buffer.from(aHex, "hex");
    const b = Buffer.from(bHex, "hex");
    if (a.length !== b.length) return false;
    return crypto.timingSafeEqual(a, b);
  } catch (e) {
    return false;
  }
}
function verifyWorkerAuth(req, res, next) {
  if (req.method === "OPTIONS") return next();
  const tsHeader = req.header("x-proxy-timestamp");
  const sigHeader = req.header("x-proxy-signature");
  if (!tsHeader || !sigHeader) {
    console.error("auth failed: missing headers");
    return res.status(401).json({ error: "unauthorized" });
  }
  const ts = Number(tsHeader);
  if (!Number.isFinite(ts)) {
    console.error("auth failed: invalid timestamp header");
    return res.status(401).json({ error: "unauthorized" });
  }
  const now = Math.floor(Date.now() / 1000);
  if (Math.abs(now - ts) > ALLOWED_WINDOW) {
    console.error("auth failed: timestamp outside allowed window");
    return res.status(401).json({ error: "unauthorized" });
  }
  const payload = `${ts}:${req.originalUrl}`;
  const expected = crypto.createHmac("sha256", WORKER_SECRET).update(payload).digest("hex");
  if (!timingSafeEqualHex(expected, sigHeader)) {
    console.error("auth failed: signature mismatch");
    return res.status(401).json({ error: "unauthorized" });
  }
  next();
}

app.get("/api/stream", verifyWorkerAuth, async (req, res) => {
  try {
    const id = req.query.id;
    if (!id) return res.status(400).json({ error: "id required" });

    let info;
    try {
      info = await fetchStreamingInfo(id);
    } catch (e) {
      console.error("fetchStreamingInfo failed:", e?.message || e);
      return res.status(502).json({ error: "no streaming info" });
    }

    const sd = info.streaming_data || {};
    const formats = [...(sd.formats || []), ...(sd.adaptive_formats || [])].filter(Boolean);
    if (!formats.length) return res.status(404).json({ error: "no formats" });

    const chosen = selectBestProgressive(formats);
    if (!chosen) return res.status(404).json({ error: "no suitable format" });

    let itag = req.query.itag ? Number(req.query.itag) : chosen.itag;
    const format = formats.find(f => f.itag === itag) || chosen;
    const url = parseSignatureUrl(format) || format.url;
    if (!url) return res.status(422).json({ error: "format has no direct url" });

    const key = sha1Key(id, format.itag);
    const finalPath = filePathForKey(key);
    const tmpPath = tmpPathForKey(key);
    const range = req.headers.range;

    if (fs.existsSync(finalPath)) {
      const stat = fs.statSync(finalPath);
      const size = stat.size;
      res.setHeader("Accept-Ranges", "bytes");
      const contentType = (format.mime_type || format.mimeType || "").split(";")[0] || "application/octet-stream";
      res.setHeader("Content-Type", contentType);

      if (range) {
        const r = parseRangeHeader(range, size);
        if (!r) return res.status(416).setHeader("Content-Range", `bytes */${size}`).end();
        const { start, end } = r;
        res.status(206);
        res.setHeader("Content-Range", `bytes ${start}-${end}/${size}`);
        res.setHeader("Content-Length", String(end - start + 1));
        const stream = fs.createReadStream(finalPath, { start, end });
        stream.on("error", () => { try { res.destroy(); } catch (e) {} });
        stream.pipe(res);
        return;
      } else {
        res.setHeader("Content-Length", String(size));
        const stream = fs.createReadStream(finalPath);
        stream.on("error", () => { try { res.destroy(); } catch (e) {} });
        stream.pipe(res);
        return;
      }
    }

    const ongoing = inProgress.get(key);
    if (ongoing) {
      if (range) {
        try {
          let upstream;
          try {
            upstream = await fetchWithTimeout(url, { headers: { Range: range } });
          } catch (e) {
            return res.status(502).json({ error: "upstream network error" });
          }
          res.status(upstream.status);
          for (const [k, v] of upstream.headers) {
            try {
              const lk = k.toLowerCase();
              if (FORWARD_HEADER_KEYS.has(lk)) res.setHeader(lk, v);
            } catch {}
          }
          if (!upstream.body) return res.status(502).json({ error: "no upstream body" });
          const upstreamStream = typeof upstream.body.pipe === "function" ? upstream.body : Readable.fromWeb(upstream.body);
          upstreamStream.on("error", () => { try { res.destroy(); } catch (e) {} });
          upstreamStream.pipe(res);
          startSharedDownload(key, url).catch(()=>{});
          return;
        } catch (e) {
          return res.status(502).json({ error: "upstream error" });
        }
      } else {
        attachClientToEntry(ongoing, res);
        return;
      }
    }

    if (range) {
      startSharedDownload(key, url).catch(()=>{});
      try {
        let upstream;
        try {
          upstream = await fetchWithTimeout(url, { headers: { Range: range } });
        } catch (e) {
          return res.status(502).json({ error: "upstream network error" });
        }
        res.status(upstream.status);
        for (const [k, v] of upstream.headers) {
          try {
            const lk = k.toLowerCase();
            if (FORWARD_HEADER_KEYS.has(lk)) res.setHeader(lk, v);
          } catch {}
        }
        if (!upstream.body) return res.status(502).json({ error: "no upstream body" });
        const upstreamStream = typeof upstream.body.pipe === "function" ? upstream.body : Readable.fromWeb(upstream.body);
        upstreamStream.on("error", () => { try { res.destroy(); } catch (e) {} });
        upstreamStream.pipe(res);
        return;
      } catch (e) {
        return res.status(502).json({ error: "upstream error" });
      }
    } else {
      const entry = await startSharedDownload(key, url);
      attachClientToEntry(entry, res);
      return;
    }
  } catch (e) {
    console.error("unexpected handler error:", String(e?.message || e));
    try { res.status(500).json({ error: String(e?.message || e) }); } catch {}
  }
});

app.listen(port, () => {
  console.log(`Server listening on ${port}`);
});

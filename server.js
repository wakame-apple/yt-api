import 'dotenv/config';
import express from 'express';
import crypto from 'crypto';
import { spawn } from 'node:child_process';

// =====================================================
// Config
// =====================================================
const {
  WORKER_SECRET,
  PORT = 3000,
  PROXY_URL,
} = process.env;

if (!WORKER_SECRET) {
  console.error('WORKER_SECRET is required');
  process.exit(1);
}

const app = express();
const port = Number(PORT) || 3000;

const CONFIG = {
  ytDlpBin: '/opt/venv/bin/yt-dlp',
  ytTimeoutMs: 15_000,
  requestTimeoutMs: 5_000,
  instanceBanMs: 5 * 60 * 1000,
  allowedWindowSeconds: 300,
  cacheTtlShortMs: 10 * 60 * 1000,
  cacheTtlLongMs: 4 * 60 * 60 * 1000,
  videoIdRegex: /^[a-zA-Z0-9_-]{11}$/,
};

const INVIDIOUS_INSTANCES = [
  'https://inv.nadeko.net',
  'https://invidious.f5.si',
  'https://invidious.lunivers.trade',
  'https://iv.melmac.space',
  'https://yt.omada.cafe',
  'https://invidious.nerdvpn.de',
  'https://invidious.tiekoetter.com',
  'https://yewtu.be',
];

const MANIFEST_KEYS = [
  'hlsManifestUrl',
  'hls_manifest_url',
  'hlsUrl',
  'hls',
  'dashManifestUrl',
  'dash_manifest_url',
  'manifest_url',
  'manifestUrl',
];

// =====================================================
// Middleware: auth
// =====================================================
function safeEqualHex(a, b) {
  try {
    const A = Buffer.from(String(a), 'hex');
    const B = Buffer.from(String(b), 'hex');
    if (A.length !== B.length) return false;
    return crypto.timingSafeEqual(A, B);
  } catch {
    return false;
  }
}

function verifyWorkerAuth(req, res, next) {
  const ts = req.header('x-proxy-timestamp');
  const sig = req.header('x-proxy-signature');

  if (!ts || !sig) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  const now = Math.floor(Date.now() / 1000);
  const t = Number(ts);

  if (!Number.isFinite(t) || Math.abs(now - t) > CONFIG.allowedWindowSeconds) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  const payload = `${ts}:${req.originalUrl}`;
  const expected = crypto.createHmac('sha256', WORKER_SECRET).update(payload).digest('hex');

  if (!safeEqualHex(expected, sig)) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  next();
}

app.use((req, _res, next) => {
  req.setTimeout?.(CONFIG.ytTimeoutMs + 5_000);
  next();
});

// =====================================================
// Cache
// =====================================================
const CACHE = new Map();

function cleanupCache() {
  const now = Date.now();
  for (const [id, entry] of CACHE.entries()) {
    if (now - entry.ts >= entry.ttl) {
      CACHE.delete(id);
    }
  }
}

function getCache(id) {
  cleanupCache();
  const entry = CACHE.get(id);
  if (!entry) return null;

  if (Date.now() - entry.ts >= entry.ttl) {
    CACHE.delete(id);
    return null;
  }

  return entry.data;
}

function setCache(id, data) {
  const ttl = data?.formats?.length >= 12
    ? CONFIG.cacheTtlLongMs
    : CONFIG.cacheTtlShortMs;

  CACHE.set(id, {
    ts: Date.now(),
    ttl,
    data,
  });
}

setInterval(cleanupCache, 5 * 60 * 1000).unref?.();

// =====================================================
// Helpers
// =====================================================
function isValidVideoId(id) {
  return typeof id === 'string' && CONFIG.videoIdRegex.test(id);
}

function toNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function mimeOf(f = {}) {
  return String(f.mimeType || f.mime_type || f.type || '').toLowerCase();
}

function isStoryboardFormat(f = {}) {
  const mime = mimeOf(f);
  return (
    f.format_note === 'storyboard' ||
    f.protocol === 'mhtml' ||
    f.ext === 'mhtml' ||
    mime.includes('mhtml')
  );
}

function parseUrlFromFormat(format) {
  if (!format) return null;
  if (typeof format === 'string') return format;
  if (format.url) return format.url;

  const cipher = format.signatureCipher || format.signature_cipher || format.cipher;
  if (!cipher) return null;

  try {
    const params = new URLSearchParams(cipher);
    return params.get('url');
  } catch {
    return null;
  }
}

function isUsableFormat(format = {}) {
  const url = parseUrlFromFormat(format);
  if (!url) return false;
  if (isStoryboardFormat(format)) return false;
  return true;
}

function normalizeOneFormat(f = {}) {
  const url = parseUrlFromFormat(f);

  return {
    format_id: f.format_id || f.itag || null,
    ext: f.ext || null,
    resolution: f.resolution || null,
    fps: toNumber(f.fps),
    height: toNumber(f.height),
    width: toNumber(f.width),
    tbr: toNumber(f.tbr || f.bitrate || f.averageBitrate || f.bandwidth),
    abr: toNumber(f.abr || f.audioBitrate || f.audio_bitrate),
    acodec: String(f.acodec || '').toLowerCase(),
    vcodec: String(f.vcodec || '').toLowerCase(),
    protocol: f.protocol || null,
    format_note: f.format_note || null,
    mime: mimeOf(f),
    url,
  };
}

function collectRawFormats(raw = {}) {
  return [
    ...(Array.isArray(raw.formats) ? raw.formats : []),
    ...(Array.isArray(raw.formatStreams) ? raw.formatStreams : []),
    ...(Array.isArray(raw.adaptiveFormats) ? raw.adaptiveFormats : []),
    ...(Array.isArray(raw.streamingData?.formats) ? raw.streamingData.formats : []),
  ];
}

function normalizeFormats(raw = {}) {
  return collectRawFormats(raw)
    .filter(isUsableFormat)
    .map(normalizeOneFormat)
    .filter((f) => Boolean(f.url));
}

function extractTitle(raw = {}) {
  return (
    raw.title ||
    raw.videoDetails?.title ||
    raw.video_details?.title ||
    raw.player_response?.videoDetails?.title ||
    raw.basic_info?.title ||
    raw.microformat?.title?.simpleText ||
    raw.titleText?.runs?.map?.((x) => x.text).join('') ||
    raw.video?.title ||
    null
  );
}

function hasManifestInSd(sd = {}) {
  return (
    MANIFEST_KEYS.some((k) => Boolean(sd[k])) ||
    MANIFEST_KEYS.some((k) => Boolean(sd.streamingData?.[k]))
  );
}

function isHlsFormat(f = {}) {
  const url = f.url || '';
  const mime = f.mime || '';
  return (
    url.includes('.m3u8') ||
    mime.includes('mpegurl') ||
    mime.includes('application/vnd.apple.mpegurl')
  );
}

function isLiveLike(raw = {}, sd = {}, formats = []) {
  return Boolean(
    raw.is_live ||
    raw.live_status === 'is_live' ||
    raw.liveNow ||
    raw.isLive ||
    raw.live ||
    sd.isLive ||
    hasManifestInSd(sd) ||
    formats.some(isHlsFormat)
  );
}

function selectBestVideo(formats = []) {
  return (
    formats
      .filter((f) => f.url && f.vcodec && f.vcodec !== 'none' && (!f.acodec || f.acodec === 'none'))
      .sort((a, b) =>
        (b.height || 0) - (a.height || 0) ||
        (b.tbr || b.bitrate || 0) - (a.tbr || a.bitrate || 0) ||
        (b.fps || 0) - (a.fps || 0)
      )[0] || null
  );
}

function selectBestAudio(formats = []) {
  return (
    formats
      .filter((f) => f.url && f.acodec && f.acodec !== 'none' && (!f.vcodec || f.vcodec === 'none'))
      .sort((a, b) =>
        (b.abr || b.tbr || b.bitrate || 0) - (a.abr || a.tbr || a.bitrate || 0)
      )[0] || null
  );
}

function buildProvider(name, url = null) {
  return { name, url };
}

function buildDashResponse(id, title, provider, formats) {
  const video = selectBestVideo(formats);
  const audio = selectBestAudio(formats);

  if (!video || !audio) {
    return null;
  }

  return {
    id,
    title: title || '',
    resourcetype: 'dash',
    videourl: video.url,
    audiourl: audio.url,
    provider,
    videoformat: {
      format_id: video.format_id,
      ext: video.ext,
      resolution: video.resolution,
      height: video.height,
      width: video.width,
      fps: video.fps,
      tbr: video.tbr,
      vcodec: video.vcodec,
      acodec: video.acodec,
    },
    audioformat: {
      format_id: audio.format_id,
      ext: audio.ext,
      resolution: audio.resolution,
      height: audio.height,
      width: audio.width,
      fps: audio.fps,
      tbr: audio.tbr,
      abr: audio.abr,
      vcodec: audio.vcodec,
      acodec: audio.acodec,
    },
  };
}

// =====================================================
// yt-dlp
// =====================================================
function parseYtDlpJson(stdout) {
  const text = String(stdout || '').trim();
  if (!text) {
    throw new Error('yt-dlp returned empty output');
  }

  const lines = text.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);

  for (let i = lines.length - 1; i >= 0; i -= 1) {
    try {
      return JSON.parse(lines[i]);
    } catch {
      // continue
    }
  }

  const first = text.indexOf('{');
  const last = text.lastIndexOf('}');
  if (first !== -1 && last !== -1 && last > first) {
    return JSON.parse(text.slice(first, last + 1));
  }

  throw new Error('yt-dlp returned invalid JSON');
}

function runYtDlp(videoId, { useProxy = false } = {}) {
  const url = `https://www.youtube.com/watch?v=${videoId}`;
  const args = [
    '--dump-single-json',
    '--no-playlist',
    '--no-warnings',
    '--no-progress',
    '--skip-download',
    '--extractor-args',
    'youtube:player_client=android,web,ios,tv_embedded',
    '--no-call-home',
  ];

  if (useProxy && PROXY_URL) {
    args.push('--proxy', PROXY_URL);
  }

  args.push(url);

  return new Promise((resolve, reject) => {
    const child = spawn(CONFIG.ytDlpBin, args, {
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    let stdout = '';
    let stderr = '';

    const timer = setTimeout(() => {
      child.kill('SIGKILL');
    }, CONFIG.ytTimeoutMs);

    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString('utf8');
    });

    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString('utf8');
    });

    child.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });

    child.on('close', (code, signal) => {
      clearTimeout(timer);

      if (code !== 0) {
        return reject(
          new Error(`yt-dlp failed (${code ?? signal ?? 'unknown'}): ${stderr.trim() || 'no stderr output'}`)
        );
      }

      try {
        resolve(parseYtDlpJson(stdout));
      } catch (err) {
        reject(err);
      }
    });
  });
}

// =====================================================
// Invidious
// =====================================================
const badInstances = new Map();

function markInstanceBad(instance) {
  badInstances.set(instance, Date.now());
}

function isInstanceHealthy(instance) {
  const ts = badInstances.get(instance);
  if (!ts) return true;

  if (Date.now() - ts > CONFIG.instanceBanMs) {
    badInstances.delete(instance);
    return true;
  }

  return false;
}

function rotateInstances(list = []) {
  if (!Array.isArray(list) || list.length === 0) return [];
  const healthy = list.filter(isInstanceHealthy);
  return healthy.length ? healthy : list;
}

async function fastestFetch(instances, buildUrl, parser) {
  if (!instances?.length) throw new Error('no instances');

  const controllers = [];

  const tasks = instances.map((base) => (async () => {
    const controller = new AbortController();
    controllers.push(controller);

    const timeout = setTimeout(() => controller.abort(), CONFIG.requestTimeoutMs);

    try {
      const res = await fetch(buildUrl(base), { signal: controller.signal });
      clearTimeout(timeout);

      if (!res.ok) {
        markInstanceBad(base);
        throw new Error(`bad response ${res.status} from ${base}`);
      }

      const json = await res.json();
      const parsed = parser(json);

      if (!parsed) {
        markInstanceBad(base);
        throw new Error(`parse failed from ${base}`);
      }

      return { instance: base, data: parsed };
    } catch (err) {
      markInstanceBad(base);
      throw err;
    }
  })());

  try {
    const result = await Promise.any(tasks);
    controllers.forEach((c) => c.abort());
    return result;
  } catch {
    controllers.forEach((c) => c.abort());
    throw new Error('All instances failed');
  }
}

async function fetchFromInvidious(videoId) {
  const instances = rotateInstances(INVIDIOUS_INSTANCES);

  const result = await fastestFetch(
    instances,
    (base) => `${base.replace(/\/$/, '')}/api/v1/videos/${videoId}`,
    (data) => {
      const formats = [
        ...(Array.isArray(data.formatStreams) ? data.formatStreams : []),
        ...(Array.isArray(data.adaptiveFormats) ? data.adaptiveFormats : []),
        ...(Array.isArray(data.streamingData?.formats) ? data.streamingData.formats : []),
      ];

      const sd = { formats };
      const is_live = Boolean(
        data.liveNow || data.isLive || data.is_live || data.live || data.streamingData?.isLive
      );

      return { raw: data, streaming_data: sd, is_live };
    }
  );

  const raw = result.data.raw;
  const sd = result.data.streaming_data;
  const formats = normalizeFormats(raw);

  return {
    provider: 'invidious',
    instance: result.instance,
    raw,
    streaming_data: sd,
    formats,
    is_live: result.data.is_live,
  };
}

// =====================================================
// Source orchestration
// priority: yt-dlp(proxy) -> invidious -> yt-dlp(direct)
// =====================================================
async function fetchFromYtDlpSource(videoId, { useProxy = false } = {}) {
  const raw = await runYtDlp(videoId, { useProxy });
  const formats = normalizeFormats(raw);
  const streaming_data = {
    formats: Array.isArray(raw.formats) ? raw.formats : [],
    streamingData: raw.streamingData && typeof raw.streamingData === 'object' ? raw.streamingData : undefined,
  };

  return {
    provider: 'yt-dlp',
    instance: useProxy ? (PROXY_URL || null) : null,
    raw,
    streaming_data,
    formats,
    is_live: isLiveLike(raw, streaming_data, formats),
  };
}

async function fetchStreamingInfo(videoId) {
  if (PROXY_URL) {
    try {
      return await fetchFromYtDlpSource(videoId, { useProxy: true });
    } catch (e) {
      console.warn('proxied yt-dlp failed, falling back to invidious:', e?.message || e);
    }
  }

  try {
    const inv = await fetchFromInvidious(videoId);
    const invFormats = normalizeFormats(inv.raw);

    if (invFormats.length) {
      return { ...inv, formats: invFormats };
    }

    console.warn('invidious returned no usable formats, falling back to direct yt-dlp');
  } catch (e) {
    console.warn('invidious failed, falling back to direct yt-dlp:', e?.message || e);
  }

  return fetchFromYtDlpSource(videoId, { useProxy: false });
}

// =====================================================
// API handlers
// =====================================================
async function handleStreamRequest(req, res) {
  try {
    const id = String(req.params.video_id || '');
    if (!id) return res.status(400).json({ error: 'id required' });
    if (!isValidVideoId(id)) return res.status(400).json({ error: 'invalid video id' });

    const cached = getCache(id);
    if (cached) {
      return res.json(cached);
    }

    const info = await fetchStreamingInfo(id);

    const rawFormats = info.formats?.length ? info.formats : normalizeFormats(info.raw);
    const video = selectBestVideo(rawFormats);
    const audio = selectBestAudio(rawFormats);

    if (!video || !audio) {
      return res.status(404).json({
        error: 'no separate video/audio stream found',
      });
    }

    const response = buildDashResponse(
      id,
      extractTitle(info.raw),
      buildProvider(info.provider, info.instance),
      rawFormats
    );

    if (!response) {
      return res.status(404).json({
        error: 'no separate video/audio stream found',
      });
    }

    setCache(id, response);
    return res.json(response);
  } catch (err) {
    console.error('Unexpected error in stream handler:', err);
    return res.status(500).json({
      error: err?.message || 'internal error',
    });
  }
}

app.get('/api/stream/:video_id', verifyWorkerAuth, handleStreamRequest);

// =====================================================
// Start
// =====================================================
app.listen(port, () => {
  console.log(`Server running on ${port}`);
});

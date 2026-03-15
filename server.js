import express from 'express';
import { Innertube } from 'youtubei.js';
import crypto from 'crypto';

// ======= Configuration =======
const { WORKER_SECRET, PORT } = process.env;
if (!WORKER_SECRET) {
  console.error('WORKER_SECRET is required');
  process.exit(1);
}

const app = express();
const port = Number(PORT || 3000);
const ALLOWED_WINDOW = 300; // seconds
const INSTANCE_BAN_MS = 5 * 60 * 1000; // ms
const REQUEST_TIMEOUT_MS = 6_000; // per invidious request
const CACHE_TTL_MS = 30_000; // simple in-memory cache TTL for streaming info
const YT_ID_REGEX = /^[a-zA-Z0-9_-]{11}$/;

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

// ======= Utilities =======
// Minimal logger wrapper so we can add the error_id
const log = (...args) => console.log(...args);
const errlog = (...args) => console.error(...args);

const cache = new Map(); // key -> { ts, value }
const getCached = (k) => {
  const v = cache.get(k);
  if (!v) return null;
  if (Date.now() - v.ts > CACHE_TTL_MS) { cache.delete(k); return null; }
  return v.value;
};
const setCached = (k, value) => cache.set(k, { ts: Date.now(), value });

let ytClient = null;
const getYtClient = async () => {
  if (!ytClient) ytClient = await Innertube.create({ client_type: 'ANDROID', generate_session_locally: true });
  return ytClient;
};

const badInstances = new Map(); // instance -> timestamp
const markBad = (instance) => badInstances.set(instance, Date.now());

const rotateInstances = (list) => {
  const now = Date.now();
  const healthy = list.filter(i => {
    const t = badInstances.get(i);
    if (!t) return true;
    if (now - t > INSTANCE_BAN_MS) { badInstances.delete(i); return true; }
    return false;
  });
  return healthy.length ? healthy : list.slice();
};

const safeHexEqual = (a, b) => {
  try {
    const A = Buffer.from(String(a ?? ''), 'hex');
    const B = Buffer.from(String(b ?? ''), 'hex');
    if (A.length !== B.length) return false;
    return crypto.timingSafeEqual(A, B);
  } catch (e) {
    return false;
  }
};

const parseUrl = (format) => {
  if (!format) return null;
  if (typeof format === 'string') return format;
  if (format.url) return format.url;
  const cipher = format.signatureCipher || format.signature_cipher || format.cipher || format.s;
  if (!cipher) return null;
  try {
    const raw = typeof cipher === 'string' ? cipher : JSON.stringify(cipher);
    const params = new URLSearchParams(raw);
    return params.get('url') || null;
  } catch (e) {
    return null;
  }
};

const normalizeFormats = (sd = {}) => [
  ...(sd.formats || []),
  ...(sd.adaptiveFormats || []),
  ...(sd.adaptive_formats || []),
].map((f) => ({ ...f, mime: (f.mimeType || f.mime_type || f.type || '').toLowerCase() }));

const selectBestVideo = (formats) => formats
  .filter(f => f.mime && f.mime.includes('video'))
  .sort((a, b) => (b.height || 0) - (a.height || 0) || (b.bitrate || 0) - (a.bitrate || 0))[0] || null;

const selectBestAudio = (formats) => formats
  .filter(f => f.mime && f.mime.includes('audio'))
  .sort((a, b) => (b.bitrate || 0) - (a.bitrate || 0))[0] || null;

const selectBestProgressive = (formats) => formats
  .filter((f) => f.mime && f.mime.includes('video') && /mp4a|aac|opus/.test(f.mime))
  .sort((a, b) => (b.height || 0) - (a.height || 0))[0] || null;

// Abortable fetch with timeout
const fetchWithTimeout = async (url, opts = {}, timeoutMs = REQUEST_TIMEOUT_MS) => {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...opts, signal: controller.signal });
    clearTimeout(id);
    return res;
  } catch (err) {
    clearTimeout(id);
    throw err;
  }
};

// Run all instances in parallel and resolve with first successful parsed result.
const fastestFetch = async (instances, buildUrl, parser) => {
  const insts = rotateInstances(instances);
  if (!insts.length) throw new Error('no instances');

  const tasks = insts.map(async (base) => {
    const url = buildUrl(base);
    try {
      const res = await fetchWithTimeout(url);
      if (!res.ok) { markBad(base); throw new Error(`bad status ${res.status}`); }
      const data = await res.json();
      const parsed = parser(data);
      if (!parsed) { markBad(base); throw new Error('parse failed'); }
      return { instance: base, data: parsed };
    } catch (err) {
      log('instance failed', base, err?.message || err);
      markBad(base);
      throw err;
    }
  });

  return Promise.any(tasks);
};

// ======= Providers =======
const fetchFromInvidious = async (id) => {
  const cacheKey = `invidious:${id}`;
  const cached = getCached(cacheKey);
  if (cached) return cached;

  const result = await fastestFetch(INVIDIOUS_INSTANCES, (base) => `${base.replace(/\/+$/,'')}/api/v1/videos/${id}`, (data) => {
    const formats = [];
    if (Array.isArray(data.formatStreams)) data.formatStreams.forEach((f) => formats.push({ ...f, mimeType: f.type || f.mimeType }));
    if (Array.isArray(data.adaptiveFormats)) data.adaptiveFormats.forEach((f) => formats.push({ ...f, mimeType: f.type || f.mimeType }));
    if (Array.isArray(data.streamingData?.formats)) data.streamingData.formats.forEach((f) => formats.push(f));
    const sd = { formats };
    const is_live = Boolean(data.liveNow || data.isLive || data.is_live || data.live || data.streamingData?.isLive);
    return { streaming_data: sd, is_live, raw: data };
  });

  const out = { provider: 'invidious', instance: result.instance, streaming_data: result.data.streaming_data, is_live: result.data.is_live, raw: result.data.raw };
  setCached(cacheKey, out);
  return out;
};

const fetchFromInnertube = async (id) => {
  const cacheKey = `innertube:${id}`;
  const cached = getCached(cacheKey);
  if (cached) return cached;

  const client = await getYtClient();
  const info = await client.getInfo(id);
  if (!info?.streaming_data && !info?.player_response) throw new Error('No streaming data');
  const sd = info.streaming_data || info.player_response?.streamingData || {};
  const is_live = Boolean(
    info?.video_details?.isLive || info?.basic_info?.is_live || info?.microformat?.isLive ||
    info?.player_response?.playabilityStatus?.liveStreamability || info?.player_response?.videoDetails?.isLive ||
    info?.playability_status?.status === 'LIVE'
  );

  const out = { provider: 'innertube', streaming_data: sd, is_live, raw: info };
  setCached(cacheKey, out);
  return out;
};

const fetchStreamingInfo = async (id) => {
  try { return await fetchFromInvidious(id); } catch (e) { log('invidious failed, falling back to innertube', e?.message || e); }
  return fetchFromInnertube(id);
};

// ======= Structured Error responses =======
// sendError centralizes the shape and logging of errors.
const sendError = (req, res, {
  status = 500,
  code = 'server_error',
  message = null,
  details = null,
  hint = null,
  provider = null,
  instance = null,
  retry_after = null,
} = {}) => {
  const error_id = crypto.randomUUID();
  const payload = {
    error: {
      id: error_id,
      code,
      message,
      details: details || undefined,
      hint: hint || undefined,
      timestamp: new Date().toISOString(),
      retry_after: retry_after ?? undefined,
    },
    request: {
      method: req.method,
      path: req.originalUrl,
    },
    info: {
      provider: provider || undefined,
      instance: instance || undefined,
    },
  };

  // centralized logging with the id so logs and responses can be cross-referenced
  errlog(`[error:${error_id}]`, code, message, details ? `details=${JSON.stringify(details)}` : '');
  res.status(status).json(payload);
};

// ======= Auth middleware =======
const verifyWorkerAuth = (req, res, next) => {
  const ts = req.header('x-proxy-timestamp');
  const sig = req.header('x-proxy-signature');
  if (!ts || !sig) return sendError(req, res, { status: 401, code: 'auth_missing_headers', message: 'Required auth headers missing', hint: 'Provide x-proxy-timestamp and x-proxy-signature headers' });

  const now = Math.floor(Date.now() / 1000);
  const t = Number(ts);
  if (!Number.isFinite(t)) return sendError(req, res, { status: 400, code: 'auth_invalid_timestamp', message: 'Timestamp header is not a valid number', details: { received: ts } });
  if (Math.abs(now - t) > ALLOWED_WINDOW) return sendError(req, res, { status: 401, code: 'auth_timestamp_expired', message: 'Timestamp outside allowed window', details: { allowed_window_seconds: ALLOWED_WINDOW, server_time: now, received: t } });

  const payload = `${ts}:${req.originalUrl}`;
  const expected = crypto.createHmac('sha256', WORKER_SECRET).update(payload).digest('hex');
  if (!safeHexEqual(expected, sig)) return sendError(req, res, { status: 401, code: 'auth_invalid_signature', message: 'Signature mismatch', hint: 'Ensure HMAC-SHA256(WORKER_SECRET, `${ts}:${path}`) is used', details: { payload } });
  next();
};

// ======= Helpers =======
function isValidVideoId(id) { return typeof id === 'string' && YT_ID_REGEX.test(id); }
const makeResponse = ({ type, title = null, video_url = null, audio_url = null, provider = null, url = null }) => ({ type, title, video_url, audio_url, info: { provider, url } });

// ======= Route =======
app.get('/api/stream', verifyWorkerAuth, async (req, res) => {
  try {
    const id = String(req.query.id || '');
    if (!id) return sendError(req, res, { status: 400, code: 'missing_id', message: 'Query parameter `id` is required' });
    if (!isValidVideoId(id)) return sendError(req, res, { status: 400, code: 'invalid_id', message: 'Video id is invalid', details: { id } });

    const info = await fetchStreamingInfo(id);
    const raw = info.raw || {};
    const title = raw.title || raw.videoDetails?.title || raw.video_details?.title || raw.player_response?.videoDetails?.title || raw.player_response?.video_details?.title || raw.microformat?.title?.simpleText || raw.basic_info?.title || null;

    if (info.is_live) return sendError(req, res, { status: 403, code: 'live_not_supported', message: 'Live streams are not supported', provider: info.provider || null, instance: info.instance || null, details: { title } });

    const sd = info.streaming_data || {};
    const hlsCandidates = [sd.hlsManifestUrl, sd.hls_manifest_url, sd.hlsUrl, sd.hls, sd.streamingData?.hlsManifestUrl, sd.streamingData?.hls_manifest_url].filter(Boolean);
    if (hlsCandidates.length) return sendError(req, res, { status: 403, code: 'hls_not_supported', message: 'HLS streams are not supported', provider: info.provider || null, instance: info.instance || null, details: { candidates: hlsCandidates.slice(0,3) } });

    const dashCandidates = [sd.dashManifestUrl, sd.dash_manifest_url, sd.streamingData?.dashManifestUrl, sd.streamingData?.dash_manifest_url].filter(Boolean);
    if (dashCandidates.length) return sendError(req, res, { status: 403, code: 'dash_not_supported', message: 'DASH streams are not supported', provider: info.provider || null, instance: info.instance || null, details: { candidates: dashCandidates.slice(0,3) } });

    const formats = normalizeFormats(sd);
    if (!formats.length) return sendError(req, res, { status: 404, code: 'no_formats', message: 'No playable formats found', provider: info.provider || null, instance: info.instance || null });

    const containsHlsFormat = formats.some((f) => { const url = parseUrl(f) || ''; return (f.mime && f.mime.includes('mpegurl')) || url.includes('.m3u8') || /application\/vnd\.apple\.mpegurl/.test(f.mime || ''); });
    if (containsHlsFormat) return sendError(req, res, { status: 403, code: 'hls_not_supported', message: 'HLS formats detected and not supported', provider: info.provider || null, instance: info.instance || null });

    const video = selectBestVideo(formats);
    const audio = selectBestAudio(formats);
    if (video && audio) {
      return res.json(makeResponse({ type: 'dash', title, video_url: parseUrl(video), audio_url: parseUrl(audio), provider: info.provider || null, url: info.instance || null }));
    }

    const progressive = selectBestProgressive(formats);
    if (progressive) {
      return res.json(makeResponse({ type: 'progressive', title: parseUrl(progressive), url: info.instance || null }));
    }

    return sendError(req, res, { status: 404, code: 'no_compatible_format', message: 'No compatible progressive/DASH formats available', provider: info.provider || null, instance: info.instance || null });
  } catch (e) {
    // Unexpected errors go through centralized error response
    return sendError(req, res, { status: 500, code: 'unexpected_error', message: 'Unexpected server error', details: e?.message });
  }
});

// Global error-handling middleware (last resort)
app.use((err, req, res, next) => {
  if (res.headersSent) return next(err);
  sendError(req, res, { status: 500, code: 'unhandled_exception', message: 'Unhandled exception', details: err?.message });
});

// ======= Startup =======
const server = app.listen(port, () => console.log(`Server listening on ${port}`));

// graceful shutdown
process.on('SIGINT', () => server.close(() => process.exit(0)));
process.on('SIGTERM', () => server.close(() => process.exit(0)));
  if (!v) return null;
  if (Date.now() - v.ts > CACHE_TTL_MS) { cache.delete(k); return null; }
  return v.value;
};
const setCached = (k, value) => cache.set(k, { ts: Date.now(), value });

let ytClient = null;
const getYtClient = async () => {
  if (!ytClient) ytClient = await Innertube.create({ client_type: 'ANDROID', generate_session_locally: true });
  return ytClient;
};

const badInstances = new Map(); // instance -> timestamp
const markBad = (instance) => badInstances.set(instance, Date.now());

const rotateInstances = (list) => {
  const now = Date.now();
  // prefer healthy instances; if none, return original list
  const healthy = list.filter(i => {
    const t = badInstances.get(i);
    if (!t) return true;
    if (now - t > INSTANCE_BAN_MS) { badInstances.delete(i); return true; }
    return false;
  });
  return healthy.length ? healthy : list.slice();
};

const safeHexEqual = (a, b) => {
  try {
    const A = Buffer.from(String(a ?? ''), 'hex');
    const B = Buffer.from(String(b ?? ''), 'hex');
    if (A.length !== B.length) return false;
    return crypto.timingSafeEqual(A, B);
  } catch (e) {
    return false;
  }
};

const parseUrl = (format) => {
  if (!format) return null;
  if (typeof format === 'string') return format;
  if (format.url) return format.url;
  const cipher = format.signatureCipher || format.signature_cipher || format.cipher || format.s;
  if (!cipher) return null;
  try {
    // some invidious formats embed the whole querystring
    const raw = typeof cipher === 'string' ? cipher : JSON.stringify(cipher);
    const params = new URLSearchParams(raw);
    return params.get('url') || null;
  } catch (e) {
    return null;
  }
};

const normalizeFormats = (sd = {}) => [
  ...(sd.formats || []),
  ...(sd.adaptiveFormats || []),
  ...(sd.adaptive_formats || []),
].map((f) => ({ ...f, mime: (f.mimeType || f.mime_type || f.type || '').toLowerCase() }));

const selectBestVideo = (formats) => formats
  .filter(f => f.mime && f.mime.includes('video'))
  .sort((a, b) => (b.height || 0) - (a.height || 0) || (b.bitrate || 0) - (a.bitrate || 0))[0] || null;

const selectBestAudio = (formats) => formats
  .filter(f => f.mime && f.mime.includes('audio'))
  .sort((a, b) => (b.bitrate || 0) - (a.bitrate || 0))[0] || null;

const selectBestProgressive = (formats) => formats
  .filter((f) => f.mime && f.mime.includes('video') && /mp4a|aac|opus/.test(f.mime))
  .sort((a, b) => (b.height || 0) - (a.height || 0))[0] || null;

// Abortable fetch with timeout
const fetchWithTimeout = async (url, opts = {}, timeoutMs = REQUEST_TIMEOUT_MS) => {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...opts, signal: controller.signal });
    clearTimeout(id);
    return res;
  } catch (err) {
    clearTimeout(id);
    throw err;
  }
};

// Run all instances in parallel and resolve with first successful parsed result.
const fastestFetch = async (instances, buildUrl, parser) => {
  const insts = rotateInstances(instances);
  if (!insts.length) throw new Error('no instances');

  const tasks = insts.map(async (base) => {
    const url = buildUrl(base);
    try {
      const res = await fetchWithTimeout(url);
      if (!res.ok) { markBad(base); throw new Error(`bad status ${res.status}`); }
      const data = await res.json();
      const parsed = parser(data);
      if (!parsed) { markBad(base); throw new Error('parse failed'); }
      return { instance: base, data: parsed };
    } catch (err) {
      log('instance failed', base, err?.message || err);
      markBad(base);
      throw err;
    }
  });

  // Promise.any will throw AggregateError if all fail
  return Promise.any(tasks);
};

// ======= Providers =======
const fetchFromInvidious = async (id) => {
  const cacheKey = `invidious:${id}`;
  const cached = getCached(cacheKey);
  if (cached) return cached;

  const result = await fastestFetch(INVIDIOUS_INSTANCES, (base) => `${base.replace(/\/+$/,'')}/api/v1/videos/${id}`, (data) => {
    const formats = [];
    if (Array.isArray(data.formatStreams)) data.formatStreams.forEach((f) => formats.push({ ...f, mimeType: f.type || f.mimeType }));
    if (Array.isArray(data.adaptiveFormats)) data.adaptiveFormats.forEach((f) => formats.push({ ...f, mimeType: f.type || f.mimeType }));
    if (Array.isArray(data.streamingData?.formats)) data.streamingData.formats.forEach((f) => formats.push(f));
    const sd = { formats };
    const is_live = Boolean(data.liveNow || data.isLive || data.is_live || data.live || data.streamingData?.isLive);
    return { streaming_data: sd, is_live, raw: data };
  });

  const out = { provider: 'invidious', instance: result.instance, streaming_data: result.data.streaming_data, is_live: result.data.is_live, raw: result.data.raw };
  setCached(cacheKey, out);
  return out;
};

const fetchFromInnertube = async (id) => {
  const cacheKey = `innertube:${id}`;
  const cached = getCached(cacheKey);
  if (cached) return cached;

  const client = await getYtClient();
  const info = await client.getInfo(id);
  if (!info?.streaming_data && !info?.player_response) throw new Error('No streaming data');
  const sd = info.streaming_data || info.player_response?.streamingData || {};
  const is_live = Boolean(
    info?.video_details?.isLive || info?.basic_info?.is_live || info?.microformat?.isLive ||
    info?.player_response?.playabilityStatus?.liveStreamability || info?.player_response?.videoDetails?.isLive ||
    info?.playability_status?.status === 'LIVE'
  );

  const out = { provider: 'innertube', streaming_data: sd, is_live, raw: info };
  setCached(cacheKey, out);
  return out;
};

const fetchStreamingInfo = async (id) => {
  // try invidious first for speed and to reduce Innertube calls
  try { return await fetchFromInvidious(id); } catch (e) { log('invidious failed, falling back to innertube', e?.message || e); }
  return fetchFromInnertube(id);
};

// ======= Auth middleware =======
const makeError = (res, status = 401) => res.status(status).json({ type: 'error', title: null, video_url: null, audio_url: null, info: { provider: null, url: null } });

const verifyWorkerAuth = (req, res, next) => {
  const ts = req.header('x-proxy-timestamp');
  const sig = req.header('x-proxy-signature');
  if (!ts || !sig) return makeError(res, 401);
  const now = Math.floor(Date.now() / 1000);
  const t = Number(ts);
  if (!Number.isFinite(t) || Math.abs(now - t) > ALLOWED_WINDOW) return makeError(res, 401);
  const payload = `${ts}:${req.originalUrl}`;
  const expected = crypto.createHmac('sha256', WORKER_SECRET).update(payload).digest('hex');
  if (!safeHexEqual(expected, sig)) return makeError(res, 401);
  next();
};

// ======= Helpers =======
function isValidVideoId(id) { return typeof id === 'string' && YT_ID_REGEX.test(id); }
const makeResponse = ({ type, title = null, video_url = null, audio_url = null, provider = null, url = null }) => ({ type, title, video_url, audio_url, info: { provider, url } });

// ======= Route =======
app.get('/api/stream', verifyWorkerAuth, async (req, res) => {
  try {
    const id = String(req.query.id || '');
    if (!id) return res.status(400).json(makeResponse({ type: 'error' }));
    if (!isValidVideoId(id)) return res.status(400).json(makeResponse({ type: 'error' }));

    const info = await fetchStreamingInfo(id);
    const raw = info.raw || {};
    const title = raw.title || raw.videoDetails?.title || raw.video_details?.title || raw.player_response?.videoDetails?.title || raw.player_response?.video_details?.title || raw.microformat?.title?.simpleText || raw.basic_info?.title || null;

    if (info.is_live) return res.status(403).json(makeResponse({ type: 'error', title, provider: info.provider || null, url: info.instance || null }));

    const sd = info.streaming_data || {};
    const hlsCandidates = [sd.hlsManifestUrl, sd.hls_manifest_url, sd.hlsUrl, sd.hls, sd.streamingData?.hlsManifestUrl, sd.streamingData?.hls_manifest_url].filter(Boolean);
    if (hlsCandidates.length) return res.status(403).json(makeResponse({ type: 'error', title, provider: info.provider || null, url: info.instance || null }));

    const dashCandidates = [sd.dashManifestUrl, sd.dash_manifest_url, sd.streamingData?.dashManifestUrl, sd.streamingData?.dash_manifest_url].filter(Boolean);
    if (dashCandidates.length) return res.status(403).json(makeResponse({ type: 'error', title, provider: info.provider || null, url: info.instance || null }));

    const formats = normalizeFormats(sd);
    if (!formats.length) return res.status(404).json(makeResponse({ type: 'error', title, provider: info.provider || null, url: info.instance || null }));

    const containsHlsFormat = formats.some((f) => { const url = parseUrl(f) || ''; return (f.mime && f.mime.includes('mpegurl')) || url.includes('.m3u8') || /application\/vnd\.apple\.mpegurl/.test(f.mime || ''); });
    if (containsHlsFormat) return res.status(403).json(makeResponse({ type: 'error', title, provider: info.provider || null, url: info.instance || null }));

    const video = selectBestVideo(formats);
    const audio = selectBestAudio(formats);
    if (video && audio) {
      return res.json(makeResponse({ type: 'dash', title, video_url: parseUrl(video), audio_url: parseUrl(audio), provider: info.provider || null, url: info.instance || null }));
    }

    const progressive = selectBestProgressive(formats);
    if (progressive) {
      return res.json(makeResponse({ type: 'progressive', title: parseUrl(progressive), url: info.instance || null }));
    }

    return res.status(404).json(makeResponse({ type: 'error', title, provider: info.provider || null, url: info.instance || null }));
  } catch (e) {
    log('unexpected error', e?.message || e);
    return res.status(500).json(makeResponse({ type: 'error', title: null, provider: null, url: null }));
  }
});

// ======= Startup =======
const server = app.listen(port, () => console.log(`Server listening on ${port}`));

// graceful shutdown
process.on('SIGINT', () => server.close(() => process.exit(0)));
process.on('SIGTERM', () => server.close(() => process.exit(0)));

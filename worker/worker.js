export default {
  async fetch(request, env) {
    const required = ["WORKER_SECRET", "SERVER_URL"];

    for (const key of required) {
      if (!env[key]) {
        return new Response(`${key} required`, { status: 500 });
      }
    }
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Headers": "*",
          "Access-Control-Allow-Methods": "GET, OPTIONS"
        }
      });
    }
    const url = new URL(request.url);
    const target = new URL(env.SERVER_URL);
    target.pathname = "/api/stream";
    target.search = url.search;
    const ts = Math.floor(Date.now() / 1000);
    async function hmacHex(secret, msg) {
      const enc = new TextEncoder();
      const keyData = enc.encode(secret);
      const imported = await crypto.subtle.importKey("raw", keyData, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
      const sig = await crypto.subtle.sign("HMAC", imported, enc.encode(msg));
      const b = new Uint8Array(sig);
      let hex = "";
      for (let i = 0; i < b.length; i++) {
        const h = b[i].toString(16);
        hex += (h.length === 1 ? "0" : "") + h;
      }
      return hex;
    }
    const payload = `${ts}:${url.pathname}${url.search}`;
    const signature = await hmacHex(env.WORKER_SECRET, payload);
    const outHeaders = new Headers(request.headers);
    outHeaders.set("x-proxy-timestamp", String(ts));
    outHeaders.set("x-proxy-signature", signature);
    const nodeRes = await fetch(target.toString(), {
      method: "GET",
      headers: outHeaders
    });
    const headers = new Headers(nodeRes.headers);
    headers.set("Access-Control-Allow-Origin", "*");
    headers.set("Access-Control-Allow-Headers", "*");
    return new Response(nodeRes.body, {
      status: nodeRes.status,
      headers
    });
  }
};
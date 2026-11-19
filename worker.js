import { connect } from "cloudflare:sockets";
import { DurableObject } from "cloudflare:workers";

const CORS = {
"Access-Control-Allow-Origin": "*",
"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
"Access-Control-Allow-Headers": "*",
};

function json(data, status = 200) {
return new Response(JSON.stringify(data), {
status,
headers: { "Content-Type": "application/json", ...CORS },
});
}

export class ChunkRelaySession extends DurableObject {
constructor(ctx, env) {
super(ctx, env);
this.socket = null;
this.writer = null;
this.reader = null;
this.buffer = [];
this.closed = false;
}

async ensureSocket(host, port) {
if (this.socket) return;
this.socket = connect({ hostname: host, port: Number(port) || 443 });
this.writer = this.socket.writable.getWriter();
this.reader = this.socket.readable.getReader();
this.readLoop();
}

async readLoop() {
try {
while (!this.closed) {
const { value, done } = await this.reader.read();
if (done) break;
if (value?.length) this.buffer.push(value);
}
} catch {
} finally {
this.closed = true;
}
}

async fetch(request) {
const url = new URL(request.url);
const action = url.pathname.split("/").pop();

if (action === "open") {
let host = url.searchParams.get("host") || url.searchParams.get("dst") || request.headers.get("X-Relay-Target");
let port = url.searchParams.get("port") || "443";
if (request.method === "POST") {
try {
const body = await request.clone().json();
host = body.host || body.dst || body.target || host;
port = body.port || port;
} catch {}
}
if (host && host.includes(":")) [host, port] = host.split(":");
if (!host) return json({ error: "Missing target host" }, 400);
try {
await this.ensureSocket(host, port);
return json({ ok: true, status: "open" }, 200);
} catch (e) {
this.closed = true;
return json({ error: String(e) }, 502);
}
}

if (action === "push" || request.method === "POST") {
const data = await request.arrayBuffer();
if (!this.socket || this.closed) return json({ error: "Not open" }, 410);
try {
if (data.byteLength > 0) await this.writer.write(new Uint8Array(data));
return new Response(null, { status: 204, headers: CORS });
} catch (e) {
this.closed = true;
return json({ error: String(e) }, 502);
}
}

if (action === "poll" || (request.method === "GET" && action !== "close")) {
const chunks = this.buffer.splice(0, this.buffer.length);
if (chunks.length === 0) {
return new Response(null, { status: this.closed ? 410 : 204, headers: CORS });
}
const total = chunks.reduce((s, c) => s + c.length, 0);
const out = new Uint8Array(total);
let off = 0;
for (const c of chunks) { out.set(c, off); off += c.length; }
return new Response(out, {
status: 200,
headers: { "Content-Type": "application/octet-stream", ...CORS },
});
}

if (action === "close") {
this.closed = true;
try { await this.writer?.close(); } catch {}
try { await this.socket?.close(); } catch {}
return new Response(null, { status: 204, headers: CORS });
}

return json({ error: "Unknown action" }, 404);
}
}

export default {
async fetch(request, env) {
if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });
const url = new URL(request.url);

if (url.pathname === "/" || url.pathname === "/health") {
return json({
status: "ok",
protocol: "chunk-relay-mtproto",
hasBinding: Boolean(env.CHUNK_RELAY),
});
}

let sid = url.searchParams.get("session") || url.searchParams.get("sid") || url.searchParams.get("id") || request.headers.get("X-Relay-Session");
if (!sid && request.method === "POST" && url.pathname.endsWith("/open")) {
try {
const b = await request.clone().json();
sid = b.session || b.sid || b.id;
} catch {}
}

if (!sid) sid = crypto.randomUUID();
if (!env.CHUNK_RELAY) return json({ error: "CHUNK_RELAY binding missing" }, 500);

const stub = env.CHUNK_RELAY.get(env.CHUNK_RELAY.idFromName(sid));
const res = await stub.fetch(request);
if (url.pathname.endsWith("/open") && res.ok) {
const data = await res.json();
return json({ ...data, session: sid, sid, id: sid }, 200);
}
return res;
},
};

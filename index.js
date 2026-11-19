import { connect } from "cloudflare:sockets";
import { DurableObject } from "cloudflare:workers";

const CORS_HEADERS = {
"Access-Control-Allow-Origin": "*",
"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
"Access-Control-Allow-Headers": "Content-Type, X-Relay-Target, X-Relay-Session, X-Relay-Seq",
};

function jsonResponse(data, status = 200) {
return new Response(JSON.stringify(data), {
status,
headers: { "Content-Type": "application/json", ...CORS_HEADERS },
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
this.lastActivity = Date.now();
}

async ensureSocket(target) {
if (this.socket) return;
const [hostname, portStr] = target.split(":");
const port = parseInt(portStr || "443", 10);
this.socket = connect({ hostname, port });
this.writer = this.socket.writable.getWriter();
this.reader = this.socket.readable.getReader();
this.readLoop();
}

async readLoop() {
try {
while (!this.closed) {
const { value, done } = await this.reader.read();
if (done) break;
if (value && value.length > 0) {
this.buffer.push(value);
}
}
} catch {
} finally {
this.closed = true;
}
}

async fetch(request) {
this.lastActivity = Date.now();
const url = new URL(request.url);
const target = request.headers.get("X-Relay-Target");

if (request.method === "POST") {
if (!target) return jsonResponse({ error: "Missing target" }, 400);
try {
await this.ensureSocket(target);
const body = await request.arrayBuffer();
if (body.byteLength > 0) {
await this.writer.write(new Uint8Array(body));
}
return new Response(null, { status: 204, headers: CORS_HEADERS });
} catch (e) {
this.closed = true;
return jsonResponse({ error: String(e) }, 502);
}
}

if (request.method === "GET") {
if (url.pathname.endsWith("/close")) {
this.closed = true;
try { await this.writer?.close(); } catch {}
try { await this.socket?.close(); } catch {}
return new Response(null, { status: 204, headers: CORS_HEADERS });
}

const chunks = this.buffer.splice(0, this.buffer.length);
if (chunks.length === 0) {
return new Response(null, {
status: this.closed ? 410 : 204,
headers: CORS_HEADERS,
});
}

const totalLen = chunks.reduce((acc, c) => acc + c.length, 0);
const merged = new Uint8Array(totalLen);
let offset = 0;
for (const c of chunks) {
merged.set(c, offset);
offset += c.length;
}

return new Response(merged, {
status: 200,
headers: { "Content-Type": "application/octet-stream", ...CORS_HEADERS },
});
}

return new Response("Method not allowed", { status: 405, headers: CORS_HEADERS });
}
}

export default {
async fetch(request, env) {
if (request.method === "OPTIONS") {
return new Response(null, { status: 204, headers: CORS_HEADERS });
}

const url = new URL(request.url);
if (url.pathname === "/health" || url.pathname === "/") {
return jsonResponse({
status: "ok",
protocol: "chunk-relay-mtproto",
hasBinding: Boolean(env.CHUNK_RELAY),
});
}

const sessionId = request.headers.get("X-Relay-Session");
if (!sessionId) {
return jsonResponse({ error: "Missing X-Relay-Session" }, 400);
}

if (!env.CHUNK_RELAY) {
return jsonResponse({ error: "CHUNK_RELAY binding missing" }, 500);
}

const id = env.CHUNK_RELAY.idFromName(sessionId);
const stub = env.CHUNK_RELAY.get(id);
return stub.fetch(request);
},
};

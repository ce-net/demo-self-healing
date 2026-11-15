// Headless Chromium over CDP: the page before, during (amber) and after a kill. node tests/shot.mjs <url> <outdir> [w h]
import { spawn } from "node:child_process"; import { writeFileSync, mkdtempSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os"; import { join } from "node:path"; import { createConnection } from "node:net";
import { randomBytes } from "node:crypto";
// Node 18 has no WebSocket: a minimal client (text frames, masked, no fragmentation) is enough for CDP
class MiniWS {
  constructor(url) {
    const u = new URL(url); this.buf = Buffer.alloc(0); this.up = false;
    this.sock = createConnection(+u.port, u.hostname, () => this.sock.write(`GET ${u.pathname} HTTP/1.1\r\nHost: ${u.host}\r\n` +
      `Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: ${randomBytes(16).toString("base64")}\r\nSec-WebSocket-Version: 13\r\n\r\n`));
    this.sock.on("data", (d) => { this.buf = Buffer.concat([this.buf, d]); this.pump(); });
  }
  pump() {
    if (!this.up) { const i = this.buf.indexOf("\r\n\r\n"); if (i < 0) return; this.buf = this.buf.subarray(i + 4); this.up = true; this.onopen && this.onopen(); }
    for (;;) {
      if (this.buf.length < 2) return; let len = this.buf[1] & 127, off = 2;
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (this.buf.length < 10) return; len = Number(this.buf.readBigUInt64BE(2)); off = 10; }
      if (this.buf.length < off + len) return; const op = this.buf[0] & 15, data = this.buf.subarray(off, off + len);
      this.buf = this.buf.subarray(off + len); if (op === 1 && this.onmessage) this.onmessage({ data: data.toString() });
    }
  }
  send(text) {
    const p = Buffer.from(text), m = randomBytes(4); let h;
    if (p.length < 126) h = Buffer.from([0x81, 0x80 | p.length]);
    else if (p.length < 65536) { h = Buffer.alloc(4); h[0] = 0x81; h[1] = 0xfe; h.writeUInt16BE(p.length, 2); }
    else { h = Buffer.alloc(10); h[0] = 0x81; h[1] = 0xff; h.writeBigUInt64BE(BigInt(p.length), 2); }
    for (let i = 0; i < p.length; i++) p[i] ^= m[i & 3]; this.sock.write(Buffer.concat([h, m, p]));
  }
  close() { this.sock.destroy(); }
}
const WS = globalThis.WebSocket || MiniWS;
const [url, out, W = "1440", H = "900"] = process.argv.slice(2); mkdirSync(out, { recursive: true });
const port = 9400 + Math.floor(Math.random() * 400);
const ch = spawn(process.env.CHROME || "chromium", ["--headless=new", "--no-sandbox", "--password-store=basic",
  `--remote-debugging-port=${port}`, `--user-data-dir=${mkdtempSync(join(tmpdir(), "shot-"))}`, `--window-size=${W},${H}`, "about:blank"],
  { stdio: "ignore" });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let ver; for (let i = 0; i < 50 && !ver; i++) { try { ver = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json(); } catch { await sleep(200); } }
const ws = new WS(ver.find((t) => t.type === "page").webSocketDebuggerUrl); await new Promise((r) => ws.onopen = r);
let id = 0; const wait = new Map(); ws.onmessage = (m) => { const d = JSON.parse(m.data); if (wait.has(d.id)) { wait.get(d.id)(d.result); wait.delete(d.id); } };
const cdp = (method, params = {}) => new Promise((r) => { const i = ++id; wait.set(i, r); ws.send(JSON.stringify({ id: i, method, params })); });
const ev = async (expr) => (await cdp("Runtime.evaluate", { expression: expr, returnByValue: true, awaitPromise: true })).result.value;
const shot = async (name) => { const r = await cdp("Page.captureScreenshot", { format: "png", captureBeyondViewport: true,
  clip: { x: 0, y: 0, width: +W, height: await ev("document.documentElement.scrollHeight"), scale: 1 } });
  writeFileSync(join(out, name), Buffer.from(r.data, "base64")); console.log("wrote", join(out, name)); };
await cdp("Emulation.setDeviceMetricsOverride", { width: +W, height: +H, deviceScaleFactor: 1, mobile: +W < 600 });
await cdp("Page.navigate", { url }); await sleep(6000);
await ev(`(async()=>{document.getElementById("name").value="headless";document.getElementById("msg").value="signed before the kill";
  document.getElementById("sign").requestSubmit(); await new Promise(r=>setTimeout(r,1500)); return 1})()`);
await shot("1-before.png");
const killAt = Date.now(); await ev(`document.getElementById("kill").click()`);
for (let i = 0; i < 40; i++) { await sleep(50); if (await ev(`document.body.classList.contains("healing")`)) break; }
await shot("2-electing.png");
for (let i = 0; i < 60; i++) { await sleep(500); if (await ev(`fetch("api/state").then(r=>r.json()).then(s=>!!(s.proof&&s.proof.at>${killAt}&&s.proof.stale))`)) break; }
await sleep(800); await shot("3-after.png");
console.log(await ev(`document.getElementById("events").innerText.split("\\n").slice(0,24).join(" | ")`));
ws.close(); ch.kill();

"use strict";
// The page reads only the demo front's API. On ce-net.com it calls the front through the public gate; anywhere else
// (the front serving this page itself) the API is relative.
const API = location.hostname === "ce-net.com" || location.hostname === "www.ce-net.com"
  ? "https://apps.ce-net.com/self-healing/api/" : "api/";
const $ = (id) => document.getElementById(id);
let st = null, lastEv = 0, shown = {}, killedEpoch = 0, guests = [];

function el(tag, cls, text) { const e = document.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; }
const short = (h) => h ? h.slice(0, 10) : "-";
const fmt = (n) => n == null ? "-" : Number(n).toLocaleString("en-US");
const clock = (ms) => new Date(ms).toLocaleTimeString("en-GB", { hour12: false });

function tick(id, to) {
  const e = $(id); if (to == null) { e.textContent = "-"; return; }
  const from = shown[id] ?? to; shown[id] = to;
  if (from === to) { e.textContent = fmt(to); return; }
  const t0 = performance.now();
  const step = (t) => { const k = Math.min(1, (t - t0) / 700), v = Math.round(from + (to - from) * (1 - Math.pow(1 - k, 3)));
    e.textContent = fmt(v); if (k < 1) requestAnimationFrame(step); };
  requestAnimationFrame(step);
}

async function api(path, body) {
  const r = await fetch(API + path, body === undefined ? { cache: "no-store" } :
    { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  return r.json();
}

function render() {
  if (!st) return;
  document.body.classList.toggle("healing", !st.leader);
  tick("acked", st.acked); tick("epoch", st.epoch); tick("counter", st.counter);
  const lead = st.replicas.find((r) => r.id === st.leader);
  $("hl").textContent = st.leader ? `leader ${lead.place} · epoch ${st.epoch} · ${fmt(st.acked)} writes acknowledged`
    : "no leader · electing";
  $("lead-dot").className = st.leader ? "g" : "r";
  const kr = $("kill-read");
  if (!st.leader) kr.textContent = "No leader. The two survivors are voting; writes wait, none are lost.";
  else if (killedEpoch && st.epoch > killedEpoch) kr.textContent = `New leader: ${lead.place}, epoch ${killedEpoch} to ${st.epoch}. Every acknowledged write is still there.`;
  else if (!killedEpoch) kr.textContent = `Leader: ${lead.place}, epoch ${st.epoch}. Kill it.`;
  $("kill").disabled = !st.leader;
  const box = $("holders"); box.textContent = "";
  for (const r of st.replicas) {
    const cls = !r.up ? "down" : r.role === "leader" ? "leader" : r.behind ? "behind" : "";
    const c = el("div", "glass holder " + cls + (r.role === "leader" ? " ticks" : ""));
    c.append(el("div", "who", r.place));
    c.append(el("div", "role", !r.up ? "Dead" : r.role === "leader" ? "Leader" : r.behind ? "Catching up" : r.role === "candidate" ? "Candidate" : "Follower"));
    const dl = el("dl");
    const row = (k, v) => { dl.append(el("dt", null, k), el("dd", null, v)); };
    if (r.up) { row("epoch", r.epoch); row("seq", fmt(r.last_seq)); row("committed", fmt(r.commit)); row("head", short(r.commit_hash));
      row("refused", fmt(r.refused) + " stale msgs"); }
    else { row("state", "process gone"); row("restart", "in a few seconds"); }
    c.append(dl); box.append(c);
  }
  const g = $("guests"); g.textContent = "";
  if (st.guests && st.guests.length) guests = st.guests;
  for (const m of guests.slice().reverse()) {
    const li = el("li"); li.append(el("span", "seq", "#" + m.seq));
    const t = el("span"); t.append(el("b", null, m.name), document.createTextNode("  " + m.msg)); li.append(t); g.append(li);
  }
  if (!guests.length) g.append(el("li", "muted empty", "No messages yet. Be the first."));
  renderProof(st.proof);
}

function renderProof(p) {
  if (!p) return;
  const box = $("proof"); box.textContent = "";
  box.append(el("div", "verdict " + (p.pass ? "pass" : "fail"), p.pass ? "Nothing lost." : "Not proven yet."));
  const row = (k, v, ok) => { const d = el("div", "row"); d.append(el("span", null, k), el("span", ok == null ? "" : ok ? "pass" : "fail", v)); box.append(d); };
  row("checked", new Date(p.at).toLocaleTimeString("en-GB", { hour12: false }));
  for (const [id, h] of Object.entries(p.heads || {})) {
    const r = st.replicas.find((x) => x.id === id), v = p.replicas[id] || {};
    row(`${r ? r.place : id} head @ ${fmt(p.head_seq)}`, short(h) + (v.chain_ok ? "  chain ok" : "  chain BAD"), !!v.chain_ok);
  }
  row("chain heads equal", p.heads_equal ? "yes" : "no", p.heads_equal);
  row("acked writes looked up", `${fmt(p.acked)} on each replica`);
  row("acked writes missing", fmt(p.missing), p.missing === 0);
  if (p.acked_newer) row("newer than slowest replica", `${fmt(p.acked_newer)}, checked next run`);
  const s = p.stale;
  if (s && s.ok) {
    const refused = s.answers.filter((a) => a.refused), took = s.answers.filter((a) => a.ok);
    row(`stale leader write, epoch ${s.stale_epoch}`, `refused by ${refused.length} of ${s.answers.length} (they are at epoch ${s.current_epoch})`,
      took.length === 0 && refused.length > 0);
  }
}

function addEvent(e) {
  const o = $("events"), li = el("li", e.kind);
  const place = e.place || "";
  const T = {
    kill: () => `Crash op sent to ${place} (leader, epoch ${e.epoch})`,
    died: () => `${place} stopped answering (was ${e.was})`,
    back: () => `${place} answers again after ${(e.down_ms / 1000).toFixed(1)} s`,
    restarted: () => `${place} restarted`,
    elected: () => `${place} elected leader, epoch ${e.prev_epoch} to ${e.epoch}`,
    "caught-up": () => `${place} caught up to seq ${fmt(e.seq)}`,
    "stale-refused": () => `epoch ${e.stale_epoch} write from ${place} refused by ${e.refused_by.length} of 2 peers`,
    proof: () => e.passed ? `proof passed: ${fmt(e.acked)} acked writes found, heads equal at seq ${fmt(e.head_seq)}`
      : `proof not passed (missing ${e.missing})`,
  };
  let text = e.kind; try { if (T[e.kind]) text = T[e.kind](); } catch (x) { /* an older event shape */ }
  li.append(el("time", null, clock(e.at)), el("span", "k", e.kind.replace("-", " ")), el("span", "t", text));
  o.prepend(li);
  while (o.children.length > 60) o.lastChild.remove();
}

async function poll() {
  try { st = await api("state"); render(); } catch (e) { $("hl").textContent = "reconnecting"; }
  setTimeout(poll, 250);
}

async function watch() {
  for (;;) {
    try {
      const r = await api("events?after=" + lastEv);
      for (const e of r.events) { if (e.n > lastEv) { lastEv = e.n; addEvent(e); } }
    } catch (e) { await new Promise((ok) => setTimeout(ok, 1000)); }
  }
}

$("kill").onclick = async () => {
  const kr = $("kill-read");
  try {
    const r = await api("kill", {});
    if (r.ok) { killedEpoch = r.epoch; kr.textContent = `Killed ${r.place} (epoch ${r.epoch}). Watch the epoch go up.`;
      setTimeout(() => { killedEpoch = 0; }, 15000); }
    else kr.textContent = r.error;
  } catch (e) { kr.textContent = "Could not reach the demo."; }
};
$("plus").onclick = async () => { try { await api("count", {}); } catch (e) { /* the next poll shows the state */ } };
$("run-proof").onclick = async () => { try { renderProof(await api("proof", {})); } catch (e) { /* shown on next poll */ } };
$("sign").onsubmit = async (ev) => {
  ev.preventDefault();
  const sr = $("sign-read");
  try {
    const r = await api("sign", { name: $("name").value, msg: $("msg").value });
    sr.textContent = r.ok ? `Acknowledged as #${r.seq}, hash ${short(r.hash)}, epoch ${r.epoch}. Now kill the leader.` : r.error;
    if (r.ok) $("msg").value = "";
  } catch (e) { sr.textContent = "Could not reach the demo."; }
};
$("theme").onclick = () => {
  const light = document.documentElement.dataset.theme !== "light";
  if (light) document.documentElement.dataset.theme = "light"; else delete document.documentElement.dataset.theme;
  $("theme").textContent = light ? "Dark" : "Light";
  try { localStorage.setItem("ce-theme", light ? "light" : "dark"); } catch (e) { /* private mode */ }
};
if (document.documentElement.dataset.theme === "light") $("theme").textContent = "Dark";
poll(); watch();

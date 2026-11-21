"use strict";
// Executes the real preview controller with fictional responses and a virtual clock.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const source = fs.readFileSync("ui/live.js", "utf8");
let passed = 0;

function harness(options = {}) {
  let now = 0;
  let sequence = 0;
  const timers = new Map();
  const listeners = new Map();
  const calls = [];
  const transitions = [];
  const ids = new Map();
  for (const id of ["preview-video", "preview-connect", "preview-disconnect", "preview-state", "preview-feedback", "preview-placeholder"]) {
    const attributes = new Map();
    const events = new Map();
    ids.set(id, {
      hidden: false, disabled: false, readyState: options.noDecode ? 0 : 2,
      textContent: "", paused: true,
      addEventListener: (name, fn) => events.set(name, fn),
      dispatch: name => events.get(name)?.(),
      click() { if (!this.disabled) this.dispatch("click"); },
      hasAttribute: name => attributes.has(name),
      removeAttribute: name => attributes.delete(name),
      set src(value) { attributes.set("src", value); },
      get src() { return attributes.get("src"); },
      load() {}, pause() { this.paused = true; },
      play() { this.paused = false; return Promise.resolve(); },
    });
  }
  Object.defineProperty(ids.get("preview-state"), "textContent", {
    set(value) { transitions.push(value); }, get() { return transitions.at(-1); },
  });
  const remote = { state: "disconnected", reason: "manual", session: null, revision: 0, csrf: "c".repeat(43) };
  if (options.busy) { remote.state = "connected"; remote.session = "o".repeat(32); }
  const response = () => ({ ok: true, json: async () => ({ ...remote }) });
  let connectAt = 0;
  let releaseConnect;
  async function fetch(path, init) {
    calls.push({ path, init });
    if (options.unavailable) throw new Error("fictional unavailable service");
    if (options.hang) return new Promise((_, reject) => init.signal.addEventListener("abort", () => reject(new Error("deadline"))));
    if (path === "/api/connect") {
      assert.equal(init.headers["X-Sentinel-CSRF"], remote.csrf);
      assert.equal(init.mode, "same-origin");
      assert.equal(init.redirect, "error");
      remote.state = "connecting"; remote.session = "s".repeat(32); remote.revision++;
      connectAt = now;
      if (options.deferConnect) await new Promise(resolve => { releaseConnect = resolve; });
    } else if (path === "/api/disconnect") {
      assert.equal(JSON.parse(init.body).session, "s".repeat(32));
      if (remote.session === null) return { ok: false, status: 409, json: async () => ({ ...remote }) };
      remote.state = "stopping"; remote.session = null; remote.revision++;
    } else if (remote.state === "connecting" && now - connectAt >= 150 && !options.neverReady) remote.state = "connected";
    return response();
  }
  const location = { protocol: options.file ? "file:" : "http:", hostname: "127.0.0.1", hash: "#live" };
  vm.runInNewContext(source, {
    document: { getElementById: id => ids.get(id) },
    window: { addEventListener: (name, fn) => listeners.set(name, fn) },
    location, fetch, AbortController,
    Date: class extends Date { static now() { return now; } },
    setTimeout(fn, ms) { const id = ++sequence; timers.set(id, { at: now + ms, fn }); return id; },
    clearTimeout(id) { timers.delete(id); },
  });
  async function flush() { await new Promise(resolve => setImmediate(resolve)); }
  async function advance(ms) {
    await flush();
    const end = now + ms;
    for (let count = 0; count < 1000; count++) {
      const next = [...timers].filter(([, t]) => t.at <= end).sort((a, b) => a[1].at - b[1].at)[0];
      if (!next) break;
      now = next[1].at; timers.delete(next[0]); next[1].fn(); await flush();
    }
    now = end; await flush();
  }
  return { ids, calls, remote, transitions, advance, flush, timers, location,
    event: name => listeners.get(name)(), releaseConnect: () => releaseConnect() };
}

async function check(name, run) { await run(); passed++; console.log(`PASS (preview mock) ${name}`); }
async function main() {
  await check("standalone fallback makes no requests and disables Connect", async () => {
    const h = harness({ file: true });
    h.ids.get("preview-connect").click(); await h.advance(10000);
    assert.equal(h.calls.length, 0); assert.equal(h.ids.get("preview-connect").disabled, true);
    assert.match(h.ids.get("preview-feedback").textContent, /Standalone/);
  });
  await check("manual states, actual source assignment and serialized rapid controls", async () => {
    const h = harness();
    assert.equal(h.calls.length, 0);
    for (let i = 0; i < 20; i++) h.ids.get("preview-connect").click();
    await h.advance(500);
    assert.equal(h.calls.filter(c => c.path === "/api/connect").length, 1);
    assert.equal(h.ids.get("preview-state").textContent, "Connected");
    assert.equal(h.ids.get("preview-video").src, "/api/preview/" + "s".repeat(32));
    for (let i = 0; i < 20; i++) h.ids.get("preview-disconnect").click();
    assert.equal(h.ids.get("preview-video").hasAttribute("src"), false);
    await h.advance(1000);
    for (const state of ["Disconnected", "Connecting", "Connected", "Stopping"]) assert.ok(h.transitions.includes(state));
    assert.equal(h.ids.get("preview-state").textContent, "Disconnected");
    assert.equal(h.calls.filter(c => c.path === "/api/disconnect").length, 1);
    assert.equal(h.timers.size, 0);
  });
  await check("Disconnect during in-flight Connect stops its returned session", async () => {
    const h = harness({ deferConnect: true });
    h.ids.get("preview-connect").click(); await h.flush();
    h.ids.get("preview-disconnect").click(); h.releaseConnect(); await h.advance(1000);
    assert.equal(h.remote.session, null);
    assert.equal(h.ids.get("preview-video").hasAttribute("src"), false);
    assert.equal(h.ids.get("preview-state").textContent, "Disconnected");
  });
  await check("unavailable and stalled service produce recoverable errors", async () => {
    for (const options of [{ unavailable: true }, { hang: true }]) {
      const h = harness(options); h.ids.get("preview-connect").click(); await h.advance(3000);
      assert.equal(h.ids.get("preview-state").textContent, "Error");
      assert.equal(h.ids.get("preview-connect").disabled, false);
      assert.match(h.ids.get("preview-feedback").textContent, /unavailable/);
      assert.equal(h.timers.size, 0);
    }
  });
  await check("another page's session is neither adopted nor stopped", async () => {
    const h = harness({ busy: true }); h.ids.get("preview-connect").click(); await h.advance(1000);
    assert.equal(h.ids.get("preview-state").textContent, "Error");
    assert.deepEqual(h.calls.map(call => call.path), ["/api/status"]);
    assert.equal(h.remote.session, "o".repeat(32));
  });
  await check("startup and decoding timeouts disconnect and detach", async () => {
    for (const options of [{ neverReady: true }, { noDecode: true }]) {
      const h = harness(options); h.ids.get("preview-connect").click(); await h.advance(7000);
      assert.equal(h.ids.get("preview-state").textContent, "Error");
      assert.equal(h.remote.session, null);
      assert.equal(h.ids.get("preview-video").hasAttribute("src"), false);
      assert.equal(h.timers.size, 0);
    }
  });
  await check("decode errors, server expiry and navigation stop preview", async () => {
    for (const action of ["decode", "expiry", "navigate", "pagehide"]) {
      const h = harness(); h.ids.get("preview-connect").click(); await h.advance(500);
      if (action === "decode") h.ids.get("preview-video").dispatch("error");
      if (action === "expiry") { h.remote.state = "stopping"; h.remote.session = null; }
      if (action === "navigate") { h.location.hash = "#recordings"; h.event("hashchange"); }
      if (action === "pagehide") h.event("pagehide");
      await h.advance(1000);
      assert.equal(h.ids.get("preview-video").hasAttribute("src"), false);
      assert.equal(h.ids.get("preview-state").textContent, action === "decode" ? "Error" : "Disconnected");
      assert.equal(h.timers.size, 0);
    }
  });
  console.log(`${passed} preview mock checks passed; 0 failed; 0 skipped. No browser decoding evidence.`);
}
main().catch(error => { console.error(error); process.exitCode = 1; });

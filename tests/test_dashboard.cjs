"use strict";

// Dependency-free, foreground browser acceptance checks. Synthetic media only.
// Run: node tests/test_dashboard.cjs [absolute path to a Chromium browser] [--service]
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const path = require("node:path");
const { pathToFileURL } = require("node:url");
const { spawn } = require("node:child_process");
const vm = require("node:vm");
const { setTimeout: delay } = require("node:timers/promises");

const root = path.resolve(__dirname, "..");
const browserPath = process.argv.slice(2).find(arg => !arg.startsWith("--")) || "C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe";
const serviceMode = process.argv.includes("--service");
let service;
let serviceOrigin;
let browser;
let socket;
let profile;
let sequence = 0;
const pending = new Map();
const events = [];
let passed = 0;

async function until(check, message, timeout = 10000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    const result = await check();
    if (result) return result;
    await delay(50);
  }
  throw new Error(message);
}

function command(method, params = {}, sessionId) {
  if (process.env.SENTINEL_UI_TEST_DEBUG) console.log(`CDP send ${method}`);
  return new Promise((resolve, reject) => {
    const id = ++sequence;
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`Timed out: ${method}`));
    }, 15000);
    pending.set(id, {
      resolve: (value) => { clearTimeout(timer); resolve(value); },
      reject: (error) => { clearTimeout(timer); reject(error); },
    });
    socket.send(JSON.stringify({ id, method, params, sessionId }));
  });
}

async function main() {
  if (process.argv[2] === "--mock") return mockChecks();
  await fs.access(browserPath);
  if (serviceMode) {
    service = spawn("python", ["-E", "-s", "-B", "-m", "sentinel.live_view", "--port", "0"], {
      cwd: root, stdio: ["ignore", "pipe", "ignore"], windowsHide: true,
    });
    let output = "";
    let serviceError;
    service.on("error", error => { serviceError = error; });
    service.stdout.on("data", chunk => { output += chunk.toString(); });
    await until(() => {
      if (serviceError) throw serviceError;
      if (service.exitCode !== null) throw new Error("Synthetic service exited early");
      const match = /Synthetic preview only: (http:\/\/127\.0\.0\.1:\d+)/.exec(output);
      if (match) { serviceOrigin = match[1]; return true; }
      return false;
    }, "Synthetic service did not start");
  }
  await fs.mkdir(path.join(root, ".local"), { recursive: true });
  profile = await fs.mkdtemp(path.join(root, ".local", "ui-browser-"));
  browser = spawn(browserPath, [
    "--headless=new", "--disable-gpu", "--no-first-run", "--no-default-browser-check",
    "--disable-background-networking", "--disable-component-update", "--disable-sync",
    "--disable-extensions", "--disable-default-apps", "--metrics-recording-only",
    "--remote-debugging-address=127.0.0.1", "--remote-debugging-port=0",
    `--user-data-dir=${profile}`, "about:blank",
  ], { stdio: "ignore", windowsHide: true });
  let launchError;
  browser.on("error", (error) => { launchError = error; });
  const portFile = path.join(profile, "DevToolsActivePort");
  const connection = await until(async () => {
    if (launchError) throw launchError;
    try { return (await fs.readFile(portFile, "utf8")).trim().split(/\r?\n/); }
    catch (error) { if (error.code !== "ENOENT") throw error; return false; }
  }, "Browser did not expose its loopback debugging endpoint.");
  assert.match(connection[0], /^\d+$/);
  assert.match(connection[1], /^\/devtools\/browser\/[\w-]+$/);
  socket = new WebSocket(`ws://127.0.0.1:${connection[0]}${connection[1]}`);
  await new Promise((resolve, reject) => {
    socket.addEventListener("open", resolve, { once: true });
    socket.addEventListener("error", reject, { once: true });
  });
  socket.addEventListener("message", ({ data }) => {
    const message = JSON.parse(data);
    if (process.env.SENTINEL_UI_TEST_DEBUG) console.log(`CDP receive ${message.method || message.id}`);
    if (message.id) {
      const request = pending.get(message.id);
      if (!request) return;
      pending.delete(message.id);
      if (message.error) request.reject(new Error(message.error.message));
      else request.resolve(message.result);
    } else {
      events.push(message);
      if (message.method === "Fetch.requestPaused") {
        const allowed = serviceOrigin && message.params.request.url.startsWith(serviceOrigin + "/");
        command(allowed ? "Fetch.continueRequest" : "Fetch.failRequest", {
          requestId: message.params.requestId, ...(allowed ? {} : { errorReason: "BlockedByClient" }),
        }, message.sessionId).catch(() => {});
      }
    }
  });
  const { targetId } = await command("Target.createTarget", { url: "about:blank" });
  const { sessionId } = await command("Target.attachToTarget", { targetId, flatten: true });
  const send = (method, params) => command(method, params, sessionId);
  async function evaluate(expression) {
    const response = await send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true, userGesture: true });
    if (response.exceptionDetails) throw new Error(response.exceptionDetails.text + ": " + response.exceptionDetails.exception?.description);
    return response.result.value;
  }
  const check = async (name, run) => { await run(); passed += 1; console.log(`PASS ${name}`); };
  await send("Page.enable");
  await send("Runtime.enable");
  await send("Network.enable");
  // Block non-file/blob traffic at the harness as well as in the UI's CSP.
  await send("Network.setBlockedURLs", { urls: [...(serviceMode ? [] : ["http://*"]), "https://*", "ws://*", "wss://*"] });
  if (serviceMode) {
    await send("Fetch.enable", { patterns: [{ urlPattern: "http://*", requestStage: "Request" }] });
    await send("Page.navigate", { url: serviceOrigin + "/" });
    await until(() => evaluate("document.readyState === 'complete' && !!document.querySelector('#preview-connect') && !document.querySelector('#preview-connect').disabled"), "Service dashboard did not open");
    await check("service has no automatic activation; generated WebM decodes across HTTP boundary", async () => {
      assert.equal(await evaluate("document.querySelector('#preview-video').hasAttribute('src')"), false);
      assert.equal(await evaluate("(async () => (await (await fetch('/api/status')).json()).state)()"), "disconnected");
      await evaluate("document.querySelector('#preview-connect').click()");
      await until(() => evaluate("document.querySelector('#preview-video').readyState >= 2"), "HTTP synthetic WebM did not decode");
      await evaluate("document.querySelector('#preview-video').play()");
      await until(() => evaluate("document.querySelector('#preview-video').currentTime > 0.2"), "HTTP synthetic playback did not advance");
      assert.equal(await evaluate("document.querySelector('#preview-video').videoWidth"), 160);
      assert.equal(await evaluate("document.querySelector('#preview-video').videoHeight"), 90);
      assert.equal(await evaluate("new URL(document.querySelector('#preview-video').currentSrc).origin"), serviceOrigin);
      assert.equal(await evaluate("document.querySelector('#preview-state').textContent"), "Connected");
      assert.match(await evaluate("document.querySelector('#view-live').textContent"), /SYNTHETIC DEMO/);
    });
    await check("service Disconnect detaches playback; repeated Connect and cancellation recover", async () => {
      await evaluate("document.querySelector('#preview-disconnect').click()");
      await until(() => evaluate("document.querySelector('#preview-state').textContent === 'Disconnected'"), "Disconnect did not finish");
      assert.equal(await evaluate("document.querySelector('#preview-video').hasAttribute('src')"), false);
      for (let attempt = 0; attempt < 3; attempt++) {
        await evaluate("document.querySelector('#preview-connect').click(); document.querySelector('#preview-disconnect').click()");
        await until(() => evaluate("document.querySelector('#preview-state').textContent === 'Disconnected'"), "Rapid controls did not settle");
      }
    });
    await check("admitted sessions release and reconnect with fresh media ownership", async () => {
      const status = () => evaluate("(async () => { const response = await fetch('/api/status'); if (!response.ok) throw new Error('Status unavailable'); return response.json(); })()");
      const initial = await status();
      assert.equal(initial.state, "disconnected");
      assert.equal(initial.session, null);
      let previousSession = null;
      let previousRevision = initial.revision;
      async function observeDisconnect(mediaPath) {
        const observation = await evaluate(`(async () => {
          const video = document.querySelector('#preview-video');
          const state = await (await fetch('/api/status')).json();
          const oldMediaStatus = (await fetch(${JSON.stringify(mediaPath)})).status;
          return {
            hasSrcAttribute: video.hasAttribute('src'),
            srcAttribute: video.getAttribute('src'), currentSrc: video.currentSrc,
            hasSrcObject: video.srcObject !== null,
            hasChildSource: video.querySelector('source') !== null,
            paused: video.paused, readyState: video.readyState,
            networkState: video.networkState, hidden: video.hidden,
            currentTime: video.currentTime,
            service: { state: state.state, session: state.session, revision: state.revision },
            oldMediaStatus,
          };
        })()`);
        // Diagnostics describe only the synthetic resource relationship, never
        // the origin, per-session capability or CSRF material returned by status.
        const describeSource = value => value === null || value === "" ? value
          : value === mediaPath || value === serviceOrigin + mediaPath
            ? "<admitted synthetic session media>" : "<different resource>";
        return {
          ...observation,
          srcAttribute: describeSource(observation.srcAttribute),
          currentSrc: describeSource(observation.currentSrc),
          service: { ...observation.service, session: observation.service.session === null
            ? null : mediaPath === `/api/preview/${observation.service.session}`
              ? "<admitted session>" : "<different session>" },
          predicates: {
            noSrcAttribute: !observation.hasSrcAttribute,
            noSrcObject: !observation.hasSrcObject,
            noChildSource: !observation.hasChildSource,
            paused: observation.paused,
            haveNothing: observation.readyState === 0,
            networkEmpty: observation.networkState === 0,
            hidden: observation.hidden,
          },
        };
      }
      for (let attempt = 0; attempt < 2; attempt++) {
        assert.equal(await evaluate("document.querySelector('#preview-video').hasAttribute('src')"), false);
        await evaluate("document.querySelector('#preview-connect').click()");
        const admitted = await until(async () => {
          const state = await status();
          return ["connecting", "connected"].includes(state.state) && state.session ? state : false;
        }, "Connect did not create a server session");
        assert.match(admitted.session, /^[A-Za-z0-9_-]{32}$/);
        assert.notEqual(admitted.session, previousSession);
        assert.ok(admitted.revision > previousRevision);
        const mediaPath = `/api/preview/${admitted.session}`;
        await until(() => evaluate("document.querySelector('#preview-state').textContent === 'Connected' && document.querySelector('#preview-video').readyState >= 2"), "Admitted session did not load synthetic media");
        assert.equal(await evaluate("document.querySelector('#preview-video').getAttribute('src')"), mediaPath);
        assert.equal(await evaluate("document.querySelector('#preview-video').currentSrc"), serviceOrigin + mediaPath);
        await evaluate("document.querySelector('#preview-disconnect').click()");
        assert.equal(await evaluate("document.querySelector('#preview-video').hasAttribute('src')"), false);
        await until(async () => {
          const state = await status();
          return state.state === "disconnected" && state.session === null
            && await evaluate("document.querySelector('#preview-state').textContent === 'Disconnected' && !document.querySelector('#preview-connect').disabled");
        }, "Disconnect did not release the admitted server session");
        try {
          // Chromium can retain currentSrc after load() empties the element.
          // Verify source ownership and inactive media state instead of that
          // informational URL; a reconnected player must select the new URL above.
          await until(() => evaluate(`(() => {
            const video = document.querySelector('#preview-video');
            return !video.hasAttribute('src') && video.srcObject === null
              && !video.querySelector('source') && video.paused && video.hidden
              && video.readyState === HTMLMediaElement.HAVE_NOTHING
              && video.networkState === HTMLMediaElement.NETWORK_EMPTY;
          })()`), "Preview media did not detach and become inactive");
        } catch (error) {
          const observation = await observeDisconnect(mediaPath);
          console.log(`DISCONNECT DIAGNOSTICS ${JSON.stringify({ attempt: attempt + 1, ...observation })}`);
          throw error;
        }
        const observation = await observeDisconnect(mediaPath);
        if (process.env.SENTINEL_UI_MEDIA_DEBUG) console.log(`DISCONNECT DIAGNOSTICS ${JSON.stringify({ attempt: attempt + 1, ...observation })}`);
        assert.equal(observation.oldMediaStatus, 410);
        previousSession = admitted.session;
        previousRevision = admitted.revision;
      }
    });
    await check("service failure leaves Recordings and Motion History usable", async () => {
      await send("Network.setBlockedURLs", { urls: [serviceOrigin + "/api/*", "https://*", "ws://*", "wss://*"] });
      await evaluate("document.querySelector('#preview-connect').click()");
      await until(() => evaluate("document.querySelector('#preview-state').textContent === 'Error'"), "Unavailable service feedback missing");
      await evaluate("location.hash = '#history'");
      await until(() => evaluate("!document.querySelector('#view-history').hidden"), "History unavailable");
      assert.equal(await evaluate("document.querySelectorAll('#event-list tr').length"), 6);
      await send("Network.setBlockedURLs", { urls: ["https://*", "ws://*", "wss://*"] });
    });
  }
  await send("Page.addScriptToEvaluateOnNewDocument", { source: `
    window.urlAudit = { created: [], revoked: [], attachedAtRevoke: [] };
    const createURL = URL.createObjectURL.bind(URL);
    const revokeURL = URL.revokeObjectURL.bind(URL);
    URL.createObjectURL = (blob) => { const url = createURL(blob); urlAudit.created.push(url); return url; };
    URL.revokeObjectURL = (url) => {
      urlAudit.attachedAtRevoke.push(document.getElementById('video-player')?.getAttribute('src') === url);
      urlAudit.revoked.push(url); revokeURL(url);
    };
  ` });
  const dashboardURL = pathToFileURL(path.join(root, "ui", "index.html")).href;
  await send("Page.navigate", { url: dashboardURL });
  await until(() => evaluate("document.readyState === 'complete' && !!document.querySelector('#event-list tr')"), "Dashboard did not open.");

  await check("file:// launch, disconnected live state and local security policy", async () => {
    assert.equal(await evaluate("document.title"), "Live View · Sentinel");
    assert.equal(await evaluate("document.querySelector('#view-live').hidden"), false);
    assert.match(await evaluate("document.querySelector('.preview-center').textContent"), /Not connected/);
    assert.match(await evaluate("document.querySelector('meta[http-equiv]').content"), /connect-src 'none'/);
    assert.equal(await evaluate("document.querySelector('#video-player').hasAttribute('src')"), false);
  });
  const navigate = async (view) => {
    await evaluate(`document.querySelector('nav [data-view="${view}"]').click()`);
    await until(() => evaluate(`document.querySelector('#breadcrumb').textContent === ${JSON.stringify({ live: "Live View", recordings: "Recordings", history: "Motion History" }[view])}`), "Navigation did not finish.");
  };
  await check("navigation, hash fallback, history filtering and demo labels", async () => {
    await navigate("history");
    assert.equal(await evaluate("document.querySelectorAll('#event-list tr').length"), 6);
    assert.equal(await evaluate("Array.from(document.querySelectorAll('#event-list tr')).every(row => row.textContent.includes('Synthetic demo'))"), true);
    await evaluate("document.querySelector('#classification-filter').value = 'Person'; document.querySelector('#classification-filter').dispatchEvent(new Event('change'))");
    assert.equal(await evaluate("document.querySelectorAll('#event-list tr').length"), 2);
    assert.equal(await evaluate("document.querySelectorAll('nav [aria-current=page]').length"), 1);
    await evaluate("location.hash = '#toString'");
    await until(() => evaluate("!document.querySelector('#view-live').hidden"), "Invalid hash did not fall back.");
    await navigate("recordings");
  });
  await check("recordings empty state and both file picker buttons", async () => {
    assert.equal(await evaluate("document.querySelector('#recordings-empty').hidden"), false);
    await send("Page.setInterceptFileChooserDialog", { enabled: true });
    for (const button of ["choose-files", "choose-files-empty"]) {
      const start = events.length;
      await evaluate(`document.getElementById('${button}').click()`);
      await until(() => events.slice(start).some((event) => event.method === "Page.fileChooserOpened"), "Picker did not open.");
    }
    await send("Page.setInterceptFileChooserDialog", { enabled: false });
  });
  const choose = (filesExpression) => evaluate(`(() => {
    const transfer = new DataTransfer();
    for (const file of ${filesExpression}) transfer.items.add(file);
    const picker = document.querySelector('#file-picker');
    picker.files = transfer.files; picker.dispatchEvent(new Event('change'));
  })()`);
  await check("unsupported, empty and mismatched files rejected without object URLs", async () => {
    await choose("[new File(['x'], 'synthetic.txt', {type:'text/plain'}), new File([], 'empty.mp4', {type:'video/mp4'}), new File(['x'], 'mismatch.webm', {type:'text/html'})]");
    assert.match(await evaluate("document.querySelector('#file-feedback').textContent"), /3 file\(s\) skipped/);
    assert.equal(await evaluate("urlAudit.created.length"), 0);
    assert.equal(await evaluate("document.querySelector('#recordings-empty').hidden"), false);
  });
  await check("synthetic WebM decodes and actually plays", async () => {
    await evaluate(`(async () => {
      const canvas = document.createElement('canvas'); canvas.width = 320; canvas.height = 180;
      const ctx = canvas.getContext('2d');
      const stream = canvas.captureStream(10);
      const mimeType = ['video/webm;codecs=vp8', 'video/webm'].find(type => MediaRecorder.isTypeSupported(type));
      if (!mimeType) throw new Error('Synthetic WebM encoding is unavailable');
      const recorder = new MediaRecorder(stream, {mimeType}); const chunks = [];
      recorder.ondataavailable = event => chunks.push(event.data);
      const stopped = new Promise(resolve => recorder.onstop = resolve);
      recorder.start();
      for (let frame = 0; frame < 12; frame++) {
        ctx.fillStyle = frame % 2 ? '#245b43' : '#eaf0e3'; ctx.fillRect(0, 0, 320, 180);
        ctx.fillStyle = '#142b25'; ctx.fillRect(frame * 20, 65, 30, 30);
        await new Promise(resolve => setTimeout(resolve, 100));
      }
      recorder.stop(); await stopped; stream.getTracks().forEach(track => track.stop());
      window.syntheticClip = new File(chunks, 'synthetic-motion.webm', {type:'video/webm'});
    })()`);
    await choose("[syntheticClip]");
    await until(() => evaluate("document.querySelector('#video-player').readyState >= 2"), "Synthetic video did not decode.");
    await evaluate("document.querySelector('#video-player').play()");
    await until(() => evaluate("document.querySelector('#video-player').currentTime > 0.1"), "Synthetic playback did not advance.");
    assert.equal(await evaluate("document.querySelector('#video-player').videoWidth"), 320);
    await navigate("live");
    assert.equal(await evaluate("document.querySelector('#video-player').paused"), true);
    await navigate("recordings");
  });
  await check("literal file names, clip switching, removal and URL detachment", async () => {
    await choose("[new File([syntheticClip], '<img src=x>.webm', {type:'video/webm'})]");
    assert.equal(await evaluate("document.querySelectorAll('#clip-list img').length"), 0);
    assert.equal(await evaluate("document.querySelectorAll('.clip-select').length"), 2);
    await evaluate("document.querySelectorAll('.clip-select')[1].click()");
    assert.equal(await evaluate("document.querySelector('#playing-title').textContent"), "<img src=x>.webm");
    await evaluate("document.querySelectorAll('.remove-clip')[1].click()");
    assert.equal(await evaluate("urlAudit.revoked.length"), 1);
    assert.equal(await evaluate("document.querySelector('#playing-title').textContent"), "synthetic-motion.webm");
  });
  await check("decode failure has useful recovery text; clear releases all references", async () => {
    await choose("[new File(['not a video'], 'synthetic-invalid.mp4', {type:'video/mp4'})]");
    await evaluate("document.querySelectorAll('.clip-select')[1].click()");
    await until(() => evaluate("document.querySelector('#playback-feedback').textContent.includes('cannot be played')"), "Decode failure not shown.");
    await evaluate("document.querySelector('#clear-clips').click()");
    assert.equal(await evaluate("document.querySelector('#video-player').hasAttribute('src')"), false);
    assert.equal(await evaluate("document.querySelector('#clip-count').textContent"), "0");
    assert.equal(await evaluate("urlAudit.created.length === urlAudit.revoked.length"), true);
    assert.equal(await evaluate("urlAudit.attachedAtRevoke.some(Boolean)"), false);
  });
  await check("session limit, repeat selection, pagehide cleanup and fresh reload", async () => {
    await choose("Array.from({length:101}, () => syntheticClip)");
    assert.equal(await evaluate("document.querySelectorAll('.clip-select').length"), 100);
    assert.match(await evaluate("document.querySelector('#file-feedback').textContent"), /1 file\(s\) skipped/);
    await evaluate("window.dispatchEvent(new PageTransitionEvent('pagehide'))");
    assert.equal(await evaluate("urlAudit.created.length === urlAudit.revoked.length"), true);
    assert.equal(await evaluate("urlAudit.attachedAtRevoke.some(Boolean)"), false);
    await choose("[syntheticClip]");
    assert.equal(await evaluate("document.querySelectorAll('.clip-select').length"), 1);
    await send("Page.reload");
    await until(() => evaluate("document.readyState === 'complete' && document.querySelector('#clip-count')?.textContent === '0'"), "Reload retained clips.");
  });
  await check("desktop and mobile layouts fit all three views", async () => {
    for (const width of [1440, 768, 390, 320]) {
      await send("Emulation.setDeviceMetricsOverride", { width, height: 900, deviceScaleFactor: 1, mobile: width < 680 });
      for (const view of ["live", "recordings", "history"]) {
        await navigate(view);
        assert.equal(await evaluate("document.documentElement.scrollWidth <= innerWidth"), true, `Page overflow: ${width}/${view}`);
        assert.equal(await evaluate("document.querySelector('h1').getBoundingClientRect().width > 0"), true);
      }
    }
  });
  await check("no dashboard network traffic, runtime errors or browser persistence", async () => {
    assert.equal(events.filter(event => event.method === "Runtime.exceptionThrown").length, 0);
    const remoteRequests = events.filter(event => event.method === "Network.requestWillBeSent" && /^(https?|wss?):/.test(event.params.request.url));
    assert.ok(remoteRequests.every(event => serviceMode && event.params.request.url.startsWith(serviceOrigin + "/")));
    assert.equal(await evaluate("localStorage.length + sessionStorage.length"), 0);
  });
  console.log(`${passed} browser checks passed; 0 failed; 0 skipped.`);
}

async function mockChecks() {
  const html = await fs.readFile(path.join(root, "ui", "index.html"), "utf8");
  const source = await fs.readFile(path.join(root, "ui", "app.js"), "utf8");
  const ids = new Map();
  const listeners = new Map();
  const created = [];
  const revoked = [];
  let focused;
  class Element {
    constructor(tag = "div") {
      this.tag = tag; this.children = []; this.attributes = new Map();
      this.listeners = new Map(); this.dataset = {}; this.hidden = false;
      this.textContent = ""; this.value = ""; this.files = []; this.paused = true;
    }
    setAttribute(name, value) { this.attributes.set(name, value); }
    getAttribute(name) { return this.attributes.get(name) ?? null; }
    hasAttribute(name) { return this.attributes.has(name); }
    removeAttribute(name) { this.attributes.delete(name); }
    set src(value) { this.setAttribute("src", value); }
    get src() { return this.getAttribute("src"); }
    append(...items) { this.children.push(...items); }
    replaceChildren(...items) { this.children = items; }
    addEventListener(type, callback) {
      if (!this.listeners.has(type)) this.listeners.set(type, []);
      this.listeners.get(type).push(callback);
    }
    dispatch(type) { for (const callback of this.listeners.get(type) || []) callback(); }
    click() { this.dispatch("click"); }
    focus() { focused = this; }
    pause() { this.paused = true; }
    load() { this.loadCalls = (this.loadCalls || 0) + 1; }
    querySelector(selector) {
      for (const child of this.children) {
        if ((selector === "button" && child.tag === "button") ||
            (selector === '[aria-pressed="true"]' && child.getAttribute("aria-pressed") === "true")) return child;
        const nested = child.querySelector(selector);
        if (nested) return nested;
      }
      return null;
    }
  }
  for (const match of html.matchAll(/<(\w+)\b([^>]*\bid="([^"]+)"[^>]*)>/g)) {
    const element = new Element(match[1]);
    element.hidden = /\bhidden\b/.test(match[2]);
    ids.set(match[3], element);
  }
  const nav = ["live", "recordings", "history"].map(view => {
    const link = new Element("a"); link.dataset.view = view; return link;
  });
  ids.get("classification-filter").value = "all";
  const player = ids.get("video-player");
  const location = { hash: "" };
  const document = {
    getElementById: id => ids.get(id),
    createElement: tag => new Element(tag),
    querySelectorAll: selector => { assert.equal(selector, "nav [data-view]"); return nav; },
  };
  const window = { addEventListener: (type, callback) => listeners.set(type, callback) };
  const context = vm.createContext({ document, window, location, URL: {
    createObjectURL: () => { const url = `blob:synthetic-${created.length}`; created.push(url); return url; },
    revokeObjectURL: url => {
      assert.notEqual(player.src, url, "Revoke must follow player detachment");
      assert.ok(!revoked.includes(url), "URL must be revoked once"); revoked.push(url);
    },
  } });
  vm.runInContext(source, context, { filename: "ui/app.js" });
  const check = (name, run) => { run(); passed += 1; console.log(`PASS (mock) ${name}`); };
  const navigate = view => { location.hash = `#${view}`; listeners.get("hashchange")(); };
  const choose = files => { ids.get("file-picker").files = files; ids.get("file-picker").dispatch("change"); };
  const synthetic = { name: "synthetic.webm", type: "video/webm", size: 1024 };
  const rows = () => ids.get("clip-list").children;

  check("source loads from disk; assets and security policy are local", () => {
    assert.match(html, /connect-src 'none'/);
    assert.match(html, /media-src blob:/);
    assert.deepEqual([...html.matchAll(/(?:src|href)="([^"]*)"/g)].map(match => match[1]).filter(value => !value.startsWith("#")), ["styles.css", "app.js", "live.js"]);
    assert.doesNotMatch(source, /\b(?:fetch|XMLHttpRequest|WebSocket|localStorage|sessionStorage|indexedDB)\b/);
    assert.equal(ids.get("view-live").hidden, false);
    assert.equal(player.hasAttribute("src"), false);
  });
  check("three views, active navigation, unknown hash and prototype-name fallback", () => {
    for (const view of ["live", "recordings", "history"]) {
      navigate(view);
      assert.equal(ids.get(`view-${view}`).hidden, false);
      assert.equal(nav.filter(link => link.hasAttribute("aria-current")).length, 1);
      assert.equal(focused, ids.get("main"));
    }
    for (const invalid of ["unknown", "toString", "__proto__"]) {
      navigate(invalid); assert.equal(ids.get("view-live").hidden, false);
    }
  });
  check("fictional UTC events and classification filtering", () => {
    assert.equal(ids.get("event-list").children.length, 6);
    const event = ids.get("event-list").children[0];
    assert.equal(event.children[3].children[0].textContent, "Synthetic demo");
    assert.equal(event.children[0].children[0].dateTime, "2026-11-01T14:32:08Z");
    ids.get("classification-filter").value = "Person";
    ids.get("classification-filter").dispatch("change");
    assert.equal(ids.get("event-list").children.length, 2);
  });
  check("empty state and both picker buttons", () => {
    navigate("recordings");
    assert.equal(ids.get("recordings-empty").hidden, false);
    let clicks = 0;
    ids.get("file-picker").addEventListener("click", () => clicks++);
    ids.get("choose-files").click(); ids.get("choose-files-empty").click();
    assert.equal(clicks, 2);
  });
  check("unsupported extensions, MIME mismatch and empty files reject before URL creation", () => {
    choose([{ ...synthetic, name: "test.txt" }, { ...synthetic, type: "text/html" }, { ...synthetic, size: 0 }]);
    assert.equal(created.length, 0);
    assert.match(ids.get("file-feedback").textContent, /3 file\(s\) skipped/);
  });
  check("selection loads a blob into the player and renders filenames as text", () => {
    choose([synthetic, { ...synthetic, name: "<img src=x>.WEBM", type: "" }, { ...synthetic, name: "synthetic.mp4", type: "video/mp4" }]);
    assert.equal(rows().length, 3);
    assert.equal(player.src, created[0]);
    assert.equal(player.loadCalls, 2);
    player.dispatch("loadedmetadata");
    assert.match(ids.get("playback-feedback").textContent, /Ready/);
    rows()[1].children[0].click();
    assert.equal(ids.get("playing-title").textContent, "<img src=x>.WEBM");
    assert.equal(rows()[1].children[0].children[0].children[0].children.length, 0);
    assert.equal(player.src, created[1]);
    player.paused = false; navigate("live"); assert.equal(player.paused, true);
    navigate("recordings");
  });
  check("decode errors recover after switching; selected and unselected removal release URLs", () => {
    player.dispatch("error");
    assert.match(ids.get("playback-feedback").textContent, /cannot be played/);
    rows()[1].children[1].click();
    assert.equal(player.src, created[0]);
    assert.equal(revoked.length, 1);
    assert.doesNotMatch(ids.get("playback-feedback").textContent, /cannot be played/);
    rows()[1].children[1].click();
    assert.equal(revoked.length, 2);
    rows()[0].children[1].click();
    assert.equal(ids.get("recordings-empty").hidden, false);
    assert.equal(player.hasAttribute("src"), false);
    assert.equal(focused, ids.get("choose-files"));
  });
  check("clear, repeat selection, session capacity and pagehide release all URLs", () => {
    choose([synthetic]); ids.get("clear-clips").click();
    choose(Array.from({ length: 101 }, () => synthetic));
    assert.equal(rows().length, 100);
    assert.match(ids.get("file-feedback").textContent, /1 file\(s\) skipped/);
    listeners.get("pagehide")();
    assert.equal(created.length, revoked.length);
    assert.equal(ids.get("clip-count").textContent, "0");
    assert.equal(player.hasAttribute("src"), false);
    assert.equal(ids.get("file-picker").value, "");
  });
  console.log(`${passed} mock checks passed; 0 failed; 0 skipped. Native decoding, layout and file dialogs are not verified by mocks.`);
}

main().catch((error) => {
  console.error(`FAIL after ${passed} checks: ${error.message}`);
  process.exitCode = 1;
}).finally(async () => {
  if (service && service.exitCode === null) {
    const stopped = new Promise(resolve => service.once("exit", resolve));
    service.kill();
    await Promise.race([stopped, delay(3000)]);
  }
  if (socket?.readyState === WebSocket.OPEN) {
    try { await command("Browser.close"); } catch { /* Browser may close before replying. */ }
    socket.close();
  }
  if (browser && browser.exitCode === null) {
    await Promise.race([new Promise(resolve => browser.once("exit", resolve)), delay(3000)]);
    if (browser.exitCode === null) browser.kill();
  }
  if (profile) {
    // Only remove the fresh test profile created in this worktree.
    assert.equal(path.dirname(profile), path.join(root, ".local"));
    assert.ok(path.basename(profile).startsWith("ui-browser-"));
    await fs.rm(profile, { recursive: true, force: true, maxRetries: 10, retryDelay: 200 });
  }
});

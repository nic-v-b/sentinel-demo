"use strict";

(() => {
  const byId = id => document.getElementById(id);
  const video = byId("preview-video");
  const connect = byId("preview-connect");
  const disconnect = byId("preview-disconnect");
  const localService = location.protocol === "http:" && location.hostname === "127.0.0.1";
  let desired = false;
  let running = false;
  let epoch = 0;
  let session = null;
  let csrf = null;
  let playbackDeadline = 0;
  let playbackFailed = false;

  function detach() {
    video.pause();
    video.removeAttribute("src");
    video.load();
    video.hidden = true;
    playbackDeadline = 0;
  }

  function render(state, message) {
    byId("preview-state").textContent = state[0].toUpperCase() + state.slice(1);
    byId("preview-feedback").textContent = message;
    byId("preview-placeholder").hidden = video.hasAttribute("src");
    connect.disabled = !localService || running || desired;
    disconnect.disabled = !running && !desired;
  }

  async function request(path, body) {
    const abort = new AbortController();
    const timer = setTimeout(() => abort.abort(), 2500);
    try {
      const response = await fetch(path, {
        method: body ? "POST" : "GET", mode: "same-origin", credentials: "omit",
        cache: "no-store", redirect: "error", signal: abort.signal,
        headers: body ? { "Content-Type": "application/json", "X-Sentinel-CSRF": csrf } : {},
        body: body ? JSON.stringify(body) : undefined,
      });
      const data = await response.json();
      // A stale Disconnect is complete when its session is already revoked.
      if (!response.ok && !(path === "/api/disconnect" && response.status === 409
          && data.session !== body.session)) throw new Error("Service request rejected");
      if (!["disconnected", "connecting", "connected", "stopping", "error"].includes(data.state)
          || !Number.isSafeInteger(data.revision)
          || !(data.session === null || typeof data.session === "string" && /^[A-Za-z0-9_-]{32}$/.test(data.session))) {
        throw new Error("Invalid service response");
      }
      return data;
    } finally {
      clearTimeout(timer);
    }
  }

  async function run() {
    if (running) return;
    running = true;
    const currentEpoch = epoch;
    let failure = false;
    const startupDeadline = Date.now() + 5000;
    render("connecting", "Opening the synthetic local preview…");
    try {
      const initial = await request("/api/status");
      if (currentEpoch !== epoch || !desired) return;
      if (typeof initial.csrf !== "string" || !/^[A-Za-z0-9_-]{43}$/.test(initial.csrf)) throw new Error("Invalid bootstrap");
      csrf = initial.csrf;
      // An existing session belongs to another page. Never adopt or stop it.
      if (!["disconnected", "error"].includes(initial.state)) throw new Error("Preview busy");
      let state = await request("/api/connect", { revision: initial.revision });
      session = state.session;
      if (currentEpoch !== epoch) return;
      while (desired) {
        if (state.state === "error" || playbackFailed) throw new Error("Preview failed");
        if (state.session !== session || ["disconnected", "stopping"].includes(state.state)) {
          session = null;
          break;
        }
        if (state.state === "connected") {
          if (!video.hasAttribute("src")) {
            video.src = `/api/preview/${session}`;
            video.hidden = false;
            video.load();
            playbackDeadline = Date.now() + 5000;
            // Muted playback is normally permitted; native controls remain usable.
            video.play().catch(() => {
              if (desired) byId("preview-feedback").textContent = "Synthetic preview ready. Use the video Play control.";
            });
          }
          render("connected", video.readyState >= 2
            ? "Synthetic animation only · no camera or audio. Session ends within 60 seconds."
            : "Connected to the synthetic service. Waiting for browser decoding…");
          if (video.readyState >= 2) playbackDeadline = 0;
          if (playbackDeadline && Date.now() >= playbackDeadline) throw new Error("Playback timeout");
        } else if (Date.now() >= startupDeadline) throw new Error("Startup timeout");
        await new Promise(resolve => setTimeout(resolve, 250));
        if (currentEpoch !== epoch) return;
        if (!desired) break;
        state = await request("/api/status");
      }
    } catch {
      failure = true;
    } finally {
      desired = false;
      detach();
      render("stopping", "Releasing the synthetic preview…");
      if (session && csrf) {
        try {
          await request("/api/disconnect", { session });
          await new Promise(resolve => setTimeout(resolve, 200));
        }
        catch { failure = true; }
      }
      session = null;
      csrf = null;
      running = false;
      playbackFailed = false;
      render(failure ? "error" : "disconnected", failure
        ? "Preview unavailable, busy, or unsupported. Start the local service and try Connect again. Any unreachable session expires automatically."
        : "Disconnected. Connect starts only a generated synthetic animation.");
    }
  }

  connect.addEventListener("click", () => {
    if (!localService || running) return;
    desired = true;
    run();
  });
  function stop() {
    desired = false;
    detach();
    if (running) render("stopping", "Releasing the synthetic preview…");
  }
  disconnect.addEventListener("click", stop);
  video.addEventListener("error", () => {
    if (video.hasAttribute("src")) playbackFailed = true;
  });
  window.addEventListener("hashchange", () => {
    if (["#recordings", "#history"].includes(location.hash)) stop();
  });
  window.addEventListener("pagehide", () => {
    epoch += 1;
    stop();
    // Best effort only. Closing during an in-flight Connect relies on the TTL.
    if (session && csrf) {
      fetch("/api/disconnect", {
        method: "POST", mode: "same-origin", credentials: "omit", redirect: "error", keepalive: true,
        headers: { "Content-Type": "application/json", "X-Sentinel-CSRF": csrf },
        body: JSON.stringify({ session }),
      }).catch(() => {});
    }
  });
  render("disconnected", localService
    ? "Disconnected. Connect starts only a generated synthetic animation."
    : "Standalone mode. Preview controls require the local service; your local clips and demo history are available.");
})();

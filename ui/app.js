"use strict";

(() => {
  const byId = (id) => document.getElementById(id);
  const views = {
    live: ["Live View", "A quiet place to keep an eye on things."],
    recordings: ["Recordings", "Your local clips. One private place to watch."],
    history: ["Motion History", "Explore the timeline. All activity shown here is synthetic."],
  };
  const player = byId("video-player");
  const picker = byId("file-picker");
  const clips = [];
  let selectedClip = null;

  function navigate(focusHeading = false) {
    const route = location.hash.slice(1);
    const view = Object.hasOwn(views, route) ? route : "live";
    for (const key of Object.keys(views)) byId(`view-${key}`).hidden = key !== view;
    document.querySelectorAll("nav [data-view]").forEach((link) => {
      if (link.dataset.view === view) link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
    });
    byId("page-title").textContent = byId("breadcrumb").textContent = views[view][0];
    byId("page-description").textContent = views[view][1];
    document.title = `${views[view][0]} · Sentinel`;
    if (view !== "recordings") player.pause();
    if (focusHeading) byId("main").focus();
  }

  function detachPlayer() {
    player.pause();
    player.removeAttribute("src");
    player.load();
    selectedClip = null;
    byId("playing-title").textContent = "Select a clip";
    byId("playback-feedback").textContent = "Use the player controls to begin playback.";
  }

  function selectClip(clip) {
    detachPlayer();
    selectedClip = clip;
    byId("playing-title").textContent = clip.file.name;
    player.src = clip.url;
    player.load();
    renderClips();
  }

  function removeClip(clip) {
    const wasSelected = selectedClip === clip;
    if (wasSelected) detachPlayer();
    clips.splice(clips.indexOf(clip), 1);
    URL.revokeObjectURL(clip.url);
    if (wasSelected && clips.length) selectClip(clips[0]);
    else renderClips();
    // Keep keyboard focus usable after the focused row disappears.
    (byId("clip-list").querySelector("button") || byId("choose-files")).focus();
  }

  function renderClips() {
    byId("clip-count").textContent = String(clips.length);
    byId("recordings-empty").hidden = clips.length > 0;
    byId("recordings-content").hidden = clips.length === 0;
    byId("clip-list").replaceChildren();
    for (const clip of clips) {
      const row = document.createElement("li");
      const button = document.createElement("button");
      button.className = "clip-select";
      button.setAttribute("aria-pressed", String(selectedClip === clip));
      const text = document.createElement("span");
      const name = document.createElement("span");
      name.className = "clip-name";
      name.textContent = clip.file.name;
      const meta = document.createElement("span");
      meta.className = "clip-meta";
      meta.textContent = `${clip.file.name.split(".").pop().toUpperCase()} · ${(clip.file.size / 1048576).toFixed(2)} MB · Local file`;
      text.append(name, meta);
      button.append(text);
      button.addEventListener("click", () => {
        selectClip(clip);
        byId("clip-list").querySelector('[aria-pressed="true"]').focus();
      });
      const remove = document.createElement("button");
      remove.className = "remove-clip";
      remove.textContent = "×";
      remove.setAttribute("aria-label", `Remove ${clip.file.name}`);
      remove.addEventListener("click", () => removeClip(clip));
      row.append(button, remove);
      byId("clip-list").append(row);
    }
  }

  function clearClips() {
    // Detach the media element before revoking its source.
    detachPlayer();
    for (const clip of clips) URL.revokeObjectURL(clip.url);
    clips.length = 0;
    picker.value = "";
    byId("file-feedback").textContent = "";
    renderClips();
  }

  picker.addEventListener("change", () => {
    let rejected = 0;
    let unavailable = 0;
    for (const file of picker.files) {
      const extension = /\.(mp4|webm)$/i.exec(file.name)?.[1].toLowerCase();
      const expectedType = extension === "mp4" ? "video/mp4" : "video/webm";
      if (!extension || !file.size || (file.type && file.type !== expectedType)) {
        rejected += 1;
        continue;
      }
      // File labels are only a preliminary check; the browser validates decoding.
      // Bound the session list without reading or buffering whole files.
      if (clips.length >= 100) {
        rejected += 1;
        continue;
      }
      try {
        clips.push({ file, url: URL.createObjectURL(file) });
      } catch {
        unavailable += 1;
      }
    }
    picker.value = "";
    byId("file-feedback").textContent = [
      rejected ? `${rejected} file(s) skipped. Choose nonempty MP4/WebM files (up to 100 clips per session).` : "",
      unavailable ? "Some clips could not be opened. Try selecting them again." : "",
    ].filter(Boolean).join(" ");
    if (!selectedClip && clips.length) selectClip(clips[0]);
    else renderClips();
  });
  byId("choose-files").addEventListener("click", () => picker.click());
  byId("choose-files-empty").addEventListener("click", () => picker.click());
  byId("clear-clips").addEventListener("click", () => {
    clearClips();
    byId("choose-files").focus();
  });
  player.addEventListener("error", () => {
    if (selectedClip && player.hasAttribute("src")) {
      byId("playback-feedback").textContent = "This clip cannot be played. It may be damaged or use an unsupported codec. Choose another MP4/WebM file.";
    }
  });
  player.addEventListener("loadedmetadata", () => {
    if (selectedClip && !player.error) byId("playback-feedback").textContent = "Ready. Use the player controls to begin playback.";
  });

  // Hand-authored fiction, deliberately independent of the observer and recorder.
  const demoEvents = [
    ["14:32:08", "Person", "Demo entrance"],
    ["13:48:21", "Animal", "Demo garden"],
    ["12:15:42", "Vehicle", "Demo driveway"],
    ["10:06:15", "Unclassified", "Demo garden"],
    ["09:41:03", "Person", "Demo entrance"],
    ["08:23:17", "Animal", "Demo garden"],
  ];
  function renderEvents() {
    const filter = byId("classification-filter").value;
    const events = demoEvents.filter((event) => filter === "all" || event[1] === filter);
    byId("event-count").textContent = String(events.length);
    byId("event-list").replaceChildren();
    for (const [time, classification, source] of events) {
      const row = document.createElement("tr");
      const values = [time, classification, source, "Synthetic demo"];
      values.forEach((value, index) => {
        const cell = document.createElement("td");
        const content = document.createElement(index === 0 ? "time" : "span");
        content.textContent = value;
        if (index === 0) content.dateTime = `2026-11-01T${time}Z`;
        if (index === 1) content.className = "classification";
        if (index === 3) content.className = "evidence";
        cell.append(content);
        row.append(cell);
      });
      byId("event-list").append(row);
    }
  }
  byId("classification-filter").addEventListener("change", renderEvents);
  window.addEventListener("hashchange", () => navigate(true));
  // pagehide also handles back/forward cache entry; pageshow gets a clean library.
  window.addEventListener("pagehide", clearClips);
  renderEvents();
  renderClips();
  navigate();
})();

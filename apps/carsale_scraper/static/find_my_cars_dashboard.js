const els = {
  outDir: document.getElementById("out_dir"),
  manualUrls: document.getElementById("manual_urls"),
  stockIdInput: document.getElementById("stockIdInput"),
  stockIdDropdown: document.getElementById("stockIdDropdown"),
  stockIdSelected: document.getElementById("stockIdSelected"),
  clearStockIdsBtn: document.getElementById("clearStockIdsBtn"),
  runModeAll: document.getElementById("runModeAll"),
  runModeNotFound: document.getElementById("runModeNotFound"),
  runModeStockOnly: document.getElementById("runModeStockOnly"),
  openUrlsInBrowser: document.getElementById("open_urls_in_browser"),
  browserVerificationOnly: document.getElementById("browser_verification_only"),
  includeArchivedUrls: document.getElementById("include_archived_urls"),
  saveUrlsBtn: document.getElementById("saveUrlsBtn"),
  savedUrlsMeta: document.getElementById("savedUrlsMeta"),
  runBtn: document.getElementById("runBtn"),
  stopBtn: document.getElementById("stopBtn"),
  refreshBtn: document.getElementById("refreshBtn"),
  sessionModeBtn: document.getElementById("sessionModeBtn"),
  openBtn: document.getElementById("openBtn"),
  reloadDataBtn: document.getElementById("reloadDataBtn"),
  refreshInventoryBtn: document.getElementById("refreshInventoryBtn"),
  runStatus: document.getElementById("runStatus"),
  liveStage: document.getElementById("liveStage"),
  liveProgressFill: document.getElementById("liveProgressFill"),
  liveProgressText: document.getElementById("liveProgressText"),
  liveEta: document.getElementById("liveEta"),
  liveRemaining: document.getElementById("liveRemaining"),
  liveFound: document.getElementById("liveFound"),
  liveSuspected: document.getElementById("liveSuspected"),
  liveVerified: document.getElementById("liveVerified"),
  liveCards: document.getElementById("liveCards"),
  liveTargets: document.getElementById("liveTargets"),
  livePage: document.getElementById("livePage"),
  liveSkipped: document.getElementById("liveSkipped"),
  liveUpdated: document.getElementById("liveUpdated"),
  liveTargetLabel: document.getElementById("liveTargetLabel"),
  statusBadge: document.getElementById("statusBadge"),
  totalCarsBadge: document.getElementById("totalCarsBadge"),
  identifiedBadge: document.getElementById("identifiedBadge"),
  notFoundBadge: document.getElementById("notFoundBadge"),
  tooManyBadge: document.getElementById("tooManyBadge"),
  onOfferBadge: document.getElementById("onOfferBadge"),
  unpublishedBadge: document.getElementById("unpublishedBadge"),
  kpiInventory: document.getElementById("kpiInventory"),
  kpiSubmittedUrls: document.getElementById("kpiSubmittedUrls"),
  kpiIdentified: document.getElementById("kpiIdentified"),
  kpiNotFound: document.getElementById("kpiNotFound"),
  kpiOnOffer: document.getElementById("kpiOnOffer"),
  kpiUnpublished: document.getElementById("kpiUnpublished"),
  kpiLastRun: document.getElementById("kpiLastRun"),
  rows: document.getElementById("carRows"),
  search: document.getElementById("searchInput"),
  statusFilter: document.getElementById("statusFilter"),
  makeFilter: document.getElementById("makeFilter"),
  modelFilter: document.getElementById("modelFilter"),
  minPrice: document.getElementById("minPrice"),
  maxPrice: document.getElementById("maxPrice"),
  quickAddBtn: document.getElementById("quickAddBtn"),
  quickAddModal: document.getElementById("quickAddModal"),
  quickAddClose: document.getElementById("quickAddClose"),
  quickAddInput: document.getElementById("quickAddInput"),
  quickAddSave: document.getElementById("quickAddSave"),
};

const state = {
  status: null,
  rows: [],
  summary: {},
  availableStockIds: [],
  selectedStockIds: new Set(),
  outDir: "",
  sessionPoolDir: "",
  sessionReuseEnabled: true,
  headlessMode: false,
  openUrlsInBrowser: true,
  browserVerificationOnly: true,
};

let imageViewerBound = false;
let progressStream = null;

function esc(s) {
  return String(s || "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[c]));
}

function stripCropParam(url) {
  try {
    const u = new URL(url);
    const params = new URLSearchParams(u.search);
    if ((params.get("pxc_method") || "").toLowerCase() === "crop") {
      params.delete("pxc_method");
      u.search = params.toString();
    }
    return u.toString();
  } catch {
    return url || "";
  }
}

function toNum(v) {
  const s = String(v ?? "").replace(/[^\d]/g, "");
  return s ? Number(s) : null;
}

function fmtDate(ts) {
  if (!ts) return "No run yet";
  return new Date(ts * 1000).toLocaleString();
}

function fmtDuration(sec) {
  const s = Number(sec);
  if (!Number.isFinite(s) || s < 0) return "--";
  if (s < 60) return `${Math.round(s)}s`;
  const m = Math.floor(s / 60);
  const rem = Math.round(s % 60);
  if (m < 60) return `${m}m ${rem}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function setStatusBadge(text) {
  els.statusBadge.textContent = `Status: ${text}`;
}

function getOutDir() {
  if (els.outDir && String(els.outDir.value || "").trim()) return String(els.outDir.value || "").trim();
  return state.outDir || "";
}

function getSessionPoolDir() {
  const explicit = String(state.sessionPoolDir || "").trim();
  if (explicit) return explicit;
  const out = String(getOutDir() || "").trim();
  if (!out) return "";
  return `${out.replace(/[\\/]+$/, "")}/session_pool`;
}

function isSessionReuseEnabled() {
  return !!state.sessionReuseEnabled;
}

function renderSessionModeButton() {
  if (!els.sessionModeBtn) return;
  if (isSessionReuseEnabled()) {
    els.sessionModeBtn.textContent = "Session Reuse: ON";
    els.sessionModeBtn.title = "Stored session is reused (fewer captcha prompts).";
  } else {
    els.sessionModeBtn.textContent = "Session Reuse: OFF (Captcha Mode)";
    els.sessionModeBtn.title = "Session reuse disabled. Fresh browser context so captcha can appear.";
  }
}

function countValidUrls(text) {
  return (text || "")
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter((s) => s && s.toLowerCase().includes("carsales.com.au/cars/details/")).length;
}

function renderSavedUrlsMeta(count, archivedCount = 0) {
  if (!els.savedUrlsMeta) return;
  els.savedUrlsMeta.textContent = `${Number(count || 0)} active URLs, ${Number(archivedCount || 0)} archived`;
}

function renderSelectedStockIds() {
  if (!els.stockIdSelected) return;
  const ids = [...state.selectedStockIds];
  if (!ids.length) {
    els.stockIdSelected.innerHTML = `<span class="stock-empty">No stock IDs selected</span>`;
    return;
  }
  els.stockIdSelected.innerHTML = ids.map((id) =>
    `<button class="stock-chip" type="button" data-stock-id="${esc(id)}">${esc(id)} <span aria-hidden="true">x</span></button>`
  ).join("");
}

function collectAvailableStockIds() {
  const set = new Set();
  for (const entry of state.rows || []) {
    const c = entry.carbarn || {};
    const s = entry.carsales || {};
    const vals = [c.stock_no, s.stock_no, s.dealer_stock_id, ...(Array.isArray(s.dealer_stock_ids) ? s.dealer_stock_ids : [])];
    for (const v0 of vals) {
      const v = String(v0 || "").trim();
      if (!v) continue;
      set.add(v);
      const digits = v.replace(/\D/g, "");
      if (digits) set.add(digits);
    }
  }
  state.availableStockIds = [...set].sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
}

function renderStockIdDropdown() {
  if (!els.stockIdDropdown || !els.stockIdInput) return;
  const q = els.stockIdInput.value.trim().toLowerCase();
  const options = state.availableStockIds
    .filter((id) => !state.selectedStockIds.has(id))
    .filter((id) => !q || id.toLowerCase().includes(q))
    .slice(0, 80);
  if (!options.length) {
    els.stockIdDropdown.classList.remove("show");
    els.stockIdDropdown.innerHTML = "";
    return;
  }
  els.stockIdDropdown.innerHTML = options
    .map((id) => `<button class="stock-option" type="button" data-stock-id="${esc(id)}">${esc(id)}</button>`)
    .join("");
  els.stockIdDropdown.classList.add("show");
}

function addSelectedStockId(raw) {
  const id = String(raw || "").trim();
  if (!id) return;
  state.selectedStockIds.add(id);
  renderSelectedStockIds();
  renderStockIdDropdown();
}

async function openInAntibot(url) {
  const res = await api("/api/open-antibot-url", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      url,
      out_dir: getOutDir(),
      session_pool_dir: getSessionPoolDir(),
      session_reuse_enabled: isSessionReuseEnabled(),
      save_storage_state_on_exit: isSessionReuseEnabled(),
    }),
  });
  if (!res.ok) {
    alert(res.error || "Failed to open anti-bot browser");
  }
}

function renderLiveProgress(progress) {
  const p = progress || {};
  const percent = Math.max(0, Math.min(100, Number(p.progress_percent || 0)));
  const stage = (p.stage || "idle").toString();
  const targetsDone = Number(p.targets_done || 0);
  const targetsTotal = Number(p.targets_total || 0);

  if (els.liveProgressFill) els.liveProgressFill.style.width = `${percent}%`;
  if (els.liveProgressText) els.liveProgressText.textContent = `${percent.toFixed(1)}%`;
  if (els.liveStage) els.liveStage.textContent = `Stage: ${stage}`;
  if (els.liveEta) els.liveEta.textContent = `ETA: ${fmtDuration(p.eta_seconds)}`;
  if (els.liveRemaining) els.liveRemaining.textContent = `URLs Left: ${Number(p.remaining_count || 0)}`;
  if (els.liveFound) els.liveFound.textContent = String(Number(p.my_cars_total || 0));
  if (els.liveSuspected) els.liveSuspected.textContent = String(Number(p.suspected_total || 0));
  if (els.liveVerified) els.liveVerified.textContent = String(Number(p.verified_done || 0));
  if (els.liveCards) els.liveCards.textContent = String(Number(p.cards_collected || 0));
  if (els.liveTargets) els.liveTargets.textContent = `${targetsDone}/${targetsTotal}`;
  if (els.livePage) els.livePage.textContent = String(Number(p.current_page || 0));
  if (els.liveSkipped) els.liveSkipped.textContent = String(Number(p.skipped_targets_total || 0));
  if (els.liveUpdated) els.liveUpdated.textContent = p.updated_at || "--";
  if (els.liveTargetLabel) els.liveTargetLabel.textContent = `Target: ${p.current_target_label || "--"}`;
}

function closeProgressStream() {
  if (progressStream) {
    progressStream.close();
    progressStream = null;
  }
}

function openProgressStream() {
  if (progressStream) return;
  progressStream = new EventSource("/api/progress/stream");
  progressStream.addEventListener("progress", (ev) => {
    try {
      const data = JSON.parse(ev.data || "{}");
      renderLiveProgress(data.progress || {});
    } catch {
      // Ignore malformed payloads.
    }
  });
  progressStream.addEventListener("terminal", () => {
    closeProgressStream();
  });
  progressStream.onerror = () => {
    closeProgressStream();
  };
}

async function api(path, options) {
  const res = await fetch(path, options);
  return res.json();
}

async function loadConfig() {
  const j = await api("/api/config");
  state.outDir = String(j.out_dir || "").trim();
  state.sessionPoolDir = String(j.session_pool_dir || "").trim();
  state.sessionReuseEnabled = !(j.session_reuse_enabled === false);
  state.headlessMode = !!j.headless_mode;
  state.openUrlsInBrowser = !(j.open_urls_in_browser === false);
  state.browserVerificationOnly = !(j.browser_verification_only === false);
  if (els.outDir) els.outDir.value = state.outDir;
  if (els.openUrlsInBrowser) els.openUrlsInBrowser.checked = !!state.openUrlsInBrowser;
  if (els.browserVerificationOnly) els.browserVerificationOnly.checked = !!state.browserVerificationOnly;
  if (els.includeArchivedUrls) els.includeArchivedUrls.checked = false;
  const savedUrls = Array.isArray(j.manual_urls) ? j.manual_urls : [];
  if (els.manualUrls && !els.manualUrls.value.trim()) {
    els.manualUrls.value = savedUrls.join("\n");
  }
  renderSavedUrlsMeta(savedUrls.length || countValidUrls(els.manualUrls ? els.manualUrls.value : ""), 0);
  renderSessionModeButton();
}

async function loadSavedUrls() {
  const outDir = encodeURIComponent(getOutDir());
  const res = await api(`/api/manual/urls?out_dir=${outDir}`);
  const urls = Array.isArray(res.urls) ? res.urls : [];
  if (els.manualUrls && !els.manualUrls.value.trim()) {
    els.manualUrls.value = urls.join("\n");
  }
  renderSavedUrlsMeta(Number(res.active_count || urls.length), Number(res.archived_count || 0));
}

async function saveUrls() {
  const payload = {
    out_dir: getOutDir(),
    manual_urls_text: els.manualUrls ? els.manualUrls.value : "",
  };
  const res = await api("/api/manual/urls", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    alert(res.error || "Failed to save URLs");
    return;
  }
  renderSavedUrlsMeta(Number(res.active_count || res.count || 0), Number(res.archived_count || 0));
}

function normalizeCarsalesUrl(raw) {
  let s = String(raw || "").trim();
  if (!s) return "";
  if (!s.toLowerCase().includes("carsales.com.au/cars/details/")) return "";
  if (!/^https?:\/\//i.test(s)) s = `https://${s.replace(/^\/+/, "")}`;
  s = s.split("?", 1)[0].trim();
  if (!s.endsWith("/")) s += "/";
  return s;
}

async function quickAddUrl() {
  if (!els.quickAddInput) return;
  const normalized = normalizeCarsalesUrl(els.quickAddInput.value);
  if (!normalized) {
    alert("Please enter a valid Carsales detail URL.");
    return;
  }
  const outDir = encodeURIComponent(getOutDir());
  const current = await api(`/api/manual/urls?out_dir=${outDir}`);
  const urls = Array.isArray(current.urls) ? current.urls.map((u) => String(u || "").trim()).filter(Boolean) : [];
  const key = normalized.toLowerCase();
  if (!urls.some((u) => u.toLowerCase() === key)) urls.push(normalized);
  const payload = {
    out_dir: getOutDir(),
    manual_urls_text: urls.join("\n"),
  };
  const res = await api("/api/manual/urls", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    alert(res.error || "Failed to save URL");
    return;
  }
  if (els.manualUrls) els.manualUrls.value = urls.join("\n");
  renderSavedUrlsMeta(Number(res.active_count || res.count || urls.length), Number(res.archived_count || 0));
  els.quickAddInput.value = "";
  if (els.quickAddModal) {
    els.quickAddModal.classList.remove("show");
    els.quickAddModal.setAttribute("aria-hidden", "true");
  }
}

async function loadStatus() {
  const outDir = encodeURIComponent(getOutDir() || "");
  const s = await api(`/api/status?out_dir=${outDir}`);
  state.status = s;
  els.runBtn.disabled = !!s.running;
  els.stopBtn.disabled = !s.running;

  if (s.running) {
    els.runStatus.textContent = `Running... started ${fmtDate(s.started_at)}`;
    setStatusBadge("Running");
    openProgressStream();
  } else if (s.return_code === null) {
    els.runStatus.textContent = "Idle (no job started yet)";
    setStatusBadge("Idle");
    closeProgressStream();
  } else if (s.return_code === 0) {
    els.runStatus.textContent = `Completed successfully at ${fmtDate(s.finished_at)}`;
    setStatusBadge("Completed");
    closeProgressStream();
  } else {
    const err = String(s.last_error || "").toLowerCase();
    if (err.includes("stopped by user") || err.includes("run_stopped")) {
      els.runStatus.textContent = "Stopped by user";
      setStatusBadge("Stopped");
    } else {
      const primaryError = String(s.display_error || s.last_error || "").trim();
      const errorHint = String(s.error_hint || "").trim();
      const composed = errorHint ? `${primaryError} ${errorHint}`.trim() : primaryError;
      els.runStatus.textContent = `Error (code ${s.return_code}) ${composed}`;
      setStatusBadge("Error");
    }
    closeProgressStream();
  }

  els.kpiLastRun.textContent = fmtDate(s.finished_at || s.started_at);
  renderLiveProgress(s.progress || {});
}

function rebuildFilters() {
  const makeSet = new Set();
  const modelSet = new Set();
  for (const r of state.rows) {
    const c = r.carbarn || {};
    const s = r.carsales || {};
    if (c.make) makeSet.add(c.make);
    if (s.make) makeSet.add(s.make);
    if (c.model) modelSet.add(c.model);
    if (s.model) modelSet.add(s.model);
  }
  const makeVal = els.makeFilter.value;
  const modelVal = els.modelFilter.value;
  els.makeFilter.innerHTML = `<option value="all">All Makes</option>` +
    [...makeSet].sort().map((m) => `<option value="${esc(m)}">${esc(m)}</option>`).join("");
  els.modelFilter.innerHTML = `<option value="all">All Models</option>` +
    [...modelSet].sort().map((m) => `<option value="${esc(m)}">${esc(m)}</option>`).join("");
  if ([...makeSet].includes(makeVal)) els.makeFilter.value = makeVal;
  if ([...modelSet].includes(modelVal)) els.modelFilter.value = modelVal;
}

async function loadComparisons(refreshInventory = false) {
  const outDir = encodeURIComponent(getOutDir());
  const q = refreshInventory ? "&refresh_inventory=1" : "";
  const res = await api(`/api/comparisons?out_dir=${outDir}${q}`);
  state.rows = res.rows || [];
  state.summary = res.summary || {};

  const identifiedTotal = Number(state.summary.identified_total || 0);
  const notFoundTotal = Number(state.summary.not_found_total || 0);
  const errorTotal = Number(state.summary.excluded_other_total || 0);
  const onOfferTotal = Number(state.summary.on_offer_total || 0);
  const unpublishedTotal = Number(state.summary.unpublished_total || 0);
  const invTotal = Number(state.summary.inventory_total || state.rows.length || 0);
  const submittedTotal = Number(state.summary.submitted_urls_total || 0);

  els.identifiedBadge.textContent = `Identified: ${identifiedTotal}`;
  els.notFoundBadge.textContent = `Not Found: ${notFoundTotal}`;
  if (els.totalCarsBadge) els.totalCarsBadge.textContent = `Total Cars: ${invTotal}`;
  if (els.tooManyBadge) els.tooManyBadge.textContent = `Errors: ${errorTotal}`;
  if (els.onOfferBadge) els.onOfferBadge.textContent = `Inventory Excluded: ${onOfferTotal}`;
  if (els.unpublishedBadge) els.unpublishedBadge.textContent = `Inventory Unpublished: ${unpublishedTotal}`;
  els.kpiIdentified.textContent = String(identifiedTotal);
  els.kpiNotFound.textContent = String(notFoundTotal);
  if (els.kpiOnOffer) els.kpiOnOffer.textContent = String(onOfferTotal);
  if (els.kpiUnpublished) els.kpiUnpublished.textContent = String(unpublishedTotal);
  els.kpiInventory.textContent = String(invTotal);
  if (els.kpiSubmittedUrls) els.kpiSubmittedUrls.textContent = String(submittedTotal);

  collectAvailableStockIds();
  renderSelectedStockIds();
  renderStockIdDropdown();

  rebuildFilters();
  renderRows();
}

function rowMatchesFilters(entry) {
  const c = entry.carbarn || {};
  const s = entry.carsales || {};
  const q = els.search.value.trim().toLowerCase();
  const statusF = els.statusFilter.value;
  const makeF = els.makeFilter.value;
  const modelF = els.modelFilter.value;
  const minP = toNum(els.minPrice.value);
  const maxP = toNum(els.maxPrice.value);

  if (statusF !== "all" && entry.status !== statusF) return false;
  if (makeF !== "all") {
    const mk = (c.make || s.make || "").toLowerCase();
    if (mk !== makeF.toLowerCase()) return false;
  }
  if (modelF !== "all") {
    const md = (c.model || s.model || "").toLowerCase();
    if (md !== modelF.toLowerCase()) return false;
  }

  const rp = toNum(c.price ?? c.price_text ?? s.price ?? s.price_text);
  if (minP !== null && (rp === null || rp < minP)) return false;
  if (maxP !== null && (rp === null || rp > maxP)) return false;

  if (q) {
    const blob = [
      c.title, c.make, c.model, c.year, c.price_text, c.odometer_text, c.stock_no, c.chassis,
      s.title, s.make, s.model, s.year, s.odometer_text, s.detail_url, s.description, s.stock_no, s.car_code,
    ].join(" ").toLowerCase();
    if (!blob.includes(q)) return false;
  }

  return true;
}

function infoChips(row) {
  const stockIds = Array.isArray(row.dealer_stock_ids)
    ? row.dealer_stock_ids.filter(Boolean)
    : [];
  const mediaCount = row.photo_count || row.image_count || "";
  const chips = [
    row.year ? `<span class="chip">Year: ${esc(row.year)}</span>` : "",
    row.make ? `<span class="chip">Make: ${esc(row.make)}</span>` : "",
    row.model ? `<span class="chip">Model: ${esc(row.model)}</span>` : "",
    (row.price_text || row.price) ? `<span class="chip">Price: ${esc(row.price_text || String(row.price))}</span>` : "",
    row.odometer_text ? `<span class="chip">${esc(row.odometer_text)}</span>` : "",
    row.stock_no ? `<span class="chip">Stock: ${esc(row.stock_no)}</span>` : "",
    row.chassis ? `<span class="chip">Chassis: ${esc(row.chassis)}</span>` : "",
    row.car_code ? `<span class="chip">Car Code: ${esc(row.car_code)}</span>` : "",
    mediaCount ? `<span class="chip">Images: ${esc(mediaCount)}</span>` : "",
    (row.photo_count && row.image_count && Number(row.image_count) !== Number(row.photo_count))
      ? `<span class="chip">Captured URLs: ${esc(row.image_count)}</span>`
      : "",
    stockIds.length > 1 ? `<span class="chip">Stock IDs: ${esc(stockIds.join(", "))}</span>` : "",
    row.vin ? `<span class="chip">VIN: ${esc(row.vin)}</span>` : "",
    row.registration_plate ? `<span class="chip">Plate: ${esc(row.registration_plate)}</span>` : "",
    row.body_type ? `<span class="chip">Body: ${esc(row.body_type)}</span>` : "",
    row.fuel ? `<span class="chip">Fuel: ${esc(row.fuel)}</span>` : "",
    row.transmission ? `<span class="chip">Trans: ${esc(row.transmission)}</span>` : "",
    row.match_score ? `<span class="chip">Score: ${esc(row.match_score)}</span>` : "",
    row.mismatch_count ? `<span class="chip chip-warn">Mismatches: ${esc(row.mismatch_count)}</span>` : "",
  ].filter(Boolean).join("");
  return chips || `<span class="chip">No extra info</span>`;
}

function normalizeImageList(row) {
  let images = [];
  if (Array.isArray(row.all_image_urls)) {
    images = row.all_image_urls;
  } else if (typeof row.all_image_urls === "string" && row.all_image_urls.trim()) {
    const raw = row.all_image_urls.trim();
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) images = parsed;
    } catch {
      images = raw.split("|");
    }
  }
  if (row.image_url) images = [row.image_url, ...images];
  const out = [];
  const seen = new Set();
  for (const u of images) {
    const clean = stripCropParam(String(u || "").trim());
    if (!clean) continue;
    const k = clean.toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(clean);
  }
  return out;
}

function cardHtml(side, row, status) {
  const images = normalizeImageList(row);
  const cleanImg = images[0] || "";
  const proxied = cleanImg ? `/api/image?url=${encodeURIComponent(cleanImg)}` : "";
  const img = cleanImg
    ? `<img class="compare-image zoomable" src="${proxied}" data-full="${proxied}" alt="${esc(row.title)}" loading="lazy" onerror="this.replaceWith(Object.assign(document.createElement('div'),{className:'compare-image placeholder',innerText:'No Image'}));" />`
    : `<div class="compare-image placeholder">No Image</div>`;
  const thumbs = images.slice(1, 9).map((u) => {
    const p = `/api/image?url=${encodeURIComponent(u)}`;
    return `<img class="thumb-image zoomable-media" src="${p}" data-full="${p}" alt="photo" loading="lazy" />`;
  }).join("");
  const gallery = thumbs ? `<div class="thumb-grid">${thumbs}</div>` : "";

  const statusLabelMap = {
    identified: "Identified",
    not_found: "Not Found",
    error: "Error",
    too_many_cards: "Too Many Cars",
    on_offer: "On Offer",
    unpublished: "Unpublished",
    excluded_other: "Excluded",
  };
  const statusTag = side === "carsales"
    ? `<span class="status-tag ${esc(status)}">${esc(statusLabelMap[status] || status)}</span>`
    : `<span class="status-tag base">Carbarn</span>`;
  const refreshBtn = side === "carsales" && row.source_url
    ? `<button class="mini-icon-btn refresh-one-btn" type="button" title="Refresh this car" data-source-url="${esc(row.source_url)}">↻</button>`
    : "";

  let warrantyBlock = "";
  if (side === "carbarn" && row.warranty) {
    const w = row.warranty;
    const yn = (v) => (v === true ? "YES" : v === false ? "NO" : "N/A");
    const cls = (v) => (v === true ? "yes" : v === false ? "no" : "na");
    warrantyBlock = `
      <div class="warranty-box">
        <div class="warranty-title">${esc(w.rule_label || "Warranty Rule")}</div>
        <div class="warranty-line ${cls(w.dealer_warranty)}">3-Month Dealer Warranty: ${yn(w.dealer_warranty)}</div>
        <div class="warranty-line ${cls(w.integrity_warranty)}">5-Years Integrity Warranty: ${yn(w.integrity_warranty)}</div>
      </div>
    `;
  }

  const linkLabel = side === "carsales" ? "Open Carsales listing" : "Open Carbarn listing";
  let mismatchBlock = "";
  if (side === "carsales") {
    const msgs = Array.isArray(row.mismatch_messages) ? row.mismatch_messages : [];
    if (msgs.length) {
      mismatchBlock = `
        <div class="mismatch-box">
          <div class="mismatch-title">Data Mismatches</div>
          ${msgs.map((m) => `<div class="mismatch-line">${esc(m)}</div>`).join("")}
        </div>
      `;
    }
  }
  let lookupBlock = "";
  if (side === "carsales" && status === "not_found") {
    const links = Array.isArray(row.lookup_urls) ? row.lookup_urls : [];
    if (links.length) {
      lookupBlock = `
        <div class="mismatch-box">
          <div class="mismatch-title">Carsales Quick Lookup</div>
          ${links.map((x) => `<div class="mismatch-line"><a class="link lookup-antibot-link" href="#" data-url="${esc(x.url || "")}">${esc(x.label || x.url || "")}</a></div>`).join("")}
        </div>
      `;
    }
  }

  return `
    <article class="compare-card">
      <div class="compare-head">
        <div class="compare-source">${side === "carbarn" ? "Carbarn Match" : "Carsales Input"}</div>
        <div class="card-head-actions">${refreshBtn}${statusTag}</div>
      </div>
      <div class="compare-media">${img}</div>
      ${gallery}
      <h3 class="car-title">${esc(row.title || (side === "carbarn" ? "Carbarn Vehicle" : "Carsales Vehicle"))}</h3>
      <div class="chips">${infoChips(row)}</div>
      ${mismatchBlock}
      ${lookupBlock}
      ${warrantyBlock}
      ${row.detail_url ? `<a class="link" href="${esc(row.detail_url)}" target="_blank" rel="noopener">${linkLabel}</a>` : ""}
    </article>
  `;
}

function renderRows() {
  const rows = state.rows.filter(rowMatchesFilters);
  if (!rows.length) {
    els.rows.innerHTML = `<div class="empty">No vehicles match current filters.</div>`;
    return;
  }

  els.rows.innerHTML = rows.map((entry) => `
    <section class="compare-row">
      ${cardHtml("carbarn", entry.carbarn || {}, entry.status)}
      ${cardHtml("carsales", entry.carsales || {}, entry.status)}
    </section>
  `).join("");

  initImageViewer();
}

function initImageViewer() {
  const viewer = document.getElementById("imageViewer");
  const viewerImg = document.getElementById("imageViewerImg");
  const closeBtn = document.getElementById("imageViewerClose");
  if (!viewer || !viewerImg || !closeBtn) return;

  document.querySelectorAll(".compare-image.zoomable, .zoomable-media").forEach((img) => {
    img.onclick = () => {
      const full = img.getAttribute("data-full") || img.getAttribute("src") || "";
      if (!full) return;
      viewerImg.src = full;
      viewer.classList.add("show");
      viewer.setAttribute("aria-hidden", "false");
    };
  });

  if (!imageViewerBound) {
    closeBtn.addEventListener("click", () => {
      viewer.classList.remove("show");
      viewer.setAttribute("aria-hidden", "true");
      viewerImg.src = "";
    });
    viewer.addEventListener("click", (e) => {
      if (e.target === viewer) {
        viewer.classList.remove("show");
        viewer.setAttribute("aria-hidden", "true");
        viewerImg.src = "";
      }
    });
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && viewer.classList.contains("show")) {
        viewer.classList.remove("show");
        viewer.setAttribute("aria-hidden", "true");
        viewerImg.src = "";
      }
    });
    imageViewerBound = true;
  }
}

async function runJob() {
  const runMode = (els.runModeNotFound && els.runModeNotFound.checked)
    ? "not_found"
    : (els.runModeStockOnly && els.runModeStockOnly.checked) ? "stock_ids" : "all";
  const stockOnly = !!(els.runModeStockOnly && els.runModeStockOnly.checked);
  const stockIdsText = stockOnly ? [...state.selectedStockIds].join("\n") : "";
  if (stockOnly && !stockIdsText) {
    alert("Select at least one Stock ID, or switch mode to 'All Saved URLs'.");
    return;
  }
  const payload = {
    out_dir: getOutDir(),
    session_pool_dir: getSessionPoolDir(),
    session_reuse_enabled: isSessionReuseEnabled(),
    save_storage_state_on_exit: isSessionReuseEnabled(),
    run_scope: runMode,
    refresh_inventory: stockIdsText ? true : false,
    open_urls_in_browser: !!state.openUrlsInBrowser,
    browser_verification_only: !!state.browserVerificationOnly,
    include_archived_urls: false,
    stock_ids_text: stockIdsText,
  };
  const res = await api("/api/run", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    alert(res.error || "Failed to start process");
    return;
  }
  openProgressStream();
  await loadStatus();
}

document.addEventListener("click", async (e) => {
  const el = e.target.closest(".lookup-antibot-link");
  if (!el) return;
  e.preventDefault();
  const url = String(el.getAttribute("data-url") || "").trim();
  if (!url) return;
  await openInAntibot(url);
});

document.addEventListener("click", async (e) => {
  const el = e.target.closest(".refresh-one-btn");
  if (!el) return;
  e.preventDefault();
  const sourceUrl = String(el.getAttribute("data-source-url") || "").trim();
  if (!sourceUrl) return;
  el.disabled = true;
  const oldText = el.textContent;
  el.textContent = "...";
  try {
    const res = await api("/api/manual/refresh-one", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        out_dir: getOutDir(),
        session_pool_dir: getSessionPoolDir(),
        session_reuse_enabled: isSessionReuseEnabled(),
        save_storage_state_on_exit: isSessionReuseEnabled(),
        source_url: sourceUrl,
        open_urls_in_browser: !!state.openUrlsInBrowser,
        browser_verification_only: !!state.browserVerificationOnly,
        refresh_inventory: true,
      }),
    });
    if (!res.ok) {
      alert(res.error || "Failed to refresh this car.");
      return;
    }
    await loadComparisons(true);
  } finally {
    el.disabled = false;
    el.textContent = oldText || "↻";
  }
});

async function stopJob() {
  els.stopBtn.disabled = true;
  els.runStatus.textContent = "Stopping... please wait";
  setStatusBadge("Stopping");
  await api("/api/stop", { method: "POST" });
  await loadStatus();
}

async function openOutput() {
  await api("/api/open-output", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ out_dir: getOutDir() }),
  });
}

async function toggleSessionMode() {
  const next = !isSessionReuseEnabled();
  if (els.sessionModeBtn) els.sessionModeBtn.disabled = true;
  try {
    const res = await api("/api/session/mode", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        out_dir: getOutDir(),
        session_pool_dir: getSessionPoolDir(),
        session_reuse_enabled: next,
        save_storage_state_on_exit: next,
        reset_browser: true,
      }),
    });
    if (!res.ok) {
      alert(res.error || "Failed to change session mode");
      return;
    }
    state.sessionReuseEnabled = !!res.session_reuse_enabled;
    if (res.session_pool_dir) state.sessionPoolDir = String(res.session_pool_dir);
    renderSessionModeButton();
    await loadStatus();
  } finally {
    if (els.sessionModeBtn) els.sessionModeBtn.disabled = false;
  }
}

function bindEvents() {
  els.runBtn.addEventListener("click", runJob);
  els.stopBtn.addEventListener("click", stopJob);
  if (els.sessionModeBtn) els.sessionModeBtn.addEventListener("click", toggleSessionMode);
  els.refreshBtn.addEventListener("click", async () => {
    await loadStatus();
    await loadComparisons(false);
  });
  if (els.openBtn) els.openBtn.addEventListener("click", openOutput);
  if (els.manualUrls) {
    els.manualUrls.addEventListener("input", () => {
      renderSavedUrlsMeta(countValidUrls(els.manualUrls.value), 0);
    });
  }
  if (els.clearStockIdsBtn) {
    els.clearStockIdsBtn.addEventListener("click", () => {
      state.selectedStockIds.clear();
      if (els.stockIdInput) els.stockIdInput.value = "";
      renderSelectedStockIds();
      renderStockIdDropdown();
    });
  }
  if (els.stockIdInput) {
    els.stockIdInput.addEventListener("input", renderStockIdDropdown);
    els.stockIdInput.addEventListener("focus", renderStockIdDropdown);
    els.stockIdInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        const raw = String(els.stockIdInput.value || "").trim();
        if (raw) addSelectedStockId(raw);
        els.stockIdInput.value = "";
        renderStockIdDropdown();
      }
      if (e.key === "Backspace" && !String(els.stockIdInput.value || "").trim()) {
        const arr = [...state.selectedStockIds];
        if (arr.length) {
          state.selectedStockIds.delete(arr[arr.length - 1]);
          renderSelectedStockIds();
          renderStockIdDropdown();
        }
      }
    });
  }
  if (els.stockIdDropdown) {
    els.stockIdDropdown.addEventListener("click", (e) => {
      const btn = e.target.closest(".stock-option");
      if (!btn) return;
      const id = btn.getAttribute("data-stock-id") || "";
      addSelectedStockId(id);
      if (els.stockIdInput) {
        els.stockIdInput.value = "";
        els.stockIdInput.focus();
      }
      renderStockIdDropdown();
    });
  }
  if (els.stockIdSelected) {
    els.stockIdSelected.addEventListener("click", (e) => {
      const chip = e.target.closest(".stock-chip");
      if (!chip) return;
      const id = chip.getAttribute("data-stock-id") || "";
      if (!id) return;
      state.selectedStockIds.delete(id);
      renderSelectedStockIds();
      renderStockIdDropdown();
    });
  }
  document.addEventListener("click", (e) => {
    if (
      els.stockIdDropdown
      && els.stockIdInput
      && !els.stockIdDropdown.contains(e.target)
      && e.target !== els.stockIdInput
    ) {
      els.stockIdDropdown.classList.remove("show");
    }
  });
  els.reloadDataBtn.addEventListener("click", () => loadComparisons(false));
  if (els.refreshInventoryBtn) {
    els.refreshInventoryBtn.addEventListener("click", () => loadComparisons(true));
  }
  if (els.quickAddBtn && els.quickAddModal) {
    els.quickAddBtn.addEventListener("click", () => {
      els.quickAddModal.classList.add("show");
      els.quickAddModal.setAttribute("aria-hidden", "false");
      if (els.quickAddInput) {
        els.quickAddInput.value = "";
        els.quickAddInput.focus();
      }
    });
  }
  if (els.quickAddClose && els.quickAddModal) {
    els.quickAddClose.addEventListener("click", () => {
      els.quickAddModal.classList.remove("show");
      els.quickAddModal.setAttribute("aria-hidden", "true");
    });
  }
  if (els.quickAddModal) {
    els.quickAddModal.addEventListener("click", (e) => {
      if (e.target === els.quickAddModal) {
        els.quickAddModal.classList.remove("show");
        els.quickAddModal.setAttribute("aria-hidden", "true");
      }
    });
  }
  if (els.quickAddSave) {
    els.quickAddSave.addEventListener("click", quickAddUrl);
  }
  if (els.quickAddInput) {
    els.quickAddInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        quickAddUrl();
      }
    });
  }

  for (const k of ["search", "statusFilter", "makeFilter", "modelFilter", "minPrice", "maxPrice"]) {
    const node = els[k];
    node.addEventListener("input", renderRows);
    node.addEventListener("change", renderRows);
  }
}

async function boot() {
  await loadConfig();
  await loadSavedUrls();
  await loadStatus();
  await loadComparisons(false);
  bindEvents();

  setInterval(async () => {
    await loadStatus();
  }, 5000);
}

boot();

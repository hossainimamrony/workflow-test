const els = {
  rows: document.getElementById("rows"),
  loadDefaultBtn: document.getElementById("loadDefaultBtn"),
  searchInput: document.getElementById("searchInput"),
  statusFilter: document.getElementById("statusFilter"),
  makeFilter: document.getElementById("makeFilter"),
  modelFilter: document.getElementById("modelFilter"),
  minPrice: document.getElementById("minPrice"),
  maxPrice: document.getElementById("maxPrice"),
};

const state = {
  rows: [],
  updatedAt: null,
  source: "",
};

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[c]));
}

function normalizeRows(payload) {
  if (!payload || !Array.isArray(payload.rows)) return [];
  return payload.rows;
}

function chipsFor(row) {
  return [
    row.year ? `Year: ${row.year}` : "",
    row.make ? `Make: ${row.make}` : "",
    row.model ? `Model: ${row.model}` : "",
    row.price_text ? `Price: ${row.price_text}` : "",
    row.odometer_text ? `${row.odometer_text}` : "",
    row.stock_no ? `Stock: ${row.stock_no}` : "",
    row.chassis ? `Chassis: ${row.chassis}` : "",
    row.car_code ? `Car Code: ${row.car_code}` : "",
    row.photo_count ? `Images: ${row.photo_count}` : "",
    row.image_count ? `Captured URLs: ${row.image_count}` : "",
    row.vin ? `VIN: ${row.vin}` : "",
    row.registration_plate ? `Plate: ${row.registration_plate}` : "",
    row.body_type ? `Body: ${row.body_type}` : "",
    row.fuel ? `Fuel: ${row.fuel}` : "",
    row.transmission ? `Trans: ${row.transmission}` : "",
    row.match_score ? `Score: ${row.match_score}` : "",
  ].filter(Boolean);
}

function getImageCount(row) {
  const raw = row?.photo_count ?? row?.image_count;
  const numeric = Number(raw);
  if (Number.isFinite(numeric) && numeric > 0) return numeric;
  return normalizeImageList(row || {}).length;
}

function getImageWarning(row) {
  const count = getImageCount(row);
  if (!count) return null;
  if (count < 20) {
    return {
      count,
      message: "There might be less images.",
    };
  }
  if (count > 35) {
    return {
      count,
      message: "There might be duplicate images.",
    };
  }
  return null;
}

function normalizeImageList(row) {
  let images = [];
  if (Array.isArray(row?.all_image_urls)) {
    images = row.all_image_urls;
  }
  if (row?.image_url) {
    images = [row.image_url, ...images];
  }
  const out = [];
  const seen = new Set();
  for (const url of images) {
    const clean = String(url || "").trim();
    if (!clean) continue;
    const key = clean.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(clean);
  }
  return out;
}

function cleanText(v) {
  return String(v ?? "").trim();
}

function normText(v) {
  return cleanText(v).toLowerCase().replace(/\s+/g, " ");
}

function normDigits(v) {
  return cleanText(v).replace(/[^\d]/g, "");
}

function toInt(v) {
  const s = normDigits(v);
  return s ? Number(s) : null;
}

function buildDerivedMismatchMessages(entry) {
  const c = entry?.carbarn || {};
  const s = entry?.carsales || {};
  const out = [];

  const csPrice = toInt(s.price ?? s.price_text);
  const cbPrice = toInt(c.price ?? c.price_text);
  if (csPrice !== null && cbPrice !== null && csPrice !== cbPrice) {
    out.push(`Price mismatch: Carsales $${csPrice.toLocaleString()} vs Carbarn $${cbPrice.toLocaleString()}`);
  }

  const csOdo = toInt(s.odometer_km ?? s.odometer_text);
  const cbOdo = toInt(c.odometer_text);
  if (csOdo !== null && cbOdo !== null && csOdo !== cbOdo) {
    out.push(`Odometer mismatch: Carsales ${csOdo.toLocaleString()}km vs Carbarn ${cbOdo.toLocaleString()}km`);
  }

  const csYear = normText(s.year);
  const cbYear = normText(c.year);
  if (csYear && cbYear && csYear !== cbYear) {
    out.push(`Year mismatch: Carsales ${cleanText(s.year)} vs Carbarn ${cleanText(c.year)}`);
  }

  const csMake = normText(s.make);
  const cbMake = normText(c.make);
  if (csMake && cbMake && csMake !== cbMake) {
    out.push(`Make mismatch: Carsales ${cleanText(s.make)} vs Carbarn ${cleanText(c.make)}`);
  }

  return out;
}

function buildAllMismatchMessages(entry) {
  const src = Array.isArray(entry?.carsales?.mismatch_messages) ? entry.carsales.mismatch_messages : [];
  const derived = buildDerivedMismatchMessages(entry);
  const seen = new Set();
  const merged = [];
  for (const msg of [...src, ...derived]) {
    const m = cleanText(msg);
    if (!m) continue;
    const k = m.toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    merged.push(m);
  }
  return merged;
}

function cardHtml(sideLabel, row, status, entry) {
  const title = row?.title || "No title";
  const images = normalizeImageList(row || {});
  const firstImage = images[0] || "";
  const priceBadge = sideLabel === "Carsales" && cleanText(row?.price_badge_label)
    ? `<div class="image-price-badge">${esc(row.price_badge_label)}</div>`
    : "";
  const img = firstImage
    ? `<div class="card-image-wrap"><img src="${esc(firstImage)}" alt="${esc(title)}" loading="lazy" />${priceBadge}</div>`
    : "";
  const thumbs = images.slice(1, 9).map((url) => (
    `<img class="thumb" src="${esc(url)}" alt="photo" loading="lazy" />`
  )).join("");
  const mismatchMessages = sideLabel === "Carsales" ? buildAllMismatchMessages(entry) : [];
  const imageWarning = sideLabel === "Carsales" ? getImageWarning(row || {}) : null;
  const mismatchChip = sideLabel === "Carsales" && mismatchMessages.length
    ? `<span class="chip">Mismatches: ${mismatchMessages.length}</span>`
    : "";
  const chips = `${chipsFor(row || {}).map((x) => {
    if (x.startsWith("Images:") && imageWarning) {
      return `<span class="chip chip-warn">${esc(x)}</span>`;
    }
    return `<span class="chip">${esc(x)}</span>`;
  }).join("")}${mismatchChip}`;
  const link = row?.detail_url ? `<a href="${esc(row.detail_url)}" target="_blank" rel="noopener">Open listing</a>` : "";
  const mismatchBlock = mismatchMessages.length
    ? `<div class="detail-box warn"><div class="detail-title">Data Mismatches</div>${mismatchMessages.map((m) => `<div>${esc(m)}</div>`).join("")}</div>`
    : "";
  const imageWarningBlock = imageWarning
    ? `<div class="detail-box caution"><div class="detail-title">Image Count Warning (${imageWarning.count})</div><div>${esc(imageWarning.message)}</div></div>`
    : "";
  const warranty = row?.warranty || null;
  const warrantyBlock = warranty
    ? `<div class="detail-box ok">
        <div class="detail-title">${esc(warranty.rule_label || "Warranty Rule")}</div>
        <div>3-Month Dealer Warranty: ${warranty.dealer_warranty ? "YES" : "NO"}</div>
        <div>5-Years Integrity Warranty: ${warranty.integrity_warranty ? "YES" : "NO"}</div>
      </div>`
    : "";

  return `
    <article class="card">
      <span class="status">${esc(sideLabel)} | ${esc(status || "unknown")}</span>
      <h3>${esc(title)}</h3>
      ${img}
      ${thumbs ? `<div class="thumb-grid">${thumbs}</div>` : ""}
      <div class="chips">${chips}</div>
      ${imageWarningBlock}
      ${mismatchBlock}
      ${warrantyBlock}
      ${link}
    </article>
  `;
}

function rowMatches(entry, query) {
  const status = els.statusFilter.value;
  const make = els.makeFilter.value;
  const model = els.modelFilter.value;
  const minPrice = Number(els.minPrice.value || 0) || null;
  const maxPrice = Number(els.maxPrice.value || 0) || null;

  const entryStatus = String(entry?.status || "");
  const soldFlag = !!entry?.carsales?.is_sold;
  if (status === "mismatch_only") {
    if (!buildAllMismatchMessages(entry).length) return false;
  } else if (status === "sold") {
    if (!(entryStatus === "sold" || soldFlag)) return false;
  } else if (status !== "all" && entryStatus !== status) {
    return false;
  }

  const mk = String(entry?.carbarn?.make || entry?.carsales?.make || "");
  if (make !== "all" && mk.toLowerCase() !== make.toLowerCase()) return false;

  const md = String(entry?.carbarn?.model || entry?.carsales?.model || "");
  if (model !== "all" && md.toLowerCase() !== model.toLowerCase()) return false;

  const priceRaw = entry?.carbarn?.price ?? entry?.carsales?.price;
  const price = Number(priceRaw || 0) || null;
  if (minPrice !== null && (price === null || price < minPrice)) return false;
  if (maxPrice !== null && (price === null || price > maxPrice)) return false;

  const blob = [
    entry?.status,
    entry?.carbarn?.title,
    entry?.carbarn?.make,
    entry?.carbarn?.model,
    entry?.carbarn?.stock_no,
    entry?.carsales?.title,
    entry?.carsales?.make,
    entry?.carsales?.model,
    entry?.carsales?.stock_no,
  ].join(" ").toLowerCase();
  return blob.includes(query);
}

function rebuildFilters() {
  const makeSet = new Set();
  const modelSet = new Set();
  for (const entry of state.rows) {
    const m1 = String(entry?.carbarn?.make || "").trim();
    const m2 = String(entry?.carsales?.make || "").trim();
    const d1 = String(entry?.carbarn?.model || "").trim();
    const d2 = String(entry?.carsales?.model || "").trim();
    if (m1) makeSet.add(m1);
    if (m2) makeSet.add(m2);
    if (d1) modelSet.add(d1);
    if (d2) modelSet.add(d2);
  }

  const currentMake = els.makeFilter.value;
  const currentModel = els.modelFilter.value;
  els.makeFilter.innerHTML = `<option value="all">All Makes</option>${[...makeSet].sort().map((v) => `<option value="${esc(v)}">${esc(v)}</option>`).join("")}`;
  els.modelFilter.innerHTML = `<option value="all">All Models</option>${[...modelSet].sort().map((v) => `<option value="${esc(v)}">${esc(v)}</option>`).join("")}`;
  if ([...makeSet].includes(currentMake)) els.makeFilter.value = currentMake;
  if ([...modelSet].includes(currentModel)) els.modelFilter.value = currentModel;
}

function render() {
  const query = (els.searchInput.value || "").trim().toLowerCase();
  let rows = state.rows.filter((entry) => rowMatches(entry, query));
  if (els.statusFilter.value === "all") {
    rows = rows.sort((a, b) => {
      const aMismatch = buildAllMismatchMessages(a).length > 0 ? 1 : 0;
      const bMismatch = buildAllMismatchMessages(b).length > 0 ? 1 : 0;
      return aMismatch - bMismatch;
    });
  }

  if (!rows.length) {
    els.rows.innerHTML = '<div class="empty">No comparison rows found.</div>';
  } else {
    els.rows.innerHTML = rows.map((entry) => `
      <section class="row">
        ${cardHtml("Carbarn", entry.carbarn || {}, entry.status, entry)}
        ${cardHtml("Carsales", entry.carsales || {}, entry.status, entry)}
      </section>
    `).join("");
  }

}

function setData(payload, sourceLabel) {
  state.rows = normalizeRows(payload);
  state.updatedAt = payload?.updated_at || null;
  state.source = sourceLabel;
  rebuildFilters();
  render();
}

async function loadDefaultJson() {
  const res = await fetch("/api/comparisons");
  const data = await res.json();
  if (!data.ok) throw new Error(data.error || "Failed to load JSON");
  setData(data, "filesystem: apps/carsale_scraper/full_comparisons.json");
}

function bindEvents() {
  els.loadDefaultBtn.addEventListener("click", async () => {
    try {
      await loadDefaultJson();
    } catch (err) {
      console.error(err);
    }
  });

  els.searchInput.addEventListener("input", render);
  els.statusFilter.addEventListener("change", render);
  els.makeFilter.addEventListener("change", render);
  els.modelFilter.addEventListener("change", render);
  els.minPrice.addEventListener("input", render);
  els.maxPrice.addEventListener("input", render);
}

(async function boot() {
  bindEvents();
  try {
    await loadDefaultJson();
  } catch (err) {
    console.error(err);
    render();
  }
})();

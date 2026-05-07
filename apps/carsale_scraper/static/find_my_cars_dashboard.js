const els = {
  rows: document.getElementById("rows"),
  metaText: document.getElementById("metaText"),
  loadDefaultBtn: document.getElementById("loadDefaultBtn"),
  jsonUpload: document.getElementById("jsonUpload"),
  searchInput: document.getElementById("searchInput"),
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
    row.stock_no ? `Stock: ${row.stock_no}` : "",
    row.chassis ? `Chassis: ${row.chassis}` : "",
  ].filter(Boolean);
}

function cardHtml(sideLabel, row, status) {
  const title = row?.title || "No title";
  const img = row?.image_url ? `<img src="${esc(row.image_url)}" alt="${esc(title)}" loading="lazy" />` : "";
  const chips = chipsFor(row || {}).map((x) => `<span class="chip">${esc(x)}</span>`).join("");
  const link = row?.detail_url ? `<a href="${esc(row.detail_url)}" target="_blank" rel="noopener">Open listing</a>` : "";

  return `
    <article class="card">
      <span class="status">${esc(sideLabel)} | ${esc(status || "unknown")}</span>
      <h3>${esc(title)}</h3>
      ${img}
      <div class="chips">${chips}</div>
      ${link}
    </article>
  `;
}

function rowMatches(entry, query) {
  if (!query) return true;
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

function render() {
  const query = (els.searchInput.value || "").trim().toLowerCase();
  const rows = state.rows.filter((entry) => rowMatches(entry, query));

  if (!rows.length) {
    els.rows.innerHTML = '<div class="empty">No comparison rows found.</div>';
  } else {
    els.rows.innerHTML = rows.map((entry) => `
      <section class="row">
        ${cardHtml("Carbarn", entry.carbarn || {}, entry.status)}
        ${cardHtml("Carsales", entry.carsales || {}, entry.status)}
      </section>
    `).join("");
  }

  els.metaText.textContent = `${rows.length}/${state.rows.length} rows | Updated: ${state.updatedAt || "unknown"} | Source: ${state.source || "-"}`;
}

function setData(payload, sourceLabel) {
  state.rows = normalizeRows(payload);
  state.updatedAt = payload?.updated_at || null;
  state.source = sourceLabel;
  render();
}

async function loadDefaultJson() {
  const res = await fetch("/api/comparisons");
  const data = await res.json();
  if (!data.ok) throw new Error(data.error || "Failed to load JSON");
  setData(data, "server full_comparisons.json");
}

async function handleUpload(file) {
  const text = await file.text();
  const parsed = JSON.parse(text);
  if (!Array.isArray(parsed?.rows)) {
    throw new Error("Invalid JSON format. Expected { rows: [] }.");
  }
  setData(parsed, `uploaded: ${file.name}`);
}

function bindEvents() {
  els.loadDefaultBtn.addEventListener("click", async () => {
    try {
      await loadDefaultJson();
    } catch (err) {
      els.metaText.textContent = `Error: ${err.message}`;
    }
  });

  els.jsonUpload.addEventListener("change", async (event) => {
    const file = event.target.files?.[0];
    if (!file) return;
    try {
      await handleUpload(file);
    } catch (err) {
      els.metaText.textContent = `Upload error: ${err.message}`;
    }
  });

  els.searchInput.addEventListener("input", render);
}

(async function boot() {
  bindEvents();
  try {
    await loadDefaultJson();
  } catch (err) {
    els.metaText.textContent = `Startup error: ${err.message}`;
    render();
  }
})();

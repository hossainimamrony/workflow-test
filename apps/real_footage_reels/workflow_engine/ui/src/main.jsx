import React, { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";

import "./styles.css";

const commandOptions = [
  {
    value: "run",
    label: "Full Reel",
    eyebrow: "All",
    description: "Download, classify, render.",
  },
  {
    value: "prepare",
    label: "Prepare",
    eyebrow: "Frames",
    description: "Download and extract frames.",
  },
  {
    value: "download",
    label: "Download",
    eyebrow: "Raw",
    description: "Footage only.",
  },
];

const navigationItems = [
  { key: "/workflow", label: "Studio" },
  { key: "/runs", label: "Runs" },
];

const dateFormatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: "medium",
  timeStyle: "short",
});

const timeFormatter = new Intl.DateTimeFormat(undefined, {
  timeStyle: "short",
});

const WORKFLOW_DRAFT_STORAGE_KEY = "workflow-form-draft-v1";
const THUMBNAIL_DRAFT_STORAGE_KEY_PREFIX = "thumbnail-generator-draft-v1";
const VEHICLE_INVENTORY_API_URL = "/api/vehicle-inventory";
const VEHICLE_SEARCH_DEBOUNCE_MS = 250;
const VEHICLE_SEARCH_RESULT_LIMIT = 20;
const compactNumberFormatter = new Intl.NumberFormat("en-AU", {
  maximumFractionDigits: 0,
});

function defaultWorkflowFormValues(classificationEnabled) {
  return {
    url: "",
    listingTitle: "",
    stockId: "",
    carDescription: "",
    listingPrice: "",
    priceIncludes: "",
    command: classificationEnabled ? "run" : "prepare",
    maxClips: "",
    compose: false,
    headful: false,
    voiceoverScriptApproval: true,
  };
}

function loadWorkflowFormDraft(classificationEnabled) {
  const defaults = defaultWorkflowFormValues(classificationEnabled);
  if (typeof window === "undefined") {
    return defaults;
  }
  try {
    const raw = window.localStorage.getItem(WORKFLOW_DRAFT_STORAGE_KEY);
    if (!raw) {
      return defaults;
    }
    const parsed = JSON.parse(raw);
    return {
      ...defaults,
      ...parsed,
      maxClips: parsed?.maxClips === undefined || parsed?.maxClips === null ? "" : String(parsed.maxClips),
      compose: Boolean(parsed?.compose),
      headful: Boolean(parsed?.headful),
      voiceoverScriptApproval: parsed?.voiceoverScriptApproval !== false,
    };
  } catch {
    return defaults;
  }
}

function RootApp() {
  return <AppShell />;
}

function AppShell() {
  const [route, setRoute] = useState(() => parseRoute(window.location.pathname));
  const [meta, setMeta] = useState(null);
  const [jobs, setJobs] = useState([]);
  const [activeJobId, setActiveJobId] = useState(null);
  const [runs, setRuns] = useState([]);
  const [booting, setBooting] = useState(true);
  const [loadingRun, setLoadingRun] = useState(false);
  const [selectedRun, setSelectedRun] = useState(null);
  const [selectedRunError, setSelectedRunError] = useState("");
  const [notice, setNotice] = useState(null);

  const jobsByNewest = useMemo(
    () =>
      [...jobs].sort((left, right) =>
        String(right.createdAt ?? "").localeCompare(String(left.createdAt ?? "")),
      ),
    [jobs],
  );
  const activeJob = useMemo(
    () => jobs.find((job) => job.id === activeJobId) ?? null,
    [jobs, activeJobId],
  );
  const latestJob = jobsByNewest[0] ?? null;
  const classificationEnabled = Boolean(meta?.features?.analysisEnabled);

  useEffect(() => {
    const syncRoute = () => {
      setRoute(parseRoute(window.location.pathname));
    };

    window.addEventListener("popstate", syncRoute);
    return () => window.removeEventListener("popstate", syncRoute);
  }, []);

  useEffect(() => {
    void loadShellData({ initial: true });
  }, []);

  useEffect(() => {
    const timer = window.setInterval(() => {
      void loadShellData({ silent: true });
    }, 2500);

    return () => window.clearInterval(timer);
  }, [route.page, route.runId]);

  useEffect(() => {
    if (!notice) {
      return undefined;
    }

    const timer = window.setTimeout(() => {
      setNotice(null);
    }, 4200);

    return () => window.clearTimeout(timer);
  }, [notice]);

  useEffect(() => {
    if (route.page === "run-detail" && route.runId) {
      void loadRunDetail(route.runId);
      return;
    }

    setSelectedRun(null);
    setSelectedRunError("");
  }, [route.page, route.runId]);

  async function loadShellData(options = {}) {
    if (options.initial) {
      setBooting(true);
    }

    try {
      const [metaResponse, jobsResponse, runsResponse] = await Promise.all([
        fetchJson("/api/meta"),
        fetchJson("/api/jobs"),
        fetchJson("/api/runs"),
      ]);

      setMeta(metaResponse);
      setJobs(jobsResponse.jobs);
      setActiveJobId(jobsResponse.activeJobId);
      setRuns(runsResponse.runs);

      if (route.page === "run-detail" && route.runId) {
        await loadRunDetail(route.runId, { silent: true });
      }
    } catch (error) {
      if (!options.silent) {
        showNotice("error", error.message);
      }
    } finally {
      if (options.initial) {
        setBooting(false);
      }
    }
  }

  async function loadRunDetail(runId, options = {}) {
    if (!runId) {
      return;
    }

    if (!options.silent) {
      setLoadingRun(true);
    }

    try {
      const detail = await fetchJson(`/api/runs/${encodeURIComponent(runId)}`);
      setSelectedRun(stampRunMediaUrls(detail));
      setSelectedRunError("");
    } catch (error) {
      setSelectedRun(null);
      setSelectedRunError(error.message);
      if (!options.silent) {
        showNotice("error", error.message);
      }
    } finally {
      setLoadingRun(false);
    }
  }

  function showNotice(tone, text) {
    setNotice({ tone, text });
  }

  function navigate(pathname) {
    if (!pathname) {
      return;
    }

    window.history.pushState({}, "", pathname);
    setRoute(parseRoute(pathname));
  }

  async function handleStartWorkflow(values) {
    const shouldRequireScriptApproval =
      typeof values.voiceoverScriptApproval === "boolean"
        ? values.voiceoverScriptApproval
        : values.command === "run" && Boolean(values.compose) && Boolean(meta?.features?.voiceoverEnabled);
    const payload = {
      url: values.url,
      listingTitle: values.listingTitle,
      stockId: values.stockId,
      carDescription: values.carDescription,
      listingPrice: values.listingPrice,
      priceIncludes: values.priceIncludes,
      command: values.command,
      maxClips: values.maxClips ?? null,
      compose: values.command === "run" ? Boolean(values.compose) : false,
      headful: Boolean(values.headful),
      voiceoverScriptApproval: shouldRequireScriptApproval,
    };

    const job = await fetchJson("/api/jobs", {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    showNotice("success", "Started.");
    setJobs((current) => [job, ...current]);
    setActiveJobId(job.id);
    await loadShellData({ silent: true });
    navigate("/workflow");
  }

  async function handleDeleteRun(runId) {
    if (!runId || !window.confirm("Delete this run?")) {
      return;
    }
    await fetchJson(`/api/runs/${encodeURIComponent(runId)}`, { method: "DELETE" });
    showNotice("success", "Deleted.");
    await loadShellData({ silent: true });
    if (route.page === "run-detail" && route.runId === runId) {
      navigate("/workflow");
    }
  }

  async function handlePrepareFrames(runId) {
    const job = await fetchJson(`/api/runs/${encodeURIComponent(runId)}/prepare`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ headful: false }),
    });
    showNotice("success", "Started.");
    setJobs((current) => [job, ...current]);
    setActiveJobId(job.id);
    await loadShellData({ silent: true });
    navigate("/workflow");
  }

  async function handleRetryFailedJob(job) {
    const step = inferFailedStep(job);
    if (step === "download" && job.sourcePayload) {
      const s = job.sourcePayload;
      const newJob = await fetchJson("/api/jobs", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          url: s.url,
          listingTitle: s.listingTitle,
          stockId: s.stockId,
          carDescription: s.carDescription ?? "",
          listingPrice: s.listingPrice ?? "",
          priceIncludes: s.priceIncludes ?? "",
          command: s.command,
          maxClips: s.maxClips,
          compose: s.compose,
          headful: s.headful,
          voiceoverScriptApproval: s.command === "run" && Boolean(s.compose) && Boolean(meta?.features?.voiceoverEnabled),
        }),
      });
      setJobs((current) => [newJob, ...current]);
      setActiveJobId(newJob.id);
      await loadShellData({ silent: true });
      navigate("/workflow");
      return;
    }
    const runId = job.resumeRunId || runIdFromJobLogs(job.logs);
    if (step === "frames" && runId) {
      await handlePrepareFrames(runId);
      return;
    }
    if (step === "analyze" && runId) {
      await handleResumeIdentification(runId);
      return;
    }
    if (step === "compose" && runId) {
      await handleComposeRun(runId);
    }
  }

  async function handleResumeIdentification(runId) {
    if (!classificationEnabled) {
      showNotice("error", "Gemini off.");
      return;
    }

    const job = await fetchJson(`/api/runs/${encodeURIComponent(runId)}/identify`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        compose: false,
        headful: false,
      }),
    });

    showNotice("success", "Started.");
    setJobs((current) => [job, ...current]);
    setActiveJobId(job.id);
    await loadShellData({ silent: true });
    navigate("/workflow");
  }

  async function handleComposeRun(runId, options = {}) {
    const approvedScript = String(options.approvedScript ?? "").trim();
    const endpoint = `/api/runs/${encodeURIComponent(runId)}/compose`;
    const job = await fetchJson(endpoint, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        script: approvedScript,
        voiceoverScriptApproval: Boolean(meta?.features?.voiceoverEnabled) && !approvedScript,
      }),
    });

    showNotice("success", "Building full video.");
    setJobs((current) => [job, ...current]);
    setActiveJobId(job.id);
    await loadShellData({ silent: true });
    navigate("/workflow");
  }

  async function handleRerenderEnding(runId) {
    const job = await fetchJson(`/api/runs/${encodeURIComponent(runId)}/end-scene`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({}),
    });

    showNotice("success", "Rebuilding end scene.");
    setJobs((current) => [job, ...current]);
    setActiveJobId(job.id);
    await loadShellData({ silent: true });
    navigate("/workflow");
  }

  function openRun(runId) {
    navigate(`/runs/${encodeURIComponent(runId)}`);
  }

  if (booting) {
    return (
      <div className="app-shell">
        <main className="app-main loading-main">
          <LoadingState label="Loading" />
        </main>
      </div>
    );
  }

  const pageContext = {
    route,
    runs,
    activeJob,
    latestJob,
    meta,
    voiceoverEnabled: Boolean(meta?.features?.voiceoverEnabled),
    classificationEnabled,
    selectedRun,
    selectedRunError,
    loadingRun,
    navigate,
    openRun,
    onRefresh: () => loadShellData(),
    onStartWorkflow: async (values) => {
      try {
        await handleStartWorkflow(values);
      } catch (error) {
        showNotice("error", error.message);
        throw error;
      }
    },
    onResumeIdentification: async (runId) => {
      try {
        await handleResumeIdentification(runId);
      } catch (error) {
        showNotice("error", error.message);
      }
    },
    onComposeRun: async (runId, options) => {
      try {
        await handleComposeRun(runId, options);
      } catch (error) {
        showNotice("error", error.message);
      }
    },
    onRerenderEnding: async (runId) => {
      try {
        await handleRerenderEnding(runId);
      } catch (error) {
        showNotice("error", error.message);
      }
    },
    onVoiceoverDraft: async (runId) => {
      try {
        const job = await fetchJson(`/api/runs/${encodeURIComponent(runId)}/voiceover/draft`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({}),
        });
        showNotice("success", "Generating script options.");
        setJobs((current) => [job, ...current]);
        setActiveJobId(job.id);
        await loadShellData({ silent: true });
        navigate("/workflow");
      } catch (error) {
        showNotice("error", error.message);
      }
    },
    onVoiceoverApply: async (runId, script) => {
      try {
        const job = await fetchJson(`/api/runs/${encodeURIComponent(runId)}/voiceover/apply`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ script }),
        });
        showNotice("success", "Stitching voice-over.");
        setJobs((current) => [job, ...current]);
        setActiveJobId(job.id);
        await loadShellData({ silent: true });
        navigate("/workflow");
      } catch (error) {
        showNotice("error", error.message);
      }
    },
    onPrepareFrames: async (runId) => {
      try {
        await handlePrepareFrames(runId);
      } catch (error) {
        showNotice("error", error.message);
      }
    },
    onDeleteRun: async (runId) => {
      try {
        await handleDeleteRun(runId);
      } catch (error) {
        showNotice("error", error.message);
      }
    },
    onRetryFailedJob: async (job) => {
      try {
        await handleRetryFailedJob(job);
      } catch (error) {
        showNotice("error", error.message);
      }
    },
  };

  return (
    <div className="app-shell">
      <Notice notice={notice} onDismiss={() => setNotice(null)} />

      <header className="site-header">
        <div className="site-header__brand">
          <div className="site-brand-lockup">
            <div className="site-brand-copy">
              <h1 className="site-title">AU Real Footage Reels</h1>
            </div>
          </div>
        </div>

        <nav className="site-nav" aria-label="Primary">
          {navigationItems.map((item) => {
            const active = menuKeyForRoute(route) === item.key;
            return (
              <button
                key={item.key}
                type="button"
                className={`nav-link${active ? " is-active" : ""}`}
                onClick={() => navigate(item.key)}
              >
                {item.label}
              </button>
            );
          })}
        </nav>

      </header>

      <main className="app-main">
        <div className="page-intro">
          <h2 className="page-intro__title">{pageTitle(route.page)}</h2>
        </div>

        {renderPage(pageContext)}
      </main>
    </div>
  );
}

function Notice({ notice, onDismiss }) {
  if (!notice) {
    return null;
  }

  return (
    <div className={`notice notice--${notice.tone}`} role="status" aria-live="polite">
      <span>{notice.text}</span>
      <button type="button" className="notice__dismiss" onClick={onDismiss} aria-label="Dismiss notice">
        Close
      </button>
    </div>
  );
}

function renderPage(context) {
  switch (context.route.page) {
    case "dashboard":
      return <CompactDashboardPage {...context} />;
    case "run-detail":
      return <CompactRunDetailPage {...context} />;
    default:
      return (
        <EmptyState title="Not found" actionLabel="Studio" onAction={() => context.navigate("/workflow")} />
      );
  }
}

function inferFailedStep(job) {
  if (!job || job.status !== "failed") {
    return null;
  }
  const phase = job.progress?.phase;
  if (phase === "frames") {
    return "frames";
  }
  if (phase === "analyze") {
    return "analyze";
  }
  if (phase === "compose") {
    return "compose";
  }
  if (phase === "voiceover") {
    return "compose";
  }
  if (phase === "done") {
    return "download";
  }
  return "download";
}

function runIdFromJobLogs(logs) {
  if (!logs?.length) {
    return null;
  }
  for (const entry of logs) {
    const msg = String(entry.message ?? "");
    const m = /[\\/]runs[\\/]([^"'\s]+)/u.exec(msg);
    if (m) {
      return m[1];
    }
  }
  return null;
}

function VehicleInventoryLookup({ onSelect }) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const [vehicles, setVehicles] = useState([]);
  const [statusText, setStatusText] = useState("Type stock ID or title.");
  const [error, setError] = useState("");
  const [cacheStatus, setCacheStatus] = useState(null);
  const requestIdRef = useRef(0);

  useEffect(() => {
    if (!open) {
      return undefined;
    }

    const requestId = requestIdRef.current + 1;
    requestIdRef.current = requestId;
    const controller = new AbortController();
    const preparedQuery = query.trim();
    const timer = window.setTimeout(() => {
      setError("");
      setStatusText(preparedQuery ? "Searching..." : "Loading inventory...");

      Promise.all([
        fetchVehicleInventoryMatches(preparedQuery, { signal: controller.signal }),
        fetchVehicleInventoryStatus({ signal: controller.signal }),
      ])
        .then(([result, status]) => {
          if (requestIdRef.current !== requestId) {
            return;
          }
          setVehicles(result.matches ?? []);
          setCacheStatus(status);
          setStatusText(formatVehicleSearchResult(result, preparedQuery, status));
        })
        .catch((searchError) => {
          if (searchError?.name === "AbortError" || requestIdRef.current !== requestId) {
            return;
          }
          setVehicles([]);
          setError(inventoryLookupErrorMessage(searchError));
        });
    }, preparedQuery ? VEHICLE_SEARCH_DEBOUNCE_MS : 0);

    return () => {
      window.clearTimeout(timer);
      controller.abort();
    };
  }, [open, query]);

  function handleSelect(vehicle) {
    setQuery(formatInventoryVehicleSearchLabel(vehicle));
    setOpen(false);
    setError("");
    setStatusText("Selected.");
    onSelect(workflowValuesFromInventoryVehicle(vehicle));
  }

  function handleClear() {
    setQuery("");
    setVehicles([]);
    setError("");
    setStatusText("Type stock ID or title.");
    setOpen(true);
  }

  async function handleRefresh() {
    if (cacheStatus?.refreshing) {
      return;
    }
    setError("");
    setStatusText("Refreshing cache...");
    setCacheStatus((current) => (current ? { ...current, refreshing: true } : { refreshing: true }));
    try {
      await refreshVehicleInventoryCache();
      await pollVehicleInventoryStatus({
        onUpdate: (nextStatus) => {
          setCacheStatus(nextStatus);
          if (nextStatus?.refreshing) {
            setStatusText("Refreshing cache...");
          }
        },
      });
    } catch (refreshError) {
      setError(inventoryLookupErrorMessage(refreshError));
    }
  }

  function handleKeyDown(event) {
    if (event.key === "Escape") {
      setOpen(false);
      return;
    }
    if (event.key === "Enter" && open && vehicles[0]) {
      event.preventDefault();
      handleSelect(vehicles[0]);
    }
  }

  return (
    <div className="field field--span-full vehicle-lookup">
      <div className="vehicle-lookup__head">
        <span className="field__label">Inventory Search</span>
        <button
          type="button"
          className="button button--secondary vehicle-lookup__refresh"
          disabled={Boolean(cacheStatus?.refreshing)}
          onMouseDown={(event) => event.preventDefault()}
          onClick={() => void handleRefresh()}
        >
          {cacheStatus?.refreshing ? "Refreshing..." : "Refresh"}
        </button>
      </div>
      <div className="vehicle-lookup__control">
        <input
          className="field__input vehicle-lookup__input"
          type="search"
          autoComplete="off"
          spellCheck={false}
          placeholder="Stock ID or title"
          value={query}
          aria-label="Search inventory by stock ID or title"
          aria-expanded={open}
          onFocus={() => setOpen(true)}
          onBlur={() => {
            window.setTimeout(() => setOpen(false), 120);
          }}
          onChange={(event) => {
            setQuery(event.target.value);
            setOpen(true);
          }}
          onKeyDown={handleKeyDown}
        />
        {query ? (
          <button
            className="vehicle-lookup__clear"
            type="button"
            aria-label="Clear inventory search"
            onMouseDown={(event) => event.preventDefault()}
            onClick={handleClear}
          >
            x
          </button>
        ) : null}
      </div>

      {open ? (
        <div className="vehicle-lookup__menu" role="listbox" aria-label="Inventory vehicles">
          <div className={`vehicle-lookup__status${error ? " vehicle-lookup__status--error" : ""}`}>
            {error || statusText}
          </div>
          {vehicles.length ? (
            <div className="vehicle-lookup__results">
              {vehicles.map((vehicle, index) => (
                <button
                  key={inventoryVehicleKey(vehicle, index)}
                  className="vehicle-option"
                  type="button"
                  role="option"
                  aria-selected="false"
                  onMouseDown={(event) => event.preventDefault()}
                  onClick={() => handleSelect(vehicle)}
                >
                  <span className="vehicle-option__thumb">
                    {inventoryVehiclePhotoUrl(vehicle) ? (
                      <img src={inventoryVehiclePhotoUrl(vehicle)} alt="" loading="lazy" />
                    ) : (
                      <span className="vehicle-option__placeholder" aria-hidden="true" />
                    )}
                  </span>
                  <span className="vehicle-option__main">
                    <strong className="vehicle-option__title">{cleanInventoryValue(vehicle?.title) || "Untitled vehicle"}</strong>
                    <span className="vehicle-option__meta">{formatInventoryVehicleMeta(vehicle)}</span>
                  </span>
                  {formatInventoryPrice(vehicle?.salePrice) ? (
                    <span className="vehicle-option__price">{formatInventoryPrice(vehicle.salePrice)}</span>
                  ) : null}
                </button>
              ))}
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

async function fetchVehicleInventoryMatches(query, { signal } = {}) {
  const params = new URLSearchParams();
  if (query) {
    params.set("q", query);
  }
  params.set("limit", String(VEHICLE_SEARCH_RESULT_LIMIT));
  const response = await fetch(`${VEHICLE_INVENTORY_API_URL}/search?${params.toString()}`, { signal });
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || "Inventory search failed.");
  }
  return data;
}

async function fetchVehicleInventoryStatus({ signal } = {}) {
  const response = await fetch(`${VEHICLE_INVENTORY_API_URL}/status`, { signal });
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || "Inventory status failed.");
  }
  return data;
}

async function refreshVehicleInventoryCache() {
  const response = await fetch(`${VEHICLE_INVENTORY_API_URL}/refresh`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: "{}",
  });
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || "Inventory refresh failed.");
  }
  return data;
}

async function pollVehicleInventoryStatus({ onUpdate } = {}) {
  const deadline = Date.now() + 1000 * 60 * 4;
  while (Date.now() < deadline) {
    const status = await fetchVehicleInventoryStatus();
    onUpdate?.(status);
    if (!status?.refreshing) {
      return status;
    }
    await new Promise((resolve) => window.setTimeout(resolve, 900));
  }
  throw new Error("Inventory refresh timed out.");
}

function workflowValuesFromInventoryVehicle(vehicle) {
  return {
    listingTitle: cleanInventoryValue(vehicle?.title),
    stockId: cleanInventoryValue(vehicle?.stockNo) || cleanInventoryValue(vehicle?.id),
    listingPrice: formatInventoryPrice(vehicle?.salePrice),
    carDescription: buildInventoryVehicleDescription(vehicle),
    priceIncludes: cleanInventoryList(vehicle?.outline).join("\n"),
  };
}

function buildInventoryVehicleDescription(vehicle) {
  const existingDescription = cleanInventoryValue(vehicle?.description);
  if (existingDescription) {
    return existingDescription;
  }

  const title = cleanInventoryValue(vehicle?.title);
  const details = [
    formatInventoryKm(vehicle?.odometer),
    [cleanInventoryValue(vehicle?.fuel), cleanInventoryValue(vehicle?.transmission), cleanInventoryValue(vehicle?.driveTrain)]
      .filter(Boolean)
      .join(" / "),
    vehicle?.engineCc ? `${compactNumberFormatter.format(Number(vehicle.engineCc))}cc engine` : "",
    vehicle?.seats ? `${compactNumberFormatter.format(Number(vehicle.seats))} seats` : "",
    vehicle?.doors ? `${compactNumberFormatter.format(Number(vehicle.doors))} doors` : "",
    cleanInventoryValue(vehicle?.bodyType),
    cleanInventoryValue(vehicle?.color),
    cleanInventoryValue(vehicle?.auctionGrade) ? `auction grade ${cleanInventoryValue(vehicle.auctionGrade)}` : "",
  ].filter(Boolean);
  const features = cleanInventoryList(vehicle?.features).slice(0, 10);
  const lines = [];

  if (title) {
    lines.push(title);
  }
  if (details.length) {
    lines.push(`${details.join(", ")}.`);
  }
  if (features.length) {
    lines.push(`Features: ${features.join(", ")}.`);
  }
  if (formatInventoryPrice(vehicle?.salePrice)) {
    lines.push(`Price: ${formatInventoryPrice(vehicle.salePrice)}.`);
  }

  return lines.join("\n");
}

function formatVehicleSearchResult(result, query, status) {
  const refreshing = Boolean(status?.refreshing);
  if (refreshing) {
    return "Refreshing cache...";
  }

  const totalCount = Number(status?.count ?? result?.count ?? 0);
  const cachedAt = status?.cachedAt ? ` Cached: ${formatCompactDate(status.cachedAt)}.` : "";

  if (!result?.matches?.length) {
    if (!totalCount) {
      return `Cache empty.${cachedAt} Click refresh.`;
    }
    return query ? `No matches for "${query}".${cachedAt}` : `${totalCount} vehicles cached.${cachedAt}`;
  }

  const count = result.matches.length === 1 ? "1 match" : `${result.matches.length} matches`;
  const hint = result?.hint ? ` ${String(result.hint)}` : "";
  return `${count}.${cachedAt}${hint}`.trim();
}

function formatCompactDate(value) {
  try {
    return new Intl.DateTimeFormat(undefined, { dateStyle: "medium" }).format(new Date(value));
  } catch {
    return String(value);
  }
}

function thumbnailDraftStorageKey(runId) {
  return `${THUMBNAIL_DRAFT_STORAGE_KEY_PREFIX}:${String(runId ?? "").trim()}`;
}

function loadThumbnailDraft(runId) {
  const key = thumbnailDraftStorageKey(runId);
  if (!key || typeof window === "undefined") {
    return null;
  }
  try {
    const raw = window.localStorage.getItem(key);
    if (!raw) {
      return null;
    }
    const parsed = JSON.parse(raw);
    return {
      title: String(parsed?.title ?? ""),
      subtitle: String(parsed?.subtitle ?? ""),
      referenceImageDataUrl: String(parsed?.referenceImageDataUrl ?? ""),
      generatedImageUrl: String(parsed?.generatedImageUrl ?? ""),
    };
  } catch {
    return null;
  }
}

function saveThumbnailDraft(runId, draft) {
  const key = thumbnailDraftStorageKey(runId);
  if (!key || typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.setItem(key, JSON.stringify(draft));
    return;
  } catch {
    // Fall through and attempt a smaller payload.
  }
  try {
    const reducedDraft = {
      ...draft,
      referenceImageDataUrl: "",
    };
    window.localStorage.setItem(key, JSON.stringify(reducedDraft));
  } catch {
    // Ignore storage failures to keep the UI responsive.
  }
}

function inventoryLookupErrorMessage(error) {
  if (error instanceof TypeError) {
    return "Inventory API unavailable.";
  }
  return error?.message || "Inventory lookup failed.";
}

function inventoryVehicleKey(vehicle, index) {
  return [
    cleanInventoryValue(vehicle?.id),
    cleanInventoryValue(vehicle?.stockNo),
    cleanInventoryValue(vehicle?.chassisNo),
    index,
  ]
    .filter(Boolean)
    .join("-");
}

function inventoryVehiclePhotoUrl(vehicle) {
  const photoGroups = [vehicle?.exteriorPhoto, vehicle?.auctionPhotos, vehicle?.interiorPhoto];
  for (const group of photoGroups) {
    if (!Array.isArray(group)) {
      continue;
    }
    const photo = group.map(cleanInventoryValue).find(Boolean);
    if (photo) {
      return photo;
    }
  }
  return "";
}

function formatInventoryVehicleSearchLabel(vehicle) {
  const stock = cleanInventoryValue(vehicle?.stockNo) || cleanInventoryValue(vehicle?.id);
  const title = cleanInventoryValue(vehicle?.title);
  return [stock ? `Stock ${stock}` : "", title].filter(Boolean).join(" - ");
}

function formatInventoryVehicleMeta(vehicle) {
  const details = [
    cleanInventoryValue(vehicle?.stockNo) ? `Stock ${cleanInventoryValue(vehicle.stockNo)}` : "",
    formatInventoryKm(vehicle?.odometer),
    [cleanInventoryValue(vehicle?.fuel), cleanInventoryValue(vehicle?.transmission)]
      .filter(Boolean)
      .join(" / "),
    cleanInventoryValue(vehicle?.color),
  ].filter(Boolean);
  return details.join(" | ") || cleanInventoryValue(vehicle?.chassisNo) || "Vehicle";
}

function formatInventoryKm(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return "";
  }
  return `${compactNumberFormatter.format(numeric)} km`;
}

function formatInventoryPrice(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return "";
  }
  return `AU$${compactNumberFormatter.format(numeric)}`;
}

function cleanInventoryList(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.map(cleanInventoryValue).filter(Boolean);
}

function cleanInventoryValue(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function DashboardPage({
  classificationEnabled,
  voiceoverEnabled,
  onStartWorkflow,
  activeJob,
  latestJob,
  runs,
  openRun,
  onRefresh,
  onResumeIdentification,
  onComposeRun,
  onRerenderEnding,
  onPrepareFrames,
  onDeleteRun,
  onRetryFailedJob,
}) {
  const [submitting, setSubmitting] = useState(false);
  const [formError, setFormError] = useState("");
  const [values, setValues] = useState(() => loadWorkflowFormDraft(classificationEnabled));

  const job = activeJob ?? latestJob;
  const currentCommand = values.command;
  const deliveredRuns = runs.filter((run) => run.pipeline?.render?.done).length;
  const latestRenderedRun = runs.find((run) => run.pipeline?.render?.done) ?? null;
  const readyForReview = runs.filter((run) => run.pipeline?.frames?.done && !run.pipeline?.analyze?.done).length;
  const readyToRender = runs.filter((run) => run.pipeline?.analyze?.done && !run.pipeline?.render?.done).length;

  useEffect(() => {
    if (!classificationEnabled && values.command === "run") {
      setValues((current) => ({
        ...current,
        command: "prepare",
        compose: false,
      }));
    }
  }, [classificationEnabled, values.command]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    window.localStorage.setItem(WORKFLOW_DRAFT_STORAGE_KEY, JSON.stringify(values));
  }, [values]);

  async function handleSubmit(event) {
    event.preventDefault();
    if (submitting || activeJob) {
      return;
    }

    const preparedUrl = String(values.url ?? "").trim();
    const preparedTitle = String(values.listingTitle ?? "").trim();
    const preparedStock = String(values.stockId ?? "").trim();
    const preparedDescription = String(values.carDescription ?? "").trim();

    if (!preparedUrl) {
      setFormError("URL required.");
      return;
    }
    if (!preparedTitle) {
      setFormError("Title required.");
      return;
    }
    if (!preparedStock) {
      setFormError("Stock ID required.");
      return;
    }
    if (!preparedDescription) {
      setFormError("Car description required (used for the voice-over script).");
      return;
    }

    setFormError("");
    setSubmitting(true);

    try {
      await onStartWorkflow({
        url: preparedUrl,
        listingTitle: preparedTitle,
        stockId: preparedStock,
        carDescription: preparedDescription,
        listingPrice: String(values.listingPrice ?? "").trim(),
        priceIncludes: String(values.priceIncludes ?? "").trim(),
        command: values.command,
        maxClips: values.maxClips ? Number(values.maxClips) : null,
        compose: values.compose,
        headful: values.headful,
        voiceoverScriptApproval: true,
      });
    } catch (error) {
      setFormError(error.message);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="dashboard">
      <section className="panel panel--hero dashboard__overview">
        <div className="dashboard__overview-copy">
          <h3>Start a reel.</h3>
          <p>Paste the listing, choose the mode, run it.</p>
          <div className="dashboard__overview-pills">
            <StatusChip tone={classificationEnabled ? "success" : "muted"} label={classificationEnabled ? "AI on" : "AI off"} />
            <StatusChip tone={voiceoverEnabled ? "success" : "muted"} label={voiceoverEnabled ? "Voice on" : "Voice off"} />
            <StatusChip tone={activeJob ? "warning" : "success"} label={activeJob ? "Busy" : "Ready"} />
          </div>
        </div>

        <dl className="hero-metrics hero-metrics--dense dashboard__overview-metrics">
          <Metric value={runs.length} label="Total Runs" />
          <Metric value={deliveredRuns} label="Delivered" tone="success" />
          <Metric value={readyForReview} label="Ready for AI" />
          <Metric value={readyToRender} label="Ready to Render" tone={readyToRender ? "warning" : "default"} />
        </dl>
      </section>

      <div className="dashboard__grid">
        <section className="panel panel--form-only dashboard__new">
          <h3 className="dashboard__panel-title">New Run</h3>
          <form className="workflow-form" onSubmit={handleSubmit}>
            {!classificationEnabled ? (
              <InlineCallout
                tone="info"
                title="AI off"
                description="Only download and prepare are available."
              />
            ) : null}
            {activeJob ? (
              <InlineCallout
                tone="warning"
                title="Busy"
                description="Finish the current job first."
              />
            ) : null}
            {formError ? <InlineCallout tone="danger" title="Error" description={formError} /> : null}

            <VehicleInventoryLookup
              onSelect={(vehicleValues) => {
                setFormError("");
                setValues((current) => ({
                  ...current,
                  ...vehicleValues,
                }));
              }}
            />

            <label className="field">
              <span className="field__label">URL</span>
              <input
                className="field__input"
                type="url"
                inputMode="url"
                autoComplete="off"
                placeholder="https://photos.google.com/..."
                value={values.url}
                onChange={(event) => {
                  setFormError("");
                  setValues((c) => ({ ...c, url: event.target.value }));
                }}
              />
            </label>
            <label className="field">
              <span className="field__label">Title</span>
              <input
                className="field__input"
                type="text"
                autoComplete="off"
                placeholder="2021 Toyota Hiace SLWB"
                value={values.listingTitle}
                onChange={(event) => {
                  setFormError("");
                  setValues((c) => ({ ...c, listingTitle: event.target.value }));
                }}
              />
            </label>
            <label className="field">
              <span className="field__label">Stock ID</span>
              <input
                className="field__input"
                type="text"
                autoComplete="off"
                placeholder="CB-1042"
                value={values.stockId}
                onChange={(event) => {
                  setFormError("");
                  setValues((c) => ({ ...c, stockId: event.target.value }));
                }}
              />
            </label>
            <label className="field">
              <span className="field__label">Description</span>
              <textarea
                className="field__input field__input--textarea"
                rows={5}
                autoComplete="off"
                placeholder="e.g. 2021 Toyota Hiace DX. 4WD, auto, 82k km. 2.8L turbo diesel, reversing camera. $37,900."
                value={values.carDescription}
                onChange={(event) => {
                  setFormError("");
                  setValues((c) => ({ ...c, carDescription: event.target.value }));
                }}
              />
            </label>
            <label className="field">
              <span className="field__label">Price</span>
              <input
                className="field__input"
                type="text"
                autoComplete="off"
                placeholder="AU$10,400"
                value={values.listingPrice}
                onChange={(event) => {
                  setFormError("");
                  setValues((c) => ({ ...c, listingPrice: event.target.value }));
                }}
              />
            </label>
            <label className="field">
              <span className="field__label">Price Includes</span>
              <textarea
                className="field__input field__input--textarea"
                rows={2}
                autoComplete="off"
                placeholder={"6 Months NSW Registration\nFresh Roadworthy Certificate"}
                value={values.priceIncludes}
                onChange={(event) => {
                  setFormError("");
                  setValues((c) => ({ ...c, priceIncludes: event.target.value }));
                }}
              />
            </label>

            <div className="field-group">
              <div className="field">
                <span className="field__label">Mode</span>
                <div className="mode-grid" role="radiogroup" aria-label="Workflow mode">
                  {commandOptions.map((option) => {
                    const disabled = option.value === "run" && !classificationEnabled;
                    const active = values.command === option.value;
                    return (
                      <button
                        key={option.value}
                        type="button"
                        className={`mode-card${active ? " is-active" : ""}`}
                        disabled={disabled}
                        aria-pressed={active}
                        onClick={() =>
                          !disabled &&
                          setValues((c) => ({
                            ...c,
                            command: option.value,
                            compose: option.value === "run" ? c.compose : false,
                          }))
                        }
                      >
                        <span className="mode-card__eyebrow">{option.eyebrow}</span>
                        <strong>{option.label}</strong>
                        <span>{option.description}</span>
                      </button>
                    );
                  })}
                </div>
              </div>
            </div>

            <div className="form-row">
              <label className="field">
                <span className="field__label">Max clips</span>
                <input
                  className="field__input"
                  type="number"
                  min="1"
                  inputMode="numeric"
                  value={values.maxClips}
                  onChange={(event) => setValues((c) => ({ ...c, maxClips: event.target.value }))}
                />
              </label>
              <div className="toggle-group">
                <ToggleField
                  label="Browser"
                  checked={values.headful}
                  onChange={(checked) => setValues((c) => ({ ...c, headful: checked }))}
                />
                {currentCommand === "run" ? (
                  <ToggleField
                    label="Compose"
                    checked={values.compose}
                    onChange={(checked) =>
                      setValues((c) => ({ ...c, compose: checked, voiceoverScriptApproval: true }))
                    }
                  />
                ) : null}
              </div>
            </div>

            {currentCommand === "run" && values.compose && voiceoverEnabled ? (
              <InlineCallout
                tone="info"
                title="Voice step"
                description="Approve the script later from the run page."
              />
            ) : null}

            <div className="form-actions">
              <button className="button button--primary" type="submit" disabled={Boolean(activeJob) || submitting}>
                {submitting ? "Starting..." : commandButtonLabel(currentCommand)}
              </button>
            </div>
          </form>
        </section>

        <div className="dashboard__aside">
          <section className="panel dashboard__job">
            <h3 className="dashboard__panel-title">Current Job</h3>
            {job ? (
              <>
                <div className="job-summary">
                  <span className="job-summary__cmd">{commandLabel(job.command)}</span>
                  <span className={`job-summary__status job-summary__status--${job.status}`}>{job.status}</span>
                </div>
                <p className="job-summary__meta">{formatJobSourceSummary(job)}</p>

                {job.status === "running" && job.progress ? (
                  <div className="progress-block">
                    <div className="progress-bar" role="progressbar" aria-valuenow={job.progress.percent} aria-valuemin={0} aria-valuemax={100}>
                      <div className="progress-bar__fill" style={{ width: `${Math.min(100, job.progress.percent)}%` }} />
                    </div>
                    <div className="progress-block__label">
                      <span className="progress-phase">{job.progress.phase}</span>
                      <span>{job.progress.label}</span>
                    </div>
                  </div>
                ) : null}

                <JobPipelineRail job={job} />

                {job.status === "failed" ? (
                  <div className="job-retry">
                    <InlineCallout tone="danger" title="Pipeline failed" description={job.error} />
                    <button type="button" className="button button--primary" onClick={() => void onRetryFailedJob(job)}>
                      Retry from {inferFailedStep(job) || "start"}
                    </button>
                  </div>
                ) : null}

                {job.result?.runId ? (
                  <div className="job-open-run">
                    <button className="button button--secondary" type="button" onClick={() => openRun(job.result.runId)}>
                      Open run
                    </button>
                  </div>
                ) : null}

                <details className="job-logs-toggle">
                  <summary>Log</summary>
                  {job.logs.length ? (
                    <ol className="log-list log-list--compact">
                      {job.logs.map((entry) => (
                        <li key={`${entry.at}-${entry.message}`} className="log-list__item">
                          <span className="log-list__time">{formatTime(entry.at)}</span>
                          <div>{entry.message}</div>
                        </li>
                      ))}
                    </ol>
                  ) : (
                    <EmptyBlock title="Empty" />
                  )}
                </details>
              </>
            ) : (
              <EmptyBlock title="No job" />
            )}
          </section>

          <section className="panel dashboard__sample">
            <div className="dashboard__library-head">
              <div className="dashboard__head-copy">
                <h3 className="dashboard__panel-title">Latest Output</h3>
              </div>
            </div>

            {latestRenderedRun ? (
              <div className="dashboard__sample-copy">
                <article className="dashboard__sample-card">
                  <p className="run-card__eyebrow">Rendered</p>
                  <h4>{latestRenderedRun.listingTitle || latestRenderedRun.runId}</h4>
                  {latestRenderedRun.stockId ? <p className="run-card__sub">{latestRenderedRun.stockId}</p> : null}
                  {latestRenderedRun.listingPrice ? (
                    <p className="dashboard__sample-price">{latestRenderedRun.listingPrice}</p>
                  ) : null}
                </article>

                <div className="detail-actions">
                  <button
                    className="button button--primary"
                    type="button"
                    disabled={Boolean(activeJob)}
                    onClick={() => void onRerenderEnding(latestRenderedRun.runId)}
                  >
                    Rebuild End Scene
                  </button>
                  <button
                    className="button button--secondary"
                    type="button"
                    onClick={() => openRun(latestRenderedRun.runId)}
                  >
                    Open run
                  </button>
                </div>
              </div>
            ) : (
              <EmptyBlock title="No rendered reel yet" />
            )}
          </section>
        </div>
      </div>

      <section className="panel dashboard__library" id="runs">
        <div className="dashboard__library-head">
          <h3 className="dashboard__panel-title">Runs</h3>
          <button className="button button--secondary" type="button" onClick={onRefresh}>
            Refresh
          </button>
        </div>

        {runs.length ? (
          <div className="run-list">
            {runs.map((run) => (
              <article key={run.runId} className="run-card run-card--dashboard">
                <div className="run-card__main" onClick={() => openRun(run.runId)} role="presentation">
                  <div className="run-card__header">
                    <div>
                      <p className="run-card__eyebrow">
                        {formatDate(run.createdAt)}
                        {run.stockId ? ` | ${run.stockId}` : ""}
                      </p>
                      <h4>{run.listingTitle || run.runId}</h4>
                      {run.listingTitle ? <p className="run-card__sub">{run.runId}</p> : null}
                    </div>
                  </div>
                  <RunPipelineDots pipeline={run.pipeline} />
                  <div className="run-card__stats">
                    <StatPair label="Clips" value={run.stats.downloads} />
                    <StatPair label="Frames" value={run.stats.frames} />
                    <StatPair label="AI" value={run.stats.analyzed} />
                    <StatPair label="Cut" value={run.stats.planned} />
                  </div>
                </div>

                <div className="run-card__actions">
                  {run.pipeline?.download?.done && !run.pipeline?.frames?.done ? (
                    <button
                      className="button button--ghost"
                      type="button"
                      disabled={Boolean(activeJob)}
                      onClick={(event) => {
                        event.stopPropagation();
                        void onPrepareFrames(run.runId);
                      }}
                    >
                      Frames
                    </button>
                  ) : null}
                  {run.pipeline?.frames?.done && !run.pipeline?.analyze?.done ? (
                    <button
                      className="button button--ghost"
                      type="button"
                      disabled={!classificationEnabled || Boolean(activeJob)}
                      onClick={(event) => {
                        event.stopPropagation();
                        void onResumeIdentification(run.runId);
                      }}
                    >
                      Classify
                    </button>
                  ) : null}
                  {run.pipeline?.render?.done ? (
                    <button
                      className="button button--ghost"
                      type="button"
                      disabled={Boolean(activeJob)}
                      onClick={(event) => {
                        event.stopPropagation();
                        void onRerenderEnding(run.runId);
                      }}
                    >
                      Rebuild End Scene
                    </button>
                  ) : null}
                  <button
                    className="button button--secondary"
                    type="button"
                    onClick={(event) => {
                      event.stopPropagation();
                      openRun(run.runId);
                    }}
                  >
                    View
                  </button>
                  <button
                    className="button button--danger"
                    type="button"
                    onClick={(event) => {
                      event.stopPropagation();
                      void onDeleteRun(run.runId);
                    }}
                  >
                    Delete
                  </button>
                </div>
              </article>
            ))}
          </div>
        ) : (
          <EmptyBlock title="No runs" />
        )}
      </section>
    </div>
  );
}

function VoiceoverScriptPanel({
  run,
  activeJob,
  classificationEnabled,
  voiceoverEnabled,
  onVoiceoverDraft,
  onVoiceoverApply,
  onComposeRun,
}) {
  const variants = run.voiceoverDraft?.variants ?? [];
  const busy = Boolean(activeJob);
  const hasRenderedVideo = Boolean(run.pipeline?.render?.done);
  const canReviewScripts = Boolean(
    run.pipeline?.analyze?.done || variants.length || run.voiceoverScript || run.voiceoverStatus === "failed",
  );
  const voiceoverFailed = run.voiceoverStatus === "failed" && !run.hasVoiceover;

  const [selectedId, setSelectedId] = useState("");
  const [editedScript, setEditedScript] = useState("");
  const [scriptDirty, setScriptDirty] = useState(false);

  useEffect(() => {
    const list = run.voiceoverDraft?.variants ?? [];
    const applied = String(run.voiceoverScript ?? "").trim();
    if (!list.length) {
      setSelectedId("");
      setEditedScript(applied);
      setScriptDirty(false);
      return;
    }
    setSelectedId((prev) => {
      if (applied) {
        const matched = list.find((v) => String(v.script ?? "").trim() === applied);
        return matched?.id ?? "";
      }
      if (prev && list.some((v) => v.id === prev)) {
        return prev;
      }
      return list[0].id;
    });
  }, [run.runId, run.voiceoverDraft, run.voiceoverScript]);

  useEffect(() => {
    setScriptDirty(false);
  }, [run.runId, selectedId]);

  useEffect(() => {
    if (scriptDirty) {
      return;
    }
    const list = run.voiceoverDraft?.variants ?? [];
    const v = list.find((x) => x.id === selectedId);
    if (v) {
      setEditedScript(v.script);
      return;
    }
    const applied = String(run.voiceoverScript ?? "").trim();
    if (applied) {
      setEditedScript(applied);
    }
  }, [run.voiceoverDraft, run.voiceoverScript, selectedId, scriptDirty]);

  if (!classificationEnabled || !voiceoverEnabled || !canReviewScripts) {
    return null;
  }

  async function handleBuild() {
    const text = String(editedScript ?? "").trim();
    if (!text) {
      return;
    }
    if (hasRenderedVideo) {
      await onVoiceoverApply(run.runId, text);
      return;
    }
    await onComposeRun(run.runId, { approvedScript: text });
  }

  return (
    <section className="panel voiceover-script-panel">
      <h3 className="section-heading__title section-heading__title--panel">Video Script</h3>
      <p className="field__hint">
        {hasRenderedVideo
          ? voiceoverFailed
            ? "The video build finished without audio because ElevenLabs failed after retries. Edit the script if needed, then retry voice-over stitching."
            : "Edit the script if needed, then stitch or replace the voice-over on the current reel."
          : "Pick one of the three script options, adjust it if needed, then generate the full video."}
      </p>

      {!variants.length ? (
        <div className="voiceover-script-panel__row">
          <button
            type="button"
            className="button button--primary"
            disabled={busy}
            onClick={() => void onVoiceoverDraft(run.runId)}
          >
            Generate 3 options
          </button>
        </div>
      ) : (
        <>
          <div className="voiceover-script-panel__variants" role="radiogroup" aria-label="Script draft">
            {variants.map((v) => (
              <label key={v.id} className="voiceover-variant">
                <input
                  type="radio"
                  name={`vo-variant-${run.runId}`}
                  checked={selectedId === v.id}
                  onChange={() => setSelectedId(v.id)}
                />
                <span className="voiceover-variant__label">{v.label || v.id}</span>
                <span className="voiceover-variant__preview">{v.script}</span>
              </label>
            ))}
          </div>

          <label className="field">
            <span className="field__label">Script to stitch</span>
            <textarea
              className="field__input field__input--textarea"
              rows={4}
              value={editedScript}
              onChange={(event) => {
                setScriptDirty(true);
                setEditedScript(event.target.value);
              }}
            />
          </label>

          <div className="voiceover-script-panel__row">
            <button
              type="button"
              className="button button--secondary"
              disabled={busy}
              onClick={() => void onVoiceoverDraft(run.runId)}
            >
              Regenerate options
            </button>
            <button
              type="button"
              className="button button--primary"
              disabled={busy || !String(editedScript ?? "").trim()}
              onClick={() => void handleBuild()}
            >
              {hasRenderedVideo
                ? voiceoverFailed
                  ? "Retry Voice-over"
                  : "Apply Voice-over"
                : "Generate Full Video"}
            </button>
          </div>
        </>
      )}
    </section>
  );
}

function RunPipelineDots({ pipeline }) {
  if (!pipeline) {
    return null;
  }
  const keys = ["download", "frames", "analyze", "render"];
  return (
    <div className="pipeline-dots" aria-hidden="true">
      {keys.map((k) => (
        <span key={k} className={`pipeline-dot pipeline-dot--${pipeline[k]?.done ? "on" : "off"}`} title={k} />
      ))}
    </div>
  );
}

function JobPipelineRail({ job }) {
  const phase = job.progress?.phase;
  const isRunning = job.status === "running";
  const steps = [
    { key: "download", label: "Load" },
    { key: "frames", label: "Frames" },
    { key: "analyze", label: "AI" },
    { key: "compose", label: "Build" },
    { key: "voiceover", label: "Script" },
  ];

  return (
    <ol className="pipeline-rail">
      {steps.map((s) => {
        const active = isRunning && phase === s.key;
        return (
          <li key={s.key} className={`pipeline-rail__step${active ? " pipeline-rail__step--active" : ""}`}>
            {s.label}
          </li>
        );
      })}
    </ol>
  );
}

function RunDetailPage({
  selectedRun,
  selectedRunError,
  loadingRun,
  navigate,
  onResumeIdentification,
  onComposeRun,
  onPrepareFrames,
  onDeleteRun,
  onVoiceoverDraft,
  onVoiceoverApply,
  onRerenderEnding,
  classificationEnabled,
  voiceoverEnabled,
  activeJob,
}) {
  if (loadingRun) {
    return <LoadingState label="Loading" compact />;
  }

  if (selectedRunError) {
    return (
      <section className="panel">
        <InlineCallout tone="danger" title="Error" description={selectedRunError} />
      </section>
    );
  }

  if (!selectedRun) {
    return <EmptyState title="Not found" actionLabel="Studio" onAction={() => navigate("/workflow")} />;
  }

  const p = selectedRun.pipeline;
  const previewClips = selectedRun.videos.filter((v) => v.videoUrl).slice(0, 4);
  const sequenceItems = selectedRun.plan?.composition?.segments?.length
    ? selectedRun.plan.composition.segments
    : selectedRun.plan?.sequence ?? [];

  return (
    <div className="page-stack">
      <section className="panel detail-toolbar">
        <p className="dashboard__panel-title">Run controls</p>
        <div className="detail-actions">
          <button className="button button--secondary" type="button" onClick={() => navigate("/workflow")}>
            Back
          </button>

          {p?.download?.done && !p?.frames?.done ? (
            <button
              className="button button--primary"
              type="button"
              disabled={Boolean(activeJob)}
              onClick={() => void onPrepareFrames(selectedRun.runId)}
            >
              Frames
            </button>
          ) : null}

          {p?.frames?.done && !p?.analyze?.done ? (
            <button
              className="button button--primary"
              type="button"
              disabled={!classificationEnabled || Boolean(activeJob)}
              onClick={() => void onResumeIdentification(selectedRun.runId)}
            >
              Analyze
            </button>
          ) : null}

          {p?.render?.done ? (
            <button
              className="button button--primary"
              type="button"
              disabled={Boolean(activeJob)}
              onClick={() => void onRerenderEnding(selectedRun.runId)}
            >
              Rebuild End Scene
            </button>
          ) : null}

          {selectedRun.finalReelUrl ? (
            <a className="button button--ghost" href={selectedRun.finalReelUrl} target="_blank" rel="noreferrer">
              Open output
            </a>
          ) : null}

          <button
            className="button button--danger"
            type="button"
            onClick={() => void onDeleteRun(selectedRun.runId)}
          >
            Delete
          </button>
        </div>
      </section>

      <section className="panel panel--hero detail-hero">
        <div className="hero-copy">
          <p className="run-card__eyebrow">{selectedRun.stockId ? `Stock ${selectedRun.stockId}` : "Vehicle reel"}</p>
          <h3>{selectedRun.listingTitle || selectedRun.runId}</h3>
          {selectedRun.listingTitle ? <p className="detail-run-id">{selectedRun.runId}</p> : null}
          <p className="detail-date">{formatDate(selectedRun.createdAt)}</p>
          {selectedRun.totalReelDurationSeconds != null ? (
            <p className="detail-run-id">
              Length ~{selectedRun.totalReelDurationSeconds}s
              {selectedRun.mainMontageDurationSeconds != null && selectedRun.endSceneDurationSeconds != null
                ? ` (${selectedRun.mainMontageDurationSeconds}s montage + ${selectedRun.endSceneDurationSeconds}s end card)`
                : null}
            </p>
          ) : null}
          {selectedRun.carDescription ? (
            <details className="run-detail__description">
              <summary>Car description</summary>
              <p className="run-detail__description-body">{selectedRun.carDescription}</p>
            </details>
          ) : null}
          {selectedRun.hasVoiceover && selectedRun.voiceoverScript ? (
            <details className="run-detail__voiceover" open>
              <summary>Voice-over script (main montage)</summary>
              <p className="run-detail__voiceover-body">{selectedRun.voiceoverScript}</p>
            </details>
          ) : null}
          <div className="run-detail__pipeline">
            <RunPipelineDots pipeline={selectedRun.pipeline} />
          </div>
        </div>

        <dl className="hero-metrics hero-metrics--dense">
          <Metric value={selectedRun.stats.downloads} label="Clips" />
          <Metric value={selectedRun.stats.frames} label="Frames" />
          <Metric value={selectedRun.stats.analyzed} label="AI" />
          <Metric value={selectedRun.stats.planned} label="Cut" />
        </dl>
      </section>

      {selectedRun.voiceoverStatus === "failed" ? (
        <section className="panel">
          <InlineCallout
            tone="warning"
            title="Voice-over failed. The reel stayed silent."
            description={buildVoiceoverFailureMessage(selectedRun)}
          />
        </section>
      ) : null}

      {selectedRun.finalReelUrl ? (
        <section className="panel">
          <h3 className="section-heading__title section-heading__title--panel">Final reel</h3>
          <div className="preview-frame preview-frame--hero">
            <VideoPlayer src={selectedRun.finalReelUrl} title={selectedRun.listingTitle || selectedRun.runId} />
          </div>
        </section>
      ) : null}

      <VoiceoverScriptPanel
        key={selectedRun.runId}
        run={selectedRun}
        activeJob={activeJob}
        classificationEnabled={classificationEnabled}
        voiceoverEnabled={voiceoverEnabled}
        onVoiceoverDraft={onVoiceoverDraft}
        onVoiceoverApply={onVoiceoverApply}
        onComposeRun={onComposeRun}
      />

      {previewClips.length ? (
        <section className="panel">
          <h3 className="section-heading__title section-heading__title--panel">Source footage</h3>
          <div className="source-preview-grid">
            {previewClips.map((v) => (
              <div key={v.clipId} className="source-preview-tile">
                <VideoPlayer
                  src={v.videoUrl}
                  title={v.title || v.clipId}
                  compact
                  className="video-player--tile"
                />
                <span className="source-preview-tile__id">{v.clipId}</span>
              </div>
            ))}
          </div>
        </section>
      ) : null}

      <section className="panel">
        <div className="section-heading section-heading--compact">
          <h3 className="section-heading__title">Reel sequence</h3>
        </div>

        {sequenceItems.length ? (
          <div className="sequence-grid">
            {sequenceItems.map((item, index) => (
              <article key={`${item.purpose || item.role}-${item.clipId}-${index}`} className="sequence-card">
                <div className="sequence-card__header">
                  <span className="role-pill">{roleLabel(item.purpose || item.role)}</span>
                  <span className="sequence-card__label">{formatShotLabel(item.analysis?.primaryLabel ?? item.primaryLabel)}</span>
                </div>
                <h4>{item.title || item.clipId}</h4>
                {item.frameUrl ? (
                  <a className="sequence-card__frame" href={item.frameUrl} target="_blank" rel="noreferrer">
                    <img src={item.frameUrl} alt={item.title || item.clipId} />
                  </a>
                ) : (
                  <div className="sequence-card__frame sequence-card__frame--empty">
                    <span>-</span>
                  </div>
                )}
                <div className="sequence-card__actions">
                  {item.videoUrl ? (
                    <a className="button button--ghost" href={item.videoUrl} target="_blank" rel="noreferrer">
                      Video
                    </a>
                  ) : null}
                  {item.frameUrl ? (
                    <a className="button button--secondary" href={item.frameUrl} target="_blank" rel="noreferrer">
                      Frame
                    </a>
                  ) : null}
                </div>
              </article>
            ))}
          </div>
        ) : (
          <EmptyBlock title="No sequence" />
        )}
      </section>

      <section className="panel">
        <div className="section-heading section-heading--compact">
          <h3 className="section-heading__title">All clips</h3>
        </div>

        {selectedRun.videos.length ? (
          <div className="clip-grid">
            {selectedRun.videos.map((video) => (
              <article key={video.clipId} className="clip-card">
                <div className="clip-card__media">
                  {video.frameUrl ? (
                    <a href={video.frameUrl} target="_blank" rel="noreferrer">
                      <img src={video.frameUrl} alt={video.title || video.clipId} loading="lazy" />
                    </a>
                  ) : (
                    <div className="clip-card__empty">-</div>
                  )}
                </div>

                <div className="clip-card__body">
                  <div className="clip-card__header">
                    <h4>{video.title || video.clipId}</h4>
                    <span className="clip-label">{formatShotLabel(video.analysis?.primaryLabel)}</span>
                  </div>

                  <div className="clip-card__actions">
                    {video.videoUrl ? (
                      <a className="button button--ghost" href={video.videoUrl} target="_blank" rel="noreferrer">
                        Video
                      </a>
                    ) : null}
                    {video.frameUrl ? (
                      <a className="button button--secondary" href={video.frameUrl} target="_blank" rel="noreferrer">
                        Frame
                      </a>
                    ) : null}
                  </div>
                </div>
              </article>
            ))}
          </div>
        ) : (
          <EmptyBlock title="No clips" />
        )}
      </section>
    </div>
  );
}

function CompactDashboardPage({
  classificationEnabled,
  voiceoverEnabled,
  onStartWorkflow,
  activeJob,
  latestJob,
  runs,
  openRun,
  onRefresh,
  onResumeIdentification,
  onComposeRun,
  onRerenderEnding,
  onPrepareFrames,
  onDeleteRun,
  onRetryFailedJob,
}) {
  const [submitting, setSubmitting] = useState(false);
  const [formError, setFormError] = useState("");
  const [values, setValues] = useState(() => loadWorkflowFormDraft(classificationEnabled));

  const job = activeJob ?? latestJob ?? null;
  const currentCommand = values.command;
  const latestRenderedRun = runs.find((run) => run.pipeline?.render?.done) ?? null;

  useEffect(() => {
    if (!classificationEnabled && values.command === "run") {
      setValues((current) => ({
        ...current,
        command: "prepare",
        compose: false,
      }));
    }
  }, [classificationEnabled, values.command]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    window.localStorage.setItem(WORKFLOW_DRAFT_STORAGE_KEY, JSON.stringify(values));
  }, [values]);

  async function handleSubmit(event) {
    event.preventDefault();
    if (submitting || activeJob) {
      return;
    }

    const preparedUrl = String(values.url ?? "").trim();
    const preparedTitle = String(values.listingTitle ?? "").trim();
    const preparedStock = String(values.stockId ?? "").trim();
    const preparedDescription = String(values.carDescription ?? "").trim();

    if (!classificationEnabled) {
      setFormError("AI is required so we can generate the 3 script options first.");
      return;
    }
    if (!preparedUrl) {
      setFormError("URL required.");
      return;
    }
    if (!preparedTitle) {
      setFormError("Title required.");
      return;
    }
    if (!preparedStock) {
      setFormError("Stock ID required.");
      return;
    }
    if (!preparedDescription) {
      setFormError("Description required.");
      return;
    }

    setFormError("");
    setSubmitting(true);

    try {
      await onStartWorkflow({
        url: preparedUrl,
        listingTitle: preparedTitle,
        stockId: preparedStock,
        carDescription: preparedDescription,
        listingPrice: String(values.listingPrice ?? "").trim(),
        priceIncludes: String(values.priceIncludes ?? "").trim(),
        command: "run",
        maxClips: values.maxClips ? Number(values.maxClips) : null,
        compose: false,
        headful: values.headful,
        voiceoverScriptApproval: true,
      });
    } catch (error) {
      setFormError(error.message);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="dashboard dashboard--compact">
      <section className="panel dashboard-command">
        <div className="dashboard-command__top">
          <div className="dashboard-command__title">
            <h3 className="dashboard__panel-title">New Run</h3>
          </div>
        </div>

        {job ? (
          <section className="dashboard-job-strip">
            <div className="dashboard-job-strip__head">
              <div className="job-summary">
                <span className="job-summary__cmd">{commandLabel(job.command)}</span>
                <span className={`job-summary__status job-summary__status--${job.status}`}>{job.status}</span>
              </div>
              <div className="dashboard-job-strip__actions">
                {job.result?.runId ? (
                  <button className="button button--secondary" type="button" onClick={() => openRun(job.result.runId)}>
                    Open run
                  </button>
                ) : null}
                {job.status === "failed" ? (
                  <button type="button" className="button button--primary" onClick={() => void onRetryFailedJob(job)}>
                    Retry
                  </button>
                ) : null}
              </div>
            </div>

            <p className="job-summary__meta">{formatJobSourceSummary(job)}</p>

            {job.status === "running" && job.progress ? (
              <div className="progress-block">
                <div className="progress-bar" role="progressbar" aria-valuenow={job.progress.percent} aria-valuemin={0} aria-valuemax={100}>
                  <div className="progress-bar__fill" style={{ width: `${Math.min(100, job.progress.percent)}%` }} />
                </div>
                <div className="progress-block__label">
                  <span className="progress-phase">{job.progress.phase}</span>
                  <span>{job.progress.label}</span>
                </div>
              </div>
            ) : null}

            <JobPipelineRail job={job} />

            {job.status === "failed" ? (
              <InlineCallout tone="danger" title="Failed" description={job.error} />
            ) : null}

            <details className="job-logs-toggle">
              <summary>Log</summary>
              {job.logs.length ? (
                <ol className="log-list log-list--compact">
                  {job.logs.map((entry) => (
                    <li key={`${entry.at}-${entry.message}`} className="log-list__item">
                      <span className="log-list__time">{formatTime(entry.at)}</span>
                      <div>{entry.message}</div>
                    </li>
                  ))}
                </ol>
              ) : (
                <EmptyBlock title="Empty" />
              )}
            </details>
          </section>
        ) : latestRenderedRun ? (
          <section className="dashboard-latest-strip">
            <div className="dashboard-latest-strip__title">
              <strong>{latestRenderedRun.listingTitle || latestRenderedRun.runId}</strong>
              <span>
                Latest render{latestRenderedRun.stockId ? ` | ${latestRenderedRun.stockId}` : ""}
                {latestRenderedRun.listingPrice ? ` | ${latestRenderedRun.listingPrice}` : ""}
              </span>
            </div>
            <div className="dashboard-latest-strip__actions">
              <button
                className="button button--secondary"
                type="button"
                onClick={() => openRun(latestRenderedRun.runId)}
              >
                Open
              </button>
            </div>
          </section>
        ) : null}

        <form className="workflow-form workflow-form--compact" onSubmit={handleSubmit}>
          {!classificationEnabled ? (
            <InlineCallout tone="info" title="AI off" description="This script-first workflow needs Gemini enabled." />
          ) : null}
          {activeJob ? (
            <InlineCallout tone="warning" title="Busy" description="Finish the current job first." />
          ) : null}
          {formError ? <InlineCallout tone="danger" title="Error" description={formError} /> : null}

          <div className="compact-fields">
            <VehicleInventoryLookup
              onSelect={(vehicleValues) => {
                setFormError("");
                setValues((current) => ({
                  ...current,
                  ...vehicleValues,
                }));
              }}
            />

            <label className="field field--span-full">
              <span className="field__label">URL</span>
              <input
                className="field__input"
                type="url"
                inputMode="url"
                autoComplete="off"
                placeholder="https://photos.google.com/..."
                value={values.url}
                onChange={(event) => {
                  setFormError("");
                  setValues((c) => ({ ...c, url: event.target.value }));
                }}
              />
            </label>
            <label className="field field--span-2">
              <span className="field__label">Title</span>
              <input
                className="field__input"
                type="text"
                autoComplete="off"
                placeholder="2021 Toyota Hiace SLWB"
                value={values.listingTitle}
                onChange={(event) => {
                  setFormError("");
                  setValues((c) => ({ ...c, listingTitle: event.target.value }));
                }}
              />
            </label>
            <label className="field">
              <span className="field__label">Stock ID</span>
              <input
                className="field__input"
                type="text"
                autoComplete="off"
                placeholder="CB-1042"
                value={values.stockId}
                onChange={(event) => {
                  setFormError("");
                  setValues((c) => ({ ...c, stockId: event.target.value }));
                }}
              />
            </label>
            <label className="field">
              <span className="field__label">Price</span>
              <input
                className="field__input"
                type="text"
                autoComplete="off"
                placeholder="AU$10,400"
                value={values.listingPrice}
                onChange={(event) => {
                  setFormError("");
                  setValues((c) => ({ ...c, listingPrice: event.target.value }));
                }}
              />
            </label>
            <label className="field field--span-2">
              <span className="field__label">Description</span>
              <textarea
                className="field__input field__input--textarea"
                rows={5}
                autoComplete="off"
                placeholder="Year, trim, km, features, price."
                value={values.carDescription}
                onChange={(event) => {
                  setFormError("");
                  setValues((c) => ({ ...c, carDescription: event.target.value }));
                }}
              />
            </label>
            <label className="field field--span-2">
              <span className="field__label">Price Includes</span>
              <textarea
                className="field__input field__input--textarea"
                rows={5}
                autoComplete="off"
                placeholder={"6 Months NSW Registration\nFresh Roadworthy Certificate"}
                value={values.priceIncludes}
                onChange={(event) => {
                  setFormError("");
                  setValues((c) => ({ ...c, priceIncludes: event.target.value }));
                }}
              />
            </label>
          </div>

          <InlineCallout
            tone="info"
            title="Flow"
            description="Step 1 downloads the clips, analyzes them, and prepares 3 script options. Step 2 happens in the run page, where you choose or edit the script and then build the full video."
          />

          <div className="dashboard-command__bottom">
            <div className="toggle-group toggle-group--inline">
              <label className="field">
                <span className="field__label">Max clips</span>
                <input
                  className="field__input"
                  type="number"
                  min="1"
                  inputMode="numeric"
                  value={values.maxClips}
                  onChange={(event) => setValues((c) => ({ ...c, maxClips: event.target.value }))}
                />
              </label>
              <ToggleField
                label="Browser"
                checked={values.headful}
                onChange={(checked) => setValues((c) => ({ ...c, headful: checked }))}
              />
            </div>

            <div className="form-actions">
              <button className="button button--primary" type="submit" disabled={Boolean(activeJob) || submitting}>
                {submitting ? "Starting..." : "Generate Script Options"}
              </button>
            </div>
          </div>
        </form>
      </section>

      <section className="panel dashboard__library" id="runs">
        <div className="dashboard__library-head">
          <h3 className="dashboard__panel-title">Runs</h3>
          <button className="button button--secondary" type="button" onClick={onRefresh}>
            Refresh
          </button>
        </div>

        {runs.length ? (
          <div className="run-table">
            {runs.map((run) => (
              <CompactRunRow
                key={run.runId}
                run={run}
                activeJob={activeJob}
                classificationEnabled={classificationEnabled}
                openRun={openRun}
                onPrepareFrames={onPrepareFrames}
                onResumeIdentification={onResumeIdentification}
                onComposeRun={onComposeRun}
                onDeleteRun={onDeleteRun}
              />
            ))}
          </div>
        ) : (
          <EmptyBlock title="No runs" />
        )}
      </section>

    </div>
  );
}

function CompactRunRow({
  run,
  activeJob,
  classificationEnabled,
  openRun,
  onPrepareFrames,
  onResumeIdentification,
  onComposeRun,
  onDeleteRun,
}) {
  return (
    <article className="run-row">
      <button className="run-row__main" type="button" onClick={() => openRun(run.runId)}>
        <div className="run-row__header">
          <strong className="run-row__title">{run.listingTitle || run.runId}</strong>
          <span className="run-row__meta">
            {formatDate(run.createdAt)}
            {run.stockId ? ` | ${run.stockId}` : ""}
            {run.listingTitle ? ` | ${run.runId}` : ""}
          </span>
        </div>
        <RunPipelineDots pipeline={run.pipeline} />
        <div className="run-row__stats">
          <span>
            {run.pipeline?.render?.done
              ? run.voiceoverStatus === "failed"
                ? "video ready | voice retry"
                : run.hasVoiceover
                  ? "video + voice ready"
                  : "video ready"
              : (run.voiceoverDraft?.variants?.length ?? 0) > 0
                ? "scripts ready"
                : run.pipeline?.analyze?.done
                  ? "generating scripts"
                  : "processing"}
          </span>
          <span>{run.stats.downloads} clips</span>
          <span>{run.stats.frames} frames</span>
          <span>{run.stats.analyzed} AI</span>
          <span>{run.stats.planned} cut</span>
        </div>
      </button>

      <div className="run-row__actions">
        <button className="button button--secondary" type="button" onClick={() => openRun(run.runId)}>
          View
        </button>
        <button className="button button--danger" type="button" onClick={() => void onDeleteRun(run.runId)}>
          Del
        </button>
      </div>
    </article>
  );
}

function CompactRunDetailPage({
  selectedRun,
  selectedRunError,
  loadingRun,
  navigate,
  onResumeIdentification,
  onComposeRun,
  onPrepareFrames,
  onDeleteRun,
  onVoiceoverDraft,
  onVoiceoverApply,
  onRerenderEnding,
  classificationEnabled,
  voiceoverEnabled,
  activeJob,
}) {
  const [thumbTitle, setThumbTitle] = useState("");
  const [thumbSubtitle, setThumbSubtitle] = useState("");
  const [thumbReferenceImageDataUrl, setThumbReferenceImageDataUrl] = useState("");
  const [thumbSubmitting, setThumbSubmitting] = useState(false);
  const [thumbError, setThumbError] = useState("");
  const [thumbImageUrl, setThumbImageUrl] = useState("");
  const [thumbDraftReady, setThumbDraftReady] = useState(false);
  const thumbnailReferenceInputRef = useRef(null);

  useEffect(() => {
    if (!selectedRun) {
      setThumbDraftReady(false);
      return;
    }
    const draft = loadThumbnailDraft(selectedRun.runId);
    setThumbTitle(draft?.title ?? String(selectedRun.listingTitle ?? "").trim());
    setThumbSubtitle(draft?.subtitle ?? "");
    setThumbReferenceImageDataUrl(draft?.referenceImageDataUrl ?? "");
    setThumbSubmitting(false);
    setThumbError("");
    setThumbImageUrl(draft?.generatedImageUrl ?? "");
    setThumbDraftReady(true);
  }, [selectedRun?.runId]);

  useEffect(() => {
    if (!selectedRun?.runId || !thumbDraftReady) {
      return;
    }
    saveThumbnailDraft(selectedRun.runId, {
      title: thumbTitle,
      subtitle: thumbSubtitle,
      referenceImageDataUrl: thumbReferenceImageDataUrl,
      generatedImageUrl: thumbImageUrl,
    });
  }, [
    selectedRun?.runId,
    thumbDraftReady,
    thumbTitle,
    thumbSubtitle,
    thumbReferenceImageDataUrl,
    thumbImageUrl,
  ]);

  if (loadingRun) {
    return <LoadingState label="Loading" compact />;
  }

  if (selectedRunError) {
    return (
      <section className="panel">
        <InlineCallout tone="danger" title="Error" description={selectedRunError} />
      </section>
    );
  }

  if (!selectedRun) {
    return <EmptyState title="Not found" actionLabel="Studio" onAction={() => navigate("/workflow")} />;
  }

  const p = selectedRun.pipeline;
  const previewClips = selectedRun.videos.filter((v) => v.videoUrl).slice(0, 4);
  const sequenceItems = selectedRun.plan?.composition?.segments?.length
    ? selectedRun.plan.composition.segments
    : selectedRun.plan?.sequence ?? [];
  const heroVideoUrl = selectedRun.finalReelUrl || previewClips[0]?.videoUrl || null;
  const heroLabel = selectedRun.finalReelUrl ? "Final Reel" : "Preview";
  const autoPrice = String(selectedRun.listingPrice ?? "").trim() || "AU ";

  async function handleReferenceImageChange(event) {
    const file = event.target.files?.[0];
    if (!file) {
      setThumbReferenceImageDataUrl("");
      return;
    }
    if (!String(file.type).startsWith("image/")) {
      setThumbError("Please choose an image file.");
      setThumbReferenceImageDataUrl("");
      return;
    }
    try {
      const dataUrl = await fileToDataUrl(file);
      setThumbError("");
      setThumbReferenceImageDataUrl(dataUrl);
      setThumbImageUrl("");
    } catch {
      setThumbError("Could not read the selected image.");
      setThumbReferenceImageDataUrl("");
    }
  }

  async function handleGenerateThumbnail(event) {
    event.preventDefault();
    if (thumbSubmitting) {
      return;
    }
    if (!thumbReferenceImageDataUrl) {
      setThumbError("Reference image is required.");
      return;
    }
    const preparedTitle = String(thumbTitle ?? "").trim();
    const preparedSubtitle = String(thumbSubtitle ?? "").trim();
    if (!preparedTitle) {
      setThumbError("Title is required.");
      return;
    }
    if (!preparedSubtitle) {
      setThumbError("Subtitle is required.");
      return;
    }

    setThumbSubmitting(true);
    setThumbError("");
    try {
      const generated = await fetchJson(`/api/runs/${encodeURIComponent(selectedRun.runId)}/thumbnail`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          title: preparedTitle,
          subtitle: preparedSubtitle,
          referenceImageDataUrl: thumbReferenceImageDataUrl,
        }),
      });
      setThumbImageUrl(withVersionToken(generated.imageUrl, Date.now()));
    } catch (error) {
      setThumbError(error.message);
    } finally {
      setThumbSubmitting(false);
    }
  }

  function handleThumbnailPreviewPick() {
    if (thumbSubmitting) {
      return;
    }
    thumbnailReferenceInputRef.current?.click();
  }

  const thumbnailDisplayImageUrl = thumbImageUrl || thumbReferenceImageDataUrl || "";
  const thumbnailDisplayLabel = thumbImageUrl ? "Generated thumbnail preview" : "Reference image preview";
  const thumbnailDownloadName = `${(
    String(thumbTitle || selectedRun.stockId || selectedRun.runId || "thumbnail")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/gu, "-")
      .replace(/^-+|-+$/gu, "") || "thumbnail"
  )}.png`;

  return (
    <div className="page-stack">
      <section className="panel detail-toolbar detail-toolbar--compact">
        <div className="detail-toolbar__copy">
          <h3 className="dashboard__panel-title">{selectedRun.listingTitle || selectedRun.runId}</h3>
          <p className="detail-toolbar__meta">
            {selectedRun.stockId ? `${selectedRun.stockId} | ` : ""}
            {formatDate(selectedRun.createdAt)}
          </p>
        </div>

        <div className="detail-actions">
          <button className="button button--secondary" type="button" onClick={() => navigate("/workflow")}>
            Back
          </button>

          {p?.download?.done && !p?.frames?.done ? (
            <button
              className="button button--primary"
              type="button"
              disabled={Boolean(activeJob)}
              onClick={() => void onPrepareFrames(selectedRun.runId)}
            >
              Continue Prep
            </button>
          ) : null}

          {p?.frames?.done && !p?.analyze?.done ? (
            <button
              className="button button--primary"
              type="button"
              disabled={!classificationEnabled || Boolean(activeJob)}
              onClick={() => void onResumeIdentification(selectedRun.runId)}
            >
              Continue AI
            </button>
          ) : null}

          {selectedRun.finalReelUrl ? (
            <a className="button button--ghost" href={selectedRun.finalReelUrl} target="_blank" rel="noreferrer">
              Open
            </a>
          ) : null}

          <button className="button button--danger" type="button" onClick={() => void onDeleteRun(selectedRun.runId)}>
            Delete
          </button>
        </div>
      </section>

      <section className="panel detail-overview">
          <div className="detail-overview__media">
          <p className="detail-overview__label">{heroLabel}</p>
          {heroVideoUrl ? (
            <div className="preview-frame preview-frame--hero">
              <VideoPlayer src={heroVideoUrl} title={selectedRun.listingTitle || selectedRun.runId} />
            </div>
          ) : (
            <div className="detail-preview-empty">
              <span>No preview</span>
            </div>
          )}
        </div>

        <div className="detail-overview__sidebar">
          <div className="detail-overview__header">
            <div>
              <p className="run-card__eyebrow">{selectedRun.stockId ? `Stock ${selectedRun.stockId}` : "Run"}</p>
              <h3>{selectedRun.listingTitle || selectedRun.runId}</h3>
              <p className="detail-run-id">{formatDate(selectedRun.createdAt)}</p>
              {selectedRun.totalReelDurationSeconds != null ? (
                <p className="detail-date">
                  {selectedRun.totalReelDurationSeconds}s total
                  {selectedRun.mainMontageDurationSeconds != null && selectedRun.endSceneDurationSeconds != null
                    ? ` | ${selectedRun.mainMontageDurationSeconds}s main | ${selectedRun.endSceneDurationSeconds}s end`
                    : ""}
                </p>
              ) : null}
            </div>
            <RunPipelineDots pipeline={selectedRun.pipeline} />
          </div>

          <dl className="detail-summary-grid">
            <Metric value={selectedRun.stats.downloads} label="Clips" />
            <Metric value={selectedRun.stats.frames} label="Frames" />
            <Metric value={selectedRun.stats.analyzed} label="AI" />
            <Metric value={selectedRun.stats.planned} label="Cut" />
          </dl>

          {selectedRun.carDescription ? (
            <details className="run-detail__description" open>
              <summary>Description</summary>
              <p className="run-detail__description-body">{selectedRun.carDescription}</p>
            </details>
          ) : null}

          {selectedRun.hasVoiceover && selectedRun.voiceoverScript ? (
            <details className="run-detail__voiceover" open>
              <summary>Voice-over</summary>
              <p className="run-detail__voiceover-body">{selectedRun.voiceoverScript}</p>
            </details>
          ) : null}
        </div>
      </section>

      <section className="panel thumbnail-generator">
        <div className="section-heading section-heading--compact">
          <h3 className="section-heading__title section-heading__title--panel">Photo Generator</h3>
        </div>
        <div className="thumbnail-generator__grid">
          <form className="thumbnail-generator__form" onSubmit={handleGenerateThumbnail}>
            <input
              ref={thumbnailReferenceInputRef}
              type="file"
              accept="image/*"
              tabIndex={-1}
              className="thumbnail-generator__file-input"
              onChange={(event) => {
                void handleReferenceImageChange(event);
              }}
            />
            <label className="field">
              <span className="field__label">Title</span>
              <input
                className="field__input"
                type="text"
                autoComplete="off"
                value={thumbTitle}
                onChange={(event) => setThumbTitle(event.target.value)}
                placeholder="2021 Toyota Hiace DX"
              />
            </label>
            <label className="field">
              <span className="field__label">Subtitle</span>
              <input
                className="field__input"
                type="text"
                autoComplete="off"
                value={thumbSubtitle}
                onChange={(event) => setThumbSubtitle(event.target.value)}
                placeholder="2.8L Turbo Diesel | 4WD"
              />
            </label>
            <label className="field">
              <span className="field__label">Price (Auto)</span>
              <input className="field__input" type="text" value={autoPrice} readOnly />
            </label>
            {thumbError ? <InlineCallout tone="danger" title="Error" description={thumbError} /> : null}
            <div className="form-actions">
              <button className="button button--primary" type="submit" disabled={thumbSubmitting}>
                {thumbSubmitting ? "Generating..." : "Generate Thumbnail"}
              </button>
            </div>
          </form>

          <div className={`thumbnail-generator__preview-card${thumbSubmitting ? " is-generating" : ""}`}>
            <button
              type="button"
              className="thumbnail-generator__preview-stage thumbnail-generator__preview-stage--pick"
              onClick={handleThumbnailPreviewPick}
              disabled={thumbSubmitting}
            >
              {thumbnailDisplayImageUrl ? (
                <img src={thumbnailDisplayImageUrl} alt={thumbnailDisplayLabel} />
              ) : (
                <div className="thumbnail-generator__placeholder">
                  <strong>Choose reference image</strong>
                  <span>Click here to select the car photo.</span>
                </div>
              )}
              {thumbSubmitting ? (
                <div className="thumbnail-generator__processing">
                  <span className="thumbnail-generator__processing-darken" aria-hidden="true" />
                  <span className="thumbnail-generator__processing-liquid thumbnail-generator__processing-liquid--a" aria-hidden="true" />
                  <span className="thumbnail-generator__processing-liquid thumbnail-generator__processing-liquid--b" aria-hidden="true" />
                  <span className="thumbnail-generator__processing-glow" aria-hidden="true" />
                  <span className="thumbnail-generator__processing-scan" aria-hidden="true" />
                  <span className="thumbnail-generator__processing-text">Generating thumbnail...</span>
                </div>
              ) : null}
            </button>
            <div className="thumbnail-generator__preview-floating-actions">
              {thumbImageUrl ? (
                <a className="button button--secondary" href={thumbImageUrl} download={thumbnailDownloadName}>
                  Download Image
                </a>
              ) : null}
              <button
                className="button button--ghost"
                type="button"
                disabled={thumbSubmitting}
                onClick={handleThumbnailPreviewPick}
              >
                Choose New Image
              </button>
            </div>
          </div>
        </div>
      </section>

      {selectedRun.voiceoverStatus === "failed" ? (
        <section className="panel">
          <InlineCallout
            tone="warning"
            title="Voice-over failed. The reel stayed silent."
            description={buildVoiceoverFailureMessage(selectedRun)}
          />
        </section>
      ) : null}

      <VoiceoverScriptPanel
        key={selectedRun.runId}
        run={selectedRun}
        activeJob={activeJob}
        classificationEnabled={classificationEnabled}
        voiceoverEnabled={voiceoverEnabled}
        onVoiceoverDraft={onVoiceoverDraft}
        onVoiceoverApply={onVoiceoverApply}
        onComposeRun={onComposeRun}
      />

      {previewClips.length ? (
        <section className="panel">
          <h3 className="section-heading__title section-heading__title--panel">Source</h3>
          <div className="source-preview-grid">
            {previewClips.map((v) => (
              <div key={v.clipId} className="source-preview-tile">
                <VideoPlayer
                  src={v.videoUrl}
                  title={v.title || v.clipId}
                  compact
                  className="video-player--tile"
                />
                <span className="source-preview-tile__id">{v.clipId}</span>
              </div>
            ))}
          </div>
        </section>
      ) : null}

      <section className="panel">
        <div className="section-heading section-heading--compact">
          <h3 className="section-heading__title">Sequence</h3>
        </div>

        {sequenceItems.length ? (
          <div className="sequence-grid">
            {sequenceItems.map((item, index) => (
              <article key={`${item.purpose || item.role}-${item.clipId}-${index}`} className="sequence-card">
                <div className="sequence-card__header">
                  <span className="role-pill">{roleLabel(item.purpose || item.role)}</span>
                  <span className="sequence-card__label">{formatShotLabel(item.analysis?.primaryLabel ?? item.primaryLabel)}</span>
                </div>
                <h4>{item.title || item.clipId}</h4>
                {item.frameUrl ? (
                  <a className="sequence-card__frame" href={item.frameUrl} target="_blank" rel="noreferrer">
                    <img src={item.frameUrl} alt={item.title || item.clipId} />
                  </a>
                ) : (
                  <div className="sequence-card__frame sequence-card__frame--empty">
                    <span>-</span>
                  </div>
                )}
                <div className="sequence-card__actions">
                  {item.videoUrl ? (
                    <a className="button button--ghost" href={item.videoUrl} target="_blank" rel="noreferrer">
                      Video
                    </a>
                  ) : null}
                  {item.frameUrl ? (
                    <a className="button button--secondary" href={item.frameUrl} target="_blank" rel="noreferrer">
                      Frame
                    </a>
                  ) : null}
                </div>
              </article>
            ))}
          </div>
        ) : (
          <EmptyBlock title="No sequence" />
        )}
      </section>

      <section className="panel">
        <div className="section-heading section-heading--compact">
          <h3 className="section-heading__title">Clips</h3>
        </div>

        {selectedRun.videos.length ? (
          <div className="clip-grid">
            {selectedRun.videos.map((video) => (
              <article key={video.clipId} className="clip-card">
                <div className="clip-card__media">
                  {video.frameUrl ? (
                    <a href={video.frameUrl} target="_blank" rel="noreferrer">
                      <img src={video.frameUrl} alt={video.title || video.clipId} loading="lazy" />
                    </a>
                  ) : (
                    <div className="clip-card__empty">-</div>
                  )}
                </div>

                <div className="clip-card__body">
                  <div className="clip-card__header">
                    <h4>{video.title || video.clipId}</h4>
                    <span className="clip-label">{formatShotLabel(video.analysis?.primaryLabel)}</span>
                  </div>

                  <div className="clip-card__actions">
                    {video.videoUrl ? (
                      <a className="button button--ghost" href={video.videoUrl} target="_blank" rel="noreferrer">
                        Video
                      </a>
                    ) : null}
                    {video.frameUrl ? (
                      <a className="button button--secondary" href={video.frameUrl} target="_blank" rel="noreferrer">
                        Frame
                      </a>
                    ) : null}
                  </div>
                </div>
              </article>
            ))}
          </div>
        ) : (
          <EmptyBlock title="No clips" />
        )}
      </section>
    </div>
  );
}

const VideoPlayer = React.memo(function VideoPlayer({
  src,
  title = "Video",
  compact = false,
  className = "",
}) {
  const containerRef = useRef(null);
  const videoRef = useRef(null);
  const [playing, setPlaying] = useState(false);
  const [muted, setMuted] = useState(false);
  const [duration, setDuration] = useState(0);
  const [currentTime, setCurrentTime] = useState(0);
  const [volume, setVolume] = useState(1);
  const [waiting, setWaiting] = useState(true);
  const [error, setError] = useState("");
  const [fullscreen, setFullscreen] = useState(false);

  useEffect(() => {
    setPlaying(false);
    setCurrentTime(0);
    setDuration(0);
    setWaiting(Boolean(src));
    setError("");
  }, [src]);

  useEffect(() => {
    const video = videoRef.current;
    if (!video) {
      return undefined;
    }

    const syncState = () => {
      setPlaying(!video.paused && !video.ended);
      setMuted(video.muted);
      setVolume(video.volume);
      setCurrentTime(Number.isFinite(video.currentTime) ? video.currentTime : 0);
      setDuration(Number.isFinite(video.duration) ? video.duration : 0);
    };

    const handleLoaded = () => {
      syncState();
      setWaiting(false);
      setError("");
    };

    const handlePlay = () => {
      syncState();
      setWaiting(false);
    };

    const handlePause = () => {
      syncState();
    };

    const handleWaiting = () => {
      setWaiting(true);
    };

    const handlePlaying = () => {
      syncState();
      setWaiting(false);
    };

    const handleError = () => {
      setError("Unable to load video.");
      setWaiting(false);
      setPlaying(false);
    };

    syncState();

    video.addEventListener("loadedmetadata", handleLoaded);
    video.addEventListener("loadeddata", handleLoaded);
    video.addEventListener("durationchange", syncState);
    video.addEventListener("timeupdate", syncState);
    video.addEventListener("volumechange", syncState);
    video.addEventListener("play", handlePlay);
    video.addEventListener("pause", handlePause);
    video.addEventListener("ended", handlePause);
    video.addEventListener("waiting", handleWaiting);
    video.addEventListener("playing", handlePlaying);
    video.addEventListener("canplay", handleLoaded);
    video.addEventListener("error", handleError);

    return () => {
      video.removeEventListener("loadedmetadata", handleLoaded);
      video.removeEventListener("loadeddata", handleLoaded);
      video.removeEventListener("durationchange", syncState);
      video.removeEventListener("timeupdate", syncState);
      video.removeEventListener("volumechange", syncState);
      video.removeEventListener("play", handlePlay);
      video.removeEventListener("pause", handlePause);
      video.removeEventListener("ended", handlePause);
      video.removeEventListener("waiting", handleWaiting);
      video.removeEventListener("playing", handlePlaying);
      video.removeEventListener("canplay", handleLoaded);
      video.removeEventListener("error", handleError);
    };
  }, [src]);

  useEffect(() => {
    const handleFullscreenChange = () => {
      setFullscreen(document.fullscreenElement === containerRef.current);
    };

    document.addEventListener("fullscreenchange", handleFullscreenChange);
    return () => document.removeEventListener("fullscreenchange", handleFullscreenChange);
  }, []);

  async function togglePlay() {
    const video = videoRef.current;
    if (!video) {
      return;
    }

    if (video.paused) {
      try {
        await video.play();
      } catch {
        setError("Playback was blocked.");
      }
      return;
    }

    video.pause();
  }

  function handleSeek(nextTime) {
    const video = videoRef.current;
    if (!video) {
      return;
    }

    video.currentTime = nextTime;
    setCurrentTime(nextTime);
  }

  function handleVolumeChange(nextVolume) {
    const video = videoRef.current;
    if (!video) {
      return;
    }

    video.volume = nextVolume;
    video.muted = nextVolume <= 0;
    setVolume(video.volume);
    setMuted(video.muted);
  }

  function toggleMute() {
    const video = videoRef.current;
    if (!video) {
      return;
    }

    const nextMuted = !video.muted;
    video.muted = nextMuted;
    if (!nextMuted && video.volume <= 0) {
      video.volume = 1;
    }
    setMuted(video.muted);
    setVolume(video.volume);
  }

  function restart() {
    const video = videoRef.current;
    if (!video) {
      return;
    }

    video.currentTime = 0;
    setCurrentTime(0);
  }

  async function toggleFullscreen() {
    const container = containerRef.current;
    if (!container) {
      return;
    }

    try {
      if (document.fullscreenElement === container) {
        await document.exitFullscreen();
        return;
      }
      await container.requestFullscreen();
    } catch {
      setError("Fullscreen unavailable.");
    }
  }

  const durationText = formatMediaTime(duration);
  const currentTimeText = formatMediaTime(currentTime);
  const progressMax = Math.max(duration, 0.1);
  const downloadName = useMemo(() => buildVideoDownloadName(src, title), [src, title]);
  const playerClassName = [`video-player`, compact ? "video-player--compact" : "", className]
    .filter(Boolean)
    .join(" ");

  return (
    <div className={playerClassName} ref={containerRef}>
      <div className="video-player__stage">
        <video
          ref={videoRef}
          className="video-player__media"
          preload="metadata"
          playsInline
          src={src}
          onDoubleClick={() => void toggleFullscreen()}
        />
        {waiting && !error ? <div className="video-player__status">Loading...</div> : null}
        {error ? <div className="video-player__status video-player__status--error">{error}</div> : null}
      </div>

      <div className="video-player__controls">
        <div className="video-player__row">
          <button type="button" className="video-player__button" onClick={() => void togglePlay()}>
            {playing ? "Pause" : "Play"}
          </button>
          <button type="button" className="video-player__button" onClick={restart}>
            Restart
          </button>
          {src ? (
            <a className="video-player__button video-player__button--link" href={src} download={downloadName}>
              Download
            </a>
          ) : null}
          <span className="video-player__time">
            {currentTimeText} / {durationText}
          </span>
          <button type="button" className="video-player__button" onClick={toggleMute}>
            {muted || volume <= 0 ? "Unmute" : "Mute"}
          </button>
          {!compact ? (
            <input
              className="video-player__volume"
              type="range"
              min="0"
              max="1"
              step="0.05"
              value={muted ? 0 : volume}
              onChange={(event) => handleVolumeChange(Number(event.target.value))}
              aria-label={`Volume for ${title}`}
            />
          ) : null}
          <button type="button" className="video-player__button" onClick={() => void toggleFullscreen()}>
            {fullscreen ? "Exit Full" : "Full"}
          </button>
        </div>

        <input
          className="video-player__timeline"
          type="range"
          min="0"
          max={progressMax}
          step="0.1"
          value={Math.min(currentTime, progressMax)}
          onChange={(event) => handleSeek(Number(event.target.value))}
          aria-label={`Timeline for ${title}`}
        />
      </div>
    </div>
  );
});

function formatMediaTime(value) {
  if (!Number.isFinite(value) || value <= 0) {
    return "0:00";
  }

  const totalSeconds = Math.floor(value);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (hours > 0) {
    return `${hours}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
  }

  return `${minutes}:${String(seconds).padStart(2, "0")}`;
}

function buildVideoDownloadName(src, title) {
  const safeTitle = String(title ?? "video")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "") || "video";

  let extension = "webm";

  try {
    const parsed = new URL(String(src), window.location.origin);
    const filePath = parsed.searchParams.get("path") ?? parsed.pathname;
    const match = /\.([a-z0-9]{2,5})(?:$|\?)/iu.exec(filePath);
    if (match) {
      extension = match[1].toLowerCase();
    }
  } catch {
    // Fall back to the default extension when the source can't be parsed.
  }

  return `${safeTitle}.${extension}`;
}

function Metric({ value, label, tone = "default" }) {
  return (
    <div className={`metric metric--${tone}`}>
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  );
}

function StatPair({ label, value }) {
  return (
    <div className="stat-pair">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function ToggleField({ label, description, checked, onChange }) {
  return (
    <label className="toggle-field">
      <span className="toggle-field__copy">
        <strong>{label}</strong>
        {description ? <span>{description}</span> : null}
      </span>
      <span className={`toggle${checked ? " is-on" : ""}`}>
        <input type="checkbox" checked={checked} onChange={(event) => onChange(event.target.checked)} />
        <span className="toggle__track">
          <span className="toggle__thumb" />
        </span>
      </span>
    </label>
  );
}

function InlineCallout({ tone, title, description, action = null }) {
  return (
    <div className={`callout callout--${tone}`}>
      <div className="callout__copy">
        <strong>{title}</strong>
        {description ? <p>{description}</p> : null}
      </div>
      {action ? <div className="callout__action">{action}</div> : null}
    </div>
  );
}

function EmptyState({ title, description, actionLabel, onAction }) {
  return (
    <section className="panel panel--empty">
      <div className="empty-state">
        <h3>{title}</h3>
        {description ? <p>{description}</p> : null}
        <button className="button button--primary" type="button" onClick={onAction}>
          {actionLabel}
        </button>
      </div>
    </section>
  );
}

function EmptyBlock({ title, description }) {
  return (
    <div className="empty-block">
      <strong>{title}</strong>
      {description ? <p>{description}</p> : null}
    </div>
  );
}

function LoadingState({ label, compact = false }) {
  return (
    <section className={`loading-state${compact ? " loading-state--compact" : ""}`}>
      <span className="loading-state__spinner" aria-hidden="true" />
      <p>{label}</p>
    </section>
  );
}

function fileToDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result ?? ""));
    reader.onerror = () => reject(new Error("Failed to read file."));
    reader.readAsDataURL(file);
  });
}

function StatusChip({ tone, label }) {
  return <span className={`status-chip status-chip--${tone}`}>{label}</span>;
}

function parseRoute(pathname) {
  const normalizedPath = String(pathname || "/").replace(/\/+$/u, "") || "/";

  // Match run detail robustly, including nested mount paths such as /workflow/runs/:id.
  const runMatch = /(?:^|\/)runs\/([^/]+)$/u.exec(normalizedPath);
  if (runMatch) {
    return {
      page: "run-detail",
      runId: decodeURIComponent(runMatch[1]),
    };
  }

  if (
    normalizedPath === "/" ||
    /(?:^|\/)(workflow|activity|runs)$/u.test(normalizedPath)
  ) {
    return { page: "dashboard" };
  }

  return { page: "not-found" };
}

function menuKeyForRoute(route) {
  if (route.page === "run-detail") {
    return "/runs";
  }

  if (route.page === "dashboard") {
    return "/workflow";
  }

  return "/workflow";
}

function statusTone(status) {
  if (status === "completed") {
    return "success";
  }
  if (status === "failed") {
    return "danger";
  }
  if (status === "running") {
    return "warning";
  }
  return "default";
}

function commandLabel(command) {
  if (command === "download") {
    return "Download Only";
  }
  if (command === "prepare") {
    return "Prepare Assets";
  }
  if (command === "compose") {
    return "Build Video";
  }
  if (command === "end-scene-rerender") {
    return "Rebuild End Scene";
  }
  if (command === "run") {
    return "Script Prep";
  }
  if (command === "voiceover-draft") {
    return "VO Scripts";
  }
  if (command === "voiceover-apply") {
    return "VO Stitch";
  }
  return command;
}

function commandButtonLabel(command) {
  if (command === "download") {
    return "Download footage";
  }
  if (command === "prepare") {
    return "Prepare assets";
  }
  if (command === "run") {
    return "Generate script options";
  }
  return "Start reel";
}

function formatRendererLabel(renderer) {
  if (renderer === "browser_animation") {
    return "Browser animation";
  }
  if (renderer === "ffmpeg_ass") {
    return "FFmpeg fallback";
  }
  return renderer || "Unknown";
}

function roleLabel(role) {
  if (role === "front_exterior") {
    return "Front Exterior";
  }
  if (role === "driver_door_interior_reveal") {
    return "Driver Door / Reveal";
  }
  if (role === "interior" || role === "interior_detail") {
    return "Interior / Odometer";
  }
  if (role === "rear_exterior") {
    return "Backside Exterior";
  }
  return formatShotLabel(role);
}

function formatShotLabel(label) {
  if (!label) {
    return "Unclassified";
  }

  return String(label)
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function formatJobSourceSummary(job) {
  if (!job) {
    return "-";
  }
  if (job.resumeRunId && !job.urls?.length) {
    return job.resumeRunId;
  }
  const lines = [];
  if (job.listingTitle || job.stockId) {
    const titlePart = job.listingTitle || "";
    const stockPart = job.stockId ? String(job.stockId) : "";
    const headline = [titlePart, stockPart].filter(Boolean).join(" | ");
    if (headline) {
      lines.push(headline);
    }
  }
  if (job.urls?.[0]) {
    lines.push(job.urls[0]);
  }
  if (job.carDescription) {
    const snippet = String(job.carDescription).trim().replace(/\s+/g, " ");
    lines.push(snippet.length > 120 ? `${snippet.slice(0, 117)}...` : snippet);
  }
  return lines.length ? lines.join("\n\n") : "-";
}

function buildVoiceoverFailureMessage(run) {
  const parts = [];
  if (run?.voiceoverLastAttemptAt) {
    parts.push(`Last attempt: ${formatDate(run.voiceoverLastAttemptAt)}.`);
  } else {
    parts.push("ElevenLabs could not finish after retries.");
  }
  parts.push(
    run?.voiceoverRetryable
      ? "You can retry voice-over stitching below."
      : "Check the configuration or script, then try voice-over stitching again.",
  );
  const lastError = String(run?.voiceoverLastError ?? "").trim().replace(/\s+/g, " ");
  if (lastError) {
    parts.push(`Last error: ${lastError.slice(0, 220)}${lastError.length > 220 ? "..." : ""}`);
  }
  return parts.join(" ");
}

function formatDate(value) {
  if (!value) {
    return "Unknown";
  }

  return dateFormatter.format(new Date(value));
}

function formatTime(value) {
  if (!value) {
    return "";
  }

  return timeFormatter.format(new Date(value));
}

function pageEyebrow(page) {
  if (page === "run-detail") {
    return "Run";
  }
  return "Studio";
}

function pageTitle(page) {
  if (page === "run-detail") {
    return "Run";
  }
  return "Studio";
}

function pageSummary(page, { activeJob, runsCount }) {
  if (page === "run-detail") {
    return "Run detail.";
  }

  return `${runsCount} run${runsCount === 1 ? "" : "s"}. ${activeJob ? "Busy." : "Ready."}`;
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  const data = await response.json();

  if (!response.ok) {
    throw new Error(data.error || "Request failed.");
  }

  return data;
}

function stampRunMediaUrls(run) {
  if (!run) {
    return run;
  }

  const stampItem = (item) => ({
    ...item,
    videoUrl: item?.videoUrl ?? null,
    frameUrl: item?.frameUrl ?? null,
  });

  return {
    ...run,
    finalReelUrl: withVersionToken(run.finalReelUrl, run.finalReelVersion),
    finalReelWebmUrl: withVersionToken(run.finalReelWebmUrl, run.finalReelWebmVersion),
    videos: Array.isArray(run.videos) ? run.videos.map(stampItem) : [],
    plan: run.plan
      ? {
          ...run.plan,
          sequence: Array.isArray(run.plan.sequence) ? run.plan.sequence.map(stampItem) : [],
          composition: run.plan.composition
            ? {
                ...run.plan.composition,
                segments: Array.isArray(run.plan.composition.segments)
                  ? run.plan.composition.segments.map(stampItem)
                  : [],
              }
            : null,
        }
      : null,
  };
}

function withVersionToken(url, version) {
  if (!url) {
    return url;
  }

  const base = String(url).replace(/[?&]v=[^&]+/u, "");
  if (!version) {
    return base;
  }

  const joiner = base.includes("?") ? "&" : "?";
  return `${base}${joiner}v=${encodeURIComponent(String(version))}`;
}

createRoot(document.getElementById("root")).render(<RootApp />);

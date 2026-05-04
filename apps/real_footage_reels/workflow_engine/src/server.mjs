import { createReadStream } from "node:fs";
import fs from "node:fs/promises";
import http from "node:http";
import path from "node:path";
import process from "node:process";
import { URL } from "node:url";

import {
  createRuntimeConfig,
  hasGeminiApiKey,
  hasVoiceoverEnv,
  loadEnvConfig,
} from "./lib/config.mjs";
import { ensureSampleEndScene } from "./lib/end-scene.mjs";
import { generateRunThumbnail } from "./lib/thumbnail/generator.mjs";
import { buildRunReport, listRunReports, toPublicFileUrl } from "./lib/run-report.mjs";
import { composeSavedRun, continueWorkflow, executeWorkflow, rerenderRunEndScene } from "./lib/workflow.mjs";
import { applyVoiceoverToReel, draftVoiceoverScripts } from "./lib/voiceover.mjs";

const rootDir = process.cwd();
const uiDir = path.join(rootDir, "ui");
const uiDistDir = path.join(uiDir, "dist");
const vehicleInventoryApiUrl = "https://www.cbs.s1.carbarn.com.au/carbarnau/api/v1/vehicles";
const vehicleInventoryCacheDir = path.join(rootDir, ".ui-cache");
const vehicleInventoryCachePath = path.join(vehicleInventoryCacheDir, "all_stock.json");
const jobs = new Map();
let activeJobId = null;
const vehicleInventoryCache = createVehicleInventoryCache();

const server = http.createServer(async (request, response) => {
  try {
    const requestUrl = new URL(request.url ?? "/", "http://127.0.0.1");
    const pathname = requestUrl.pathname;

    if (request.method === "GET" && pathname === "/app.js") {
      return serveStaticFile(
        response,
        path.join(uiDistDir, "app.js"),
        "text/javascript; charset=utf-8",
      );
    }

    if (request.method === "GET" && pathname === "/app.css") {
      return serveStaticFile(response, path.join(uiDistDir, "app.css"), "text/css; charset=utf-8");
    }

    if (pathname.startsWith("/api/vehicle-inventory")) {
      return handleVehicleInventoryApi(request, requestUrl, response);
    }

    if (
      request.method === "GET" &&
      !pathname.startsWith("/api/") &&
      !path.extname(pathname)
    ) {
      return serveStaticFile(response, path.join(uiDir, "index.html"), "text/html; charset=utf-8");
    }

    if (request.method === "GET" && pathname === "/api/meta") {
      const env = loadEnvConfig(rootDir);
      return sendJson(response, 200, {
        appName: "AU Real Footage Reels",
        activeJobId,
        features: {
          analysisEnabled: hasGeminiApiKey(env.GEMINI_API_KEY),
          voiceoverEnabled: hasVoiceoverEnv(env),
        },
      });
    }

    if (request.method === "GET" && pathname === "/api/end-scene/sample") {
      const env = loadEnvConfig(rootDir);
      const refresh = requestUrl.searchParams.get("refresh") === "1";
      const config = createRuntimeConfig(
        {
          command: "compose",
          urls: [],
          compose: true,
          headless: true,
        },
        env,
      );
      const sample = await ensureSampleEndScene(
        rootDir,
        config,
        () => {},
        { force: refresh },
      );
      return sendJson(response, 200, await buildEndSceneSamplePayload(sample));
    }

    if (request.method === "POST" && pathname === "/api/end-scene/sample") {
      if (activeJobId) {
        return sendJson(response, 409, {
          error: "Another workflow is already running.",
          activeJobId,
        });
      }

      const body = await readJsonBody(request);
      const env = loadEnvConfig(rootDir);
      const config = createRuntimeConfig(
        {
          command: "compose",
          urls: [],
          compose: true,
          headless: true,
        },
        env,
      );

      const listingTitle = normalizeListingText(body.listingTitle);
      const stockId = normalizeListingText(body.stockId);
      const listingPrice = normalizeListingText(body.listingPrice ?? body.price);
      const priceIncludes = normalizePriceIncludesBody(body.priceIncludes);
      const meta = {};

      if (listingTitle) {
        meta.listingTitle = listingTitle;
      }
      if (stockId) {
        meta.stockId = stockId;
      }
      if (listingPrice) {
        meta.listingPrice = listingPrice;
      }
      if (priceIncludes?.length) {
        meta.priceIncludes = priceIncludes;
      }

      const sample = await ensureSampleEndScene(
        rootDir,
        config,
        () => {},
        {
          force: body.force !== false,
          meta,
        },
      );

      return sendJson(response, 200, await buildEndSceneSamplePayload(sample));
    }

    if (request.method === "GET" && pathname === "/api/jobs") {
      return sendJson(response, 200, {
        activeJobId,
        jobs: [...jobs.values()].map(toPublicJob),
      });
    }

    if (request.method === "GET" && pathname.startsWith("/api/jobs/")) {
      const jobId = pathname.split("/").pop();
      const job = jobs.get(jobId);
      if (!job) {
        return sendJson(response, 404, { error: "Job not found." });
      }
      return sendJson(response, 200, toPublicJob(job));
    }

    if (request.method === "POST" && pathname === "/api/jobs") {
      if (activeJobId) {
        return sendJson(response, 409, {
          error: "Another workflow is already running.",
          activeJobId,
        });
      }

      const body = await readJsonBody(request);
      const urlParse = parseSingleAlbumUrl(body.urls ?? body.url);
      if (urlParse.error) {
        return sendJson(response, 400, { error: urlParse.error });
      }

      const listingTitle = normalizeListingText(body.listingTitle ?? body.title);
      const stockId = normalizeListingText(body.stockId);
      const carDescription = normalizeListingText(body.carDescription ?? body.description);
      const listingPrice = normalizeListingText(body.listingPrice ?? "");
      const priceIncludes = normalizePriceIncludesBody(body.priceIncludes);
      if (!listingTitle) {
        return sendJson(response, 400, { error: "Listing title is required." });
      }
      if (!stockId) {
        return sendJson(response, 400, { error: "Stock ID is required." });
      }
      if (!carDescription) {
        return sendJson(response, 400, { error: "Car description is required." });
      }

      const command = normalizeCommand(body.command);
      const requestedScriptApproval = command === "run"
        ? (typeof body.voiceoverScriptApproval === "boolean"
          ? body.voiceoverScriptApproval
          : Boolean(body.compose))
        : false;

      const job = createJob({
        command,
        urls: [urlParse.url],
        listingTitle,
        stockId,
        carDescription,
        listingPrice,
        priceIncludes,
        maxClips: normalizeOptionalNumber(body.maxClips),
        compose: command === "run" ? Boolean(body.compose) : false,
        headless: !Boolean(body.headful),
        voiceoverScriptApproval: requestedScriptApproval,
        sourcePayload: {
          url: urlParse.url,
          listingTitle,
          stockId,
          carDescription,
          listingPrice,
          priceIncludes,
          command,
          maxClips: normalizeOptionalNumber(body.maxClips),
          compose: command === "run" ? Boolean(body.compose) : false,
          headful: Boolean(body.headful),
          voiceoverScriptApproval: requestedScriptApproval,
        },
      });
      jobs.set(job.id, job);
      activeJobId = job.id;

      void runJob(job, body);

      return sendJson(response, 202, toPublicJob(job));
    }

    if (request.method === "GET" && pathname === "/api/runs") {
      const runs = await listRunReports(rootDir);
      return sendJson(response, 200, { runs });
    }

    const deleteRunMatch = /^\/api\/runs\/([^/]+)$/u.exec(pathname);
    if (request.method === "DELETE" && deleteRunMatch) {
      const runId = deleteRunMatch[1];
      const runDir = path.join(rootDir, "runs", runId);
      try {
        await fs.access(runDir);
      } catch {
        return sendJson(response, 404, { error: "Run not found." });
      }
      await fs.rm(runDir, { recursive: true, force: true });
      return sendJson(response, 200, { ok: true, runId });
    }

    const prepareRunMatch = /^\/api\/runs\/([^/]+)\/prepare$/u.exec(pathname);
    if (request.method === "POST" && prepareRunMatch) {
      if (activeJobId) {
        return sendJson(response, 409, {
          error: "Another workflow is already running.",
          activeJobId,
        });
      }

      const runId = prepareRunMatch[1];
      const runDir = path.join(rootDir, "runs", runId);
      const existingReport = await buildRunReport(runDir, rootDir);

      if (!existingReport) {
        return sendJson(response, 404, { error: "Run not found." });
      }

      const body = await readJsonBody(request);
      const job = createJob({
        command: "prepare",
        urls: [],
        listingTitle: "",
        stockId: "",
        maxClips: null,
        compose: false,
        headless: !Boolean(body.headful),
        resumeRunId: runId,
      });
      jobs.set(job.id, job);
      activeJobId = job.id;

      void runJob(job, body);

      return sendJson(response, 202, toPublicJob(job));
    }

    const getRunMatch = /^\/api\/runs\/([^/]+)$/u.exec(pathname);
    if (request.method === "GET" && getRunMatch) {
      const runId = getRunMatch[1];
      const runDir = path.join(rootDir, "runs", runId);
      const report = await buildRunReport(runDir, rootDir);

      if (!report) {
        return sendJson(response, 404, { error: "Run not found." });
      }

      return sendJson(response, 200, report);
    }

    const thumbnailMatch = /^\/api\/runs\/([^/]+)\/thumbnail$/u.exec(pathname);
    if (request.method === "POST" && thumbnailMatch) {
      const runId = thumbnailMatch[1];
      const runDir = path.join(rootDir, "runs", runId);
      const existingReport = await buildRunReport(runDir, rootDir);
      if (!existingReport) {
        return sendJson(response, 404, { error: "Run not found." });
      }

      const env = loadEnvConfig(rootDir);
      if (!hasGeminiApiKey(env.GEMINI_API_KEY)) {
        return sendJson(response, 400, { error: "GEMINI_API_KEY is required for thumbnails." });
      }

      const body = await readJsonBody(request);
      const title = normalizeListingText(body.title);
      const subtitle = normalizeListingText(body.subtitle);
      const referenceImageDataUrl = String(body.referenceImageDataUrl ?? "").trim();

      if (!title) {
        return sendJson(response, 400, { error: "title is required." });
      }
      if (!subtitle) {
        return sendJson(response, 400, { error: "subtitle is required." });
      }
      if (!referenceImageDataUrl) {
        return sendJson(response, 400, { error: "referenceImageDataUrl is required." });
      }

      const generated = await generateRunThumbnail({
        runDir,
        geminiApiKey: env.GEMINI_API_KEY,
        imageModel: env.THUMBNAIL_GEMINI_MODEL || env.GEMINI_IMAGE_MODEL || "",
        referenceImageDataUrl,
        title,
        subtitle,
        price: normalizeListingText(existingReport.listingPrice || "") || "AU ",
      });

      return sendJson(response, 200, {
        runId,
        imageUrl: toPublicFileUrl(generated.imagePath, rootDir),
        mimeType: generated.imageMimeType,
      });
    }

    if (request.method === "POST" && pathname.startsWith("/api/runs/") && pathname.endsWith("/identify")) {
      if (activeJobId) {
        return sendJson(response, 409, {
          error: "Another workflow is already running.",
          activeJobId,
        });
      }

      const body = await readJsonBody(request);
      const runId = pathname.split("/")[3];
      const runDir = path.join(rootDir, "runs", runId);
      const existingReport = await buildRunReport(runDir, rootDir);

      if (!existingReport) {
        return sendJson(response, 404, { error: "Run not found." });
      }

      const job = createJob({
        command: "run",
        urls: [],
        listingTitle: existingReport.listingTitle ?? "",
        stockId: existingReport.stockId ?? "",
        carDescription: existingReport.carDescription ?? "",
        listingPrice: existingReport.listingPrice ?? "",
        priceIncludes: existingReport.priceIncludes?.length ? existingReport.priceIncludes : null,
        maxClips: null,
        compose: Boolean(body.compose),
        headless: !Boolean(body.headful),
        resumeRunId: runId,
        voiceoverScriptApproval: Boolean(body.compose),
      });
      jobs.set(job.id, job);
      activeJobId = job.id;

      void runJob(job, body);

      return sendJson(response, 202, toPublicJob(job));
    }

    const voiceoverDraftMatch = /^\/api\/runs\/([^/]+)\/voiceover\/draft$/u.exec(pathname);
    if (request.method === "POST" && voiceoverDraftMatch) {
      if (activeJobId) {
        return sendJson(response, 409, {
          error: "Another workflow is already running.",
          activeJobId,
        });
      }

      const runId = voiceoverDraftMatch[1];
      const runDir = path.join(rootDir, "runs", runId);
      const existingReport = await buildRunReport(runDir, rootDir);

      if (!existingReport) {
        return sendJson(response, 404, { error: "Run not found." });
      }
      const env = loadEnvConfig(rootDir);
      if (!hasGeminiApiKey(env.GEMINI_API_KEY)) {
        return sendJson(response, 400, { error: "GEMINI_API_KEY is required for script drafts." });
      }

      const job = createJob({
        command: "voiceover-draft",
        urls: [],
        listingTitle: existingReport.listingTitle ?? "",
        stockId: existingReport.stockId ?? "",
        carDescription: existingReport.carDescription ?? "",
        listingPrice: existingReport.listingPrice ?? "",
        priceIncludes: existingReport.priceIncludes?.length ? existingReport.priceIncludes : null,
        maxClips: null,
        compose: true,
        headless: true,
        resumeRunId: runId,
      });
      jobs.set(job.id, job);
      activeJobId = job.id;

      void runJob(job, {});

      return sendJson(response, 202, toPublicJob(job));
    }

    const voiceoverApplyMatch = /^\/api\/runs\/([^/]+)\/voiceover\/apply$/u.exec(pathname);
    if (request.method === "POST" && voiceoverApplyMatch) {
      if (activeJobId) {
        return sendJson(response, 409, {
          error: "Another workflow is already running.",
          activeJobId,
        });
      }

      const runId = voiceoverApplyMatch[1];
      const runDir = path.join(rootDir, "runs", runId);
      const existingReport = await buildRunReport(runDir, rootDir);

      if (!existingReport) {
        return sendJson(response, 404, { error: "Run not found." });
      }
      if (!existingReport.pipeline?.render?.done) {
        return sendJson(response, 400, { error: "Render the final reel first." });
      }

      const env = loadEnvConfig(rootDir);
      if (!hasVoiceoverEnv(env)) {
        return sendJson(response, 400, {
          error: "ElevenLabs is not configured. Set ELEVEN_LABS_API_KEY and ELEVENLAB_VOICE_ID in .env.",
        });
      }

      const body = await readJsonBody(request);
      const script = String(body.script ?? "").trim();
      if (!script) {
        return sendJson(response, 400, { error: "script (string) is required in the JSON body." });
      }

      const job = createJob({
        command: "voiceover-apply",
        urls: [],
        listingTitle: existingReport.listingTitle ?? "",
        stockId: existingReport.stockId ?? "",
        carDescription: existingReport.carDescription ?? "",
        listingPrice: existingReport.listingPrice ?? "",
        priceIncludes: existingReport.priceIncludes?.length ? existingReport.priceIncludes : null,
        maxClips: null,
        compose: true,
        headless: true,
        resumeRunId: runId,
        approvedScript: script,
      });
      jobs.set(job.id, job);
      activeJobId = job.id;

      void runJob(job, body);

      return sendJson(response, 202, toPublicJob(job));
    }

    if (request.method === "POST" && pathname.startsWith("/api/runs/") && pathname.endsWith("/compose")) {
      if (activeJobId) {
        return sendJson(response, 409, {
          error: "Another workflow is already running.",
          activeJobId,
        });
      }

      const runId = pathname.split("/")[3];
      const runDir = path.join(rootDir, "runs", runId);
      const existingReport = await buildRunReport(runDir, rootDir);

      if (!existingReport) {
        return sendJson(response, 404, { error: "Run not found." });
      }

      const body = await readJsonBody(request);
      const approvedScript = String(body.script ?? "").trim();
      const env = loadEnvConfig(rootDir);

      if (approvedScript && !hasVoiceoverEnv(env)) {
        return sendJson(response, 400, {
          error: "ElevenLabs is not configured. Set ELEVEN_LABS_API_KEY and ELEVENLAB_VOICE_ID in .env.",
        });
      }

      const job = createJob({
        command: "compose",
        urls: [],
        listingTitle: existingReport.listingTitle ?? "",
        stockId: existingReport.stockId ?? "",
        carDescription: existingReport.carDescription ?? "",
        listingPrice: existingReport.listingPrice ?? "",
        priceIncludes: existingReport.priceIncludes?.length ? existingReport.priceIncludes : null,
        maxClips: null,
        compose: true,
        headless: true,
        resumeRunId: runId,
        voiceoverScriptApproval: !approvedScript,
        approvedScript,
      });
      jobs.set(job.id, job);
      activeJobId = job.id;

      void runJob(job, body);

      return sendJson(response, 202, toPublicJob(job));
    }

    const endSceneRerenderMatch = /^\/api\/runs\/([^/]+)\/end-scene$/u.exec(pathname);
    if (request.method === "POST" && endSceneRerenderMatch) {
      if (activeJobId) {
        return sendJson(response, 409, {
          error: "Another workflow is already running.",
          activeJobId,
        });
      }

      const runId = endSceneRerenderMatch[1];
      const runDir = path.join(rootDir, "runs", runId);
      const existingReport = await buildRunReport(runDir, rootDir);

      if (!existingReport) {
        return sendJson(response, 404, { error: "Run not found." });
      }
      if (!existingReport.pipeline?.render?.done) {
        return sendJson(response, 400, { error: "Build the reel first." });
      }

      const job = createJob({
        command: "end-scene-rerender",
        urls: [],
        listingTitle: existingReport.listingTitle ?? "",
        stockId: existingReport.stockId ?? "",
        carDescription: existingReport.carDescription ?? "",
        listingPrice: existingReport.listingPrice ?? "",
        priceIncludes: existingReport.priceIncludes?.length ? existingReport.priceIncludes : null,
        maxClips: null,
        compose: true,
        headless: true,
        resumeRunId: runId,
        voiceoverScriptApproval: true,
      });
      jobs.set(job.id, job);
      activeJobId = job.id;

      void runJob(job, {});

      return sendJson(response, 202, toPublicJob(job));
    }

    if (request.method === "GET" && pathname === "/api/file") {
      const relativePath = requestUrl.searchParams.get("path") || "";
      return serveWorkspaceFile(request, response, relativePath);
    }

    return sendJson(response, 404, { error: "Not found." });
  } catch (error) {
    return sendJson(response, 500, {
      error: error?.message ?? String(error),
    });
  }
});

const port = Number(process.env.PORT || 4173);

server.listen(port, () => {
  console.log(`UI server running at http://127.0.0.1:${port}`);
});

async function handleVehicleInventoryApi(request, requestUrl, response) {
  const pathname = requestUrl.pathname;

  if (request.method === "GET" && pathname === "/api/vehicle-inventory/status") {
    const status = vehicleInventoryCache.getStatus();
    return sendJson(response, 200, status);
  }

  if (request.method === "GET" && pathname === "/api/vehicle-inventory/search") {
    const query = String(requestUrl.searchParams.get("q") ?? "").trim();
    const limit = normalizeBoundedInteger(requestUrl.searchParams.get("limit"), 20, { min: 1, max: 50 });
    const result = await vehicleInventoryCache.search(query, { limit });
    return sendJson(response, 200, result);
  }

  if (request.method === "POST" && pathname === "/api/vehicle-inventory/refresh") {
    const kicked = vehicleInventoryCache.refresh();
    return sendJson(response, 202, {
      ok: true,
      refreshing: true,
      kicked,
    });
  }

  return sendJson(response, 404, { error: "Not found." });
}

async function runJob(job, body) {
  updateJob(job.id, {
    status: "running",
    startedAt: new Date().toISOString(),
  });

  const appendLog = (message) => {
    const nextLog = {
      at: new Date().toISOString(),
      message,
    };
    const current = jobs.get(job.id);
    current.logs.push(nextLog);
  };

  const reportProgress = (payload) => {
    updateJob(job.id, {
      progress: {
        ...payload,
        at: new Date().toISOString(),
      },
    });
  };

  const workflowHooks = { log: appendLog, onProgress: reportProgress };

  try {
    const requireResumeRunDir = (actionLabel) => {
      const runId = String(job.resumeRunId ?? "").trim();
      if (!runId) {
        throw new Error(`Cannot ${actionLabel}: missing runId for resumed job.`);
      }
      return path.join(rootDir, "runs", runId);
    };

    if (job.resumeRunId) {
      appendLog(`Resuming existing run: ${job.resumeRunId}`);
    }

    const env = loadEnvConfig(rootDir);

    if (job.command === "voiceover-draft" || job.command === "voiceover-apply") {
      const runDir = requireResumeRunDir(job.command === "voiceover-draft" ? "draft voice-over scripts" : "apply voice-over");
      const config = createRuntimeConfig(
        {
          command: "run",
          urls: [],
          listingTitle: job.listingTitle ?? "",
          stockId: job.stockId ?? "",
          carDescription: job.carDescription ?? "",
          listingPrice: job.listingPrice ?? "",
          priceIncludes: job.priceIncludes ?? null,
          maxClips: null,
          compose: true,
          headless: true,
          voiceoverScriptApproval: false,
        },
        env,
      );

      if (job.command === "voiceover-draft") {
        reportProgress({ phase: "voiceover", percent: 20, label: "Script drafts" });
        await draftVoiceoverScripts(runDir, config, appendLog);
      } else {
        const script = String(job.approvedScript ?? "").trim();
        if (!script) {
          throw new Error("Missing script text for voice-over.");
        }
        reportProgress({ phase: "voiceover", percent: 20, label: "Voice" });
        await applyVoiceoverToReel(runDir, config, appendLog, { approvedScript: script });
      }

      const report = await buildRunReport(runDir, rootDir);

      updateJob(job.id, {
        status: "completed",
        finishedAt: new Date().toISOString(),
        progress: null,
        result: {
          runId: job.resumeRunId,
          runDir,
          report,
        },
      });
    } else {
      const config = createRuntimeConfig(
        {
          command: job.command === "compose" ? "run" : job.command,
          urls: job.urls,
          listingTitle: job.listingTitle ?? "",
          stockId: job.stockId ?? "",
          carDescription: job.carDescription ?? "",
          listingPrice: job.listingPrice ?? "",
          priceIncludes: job.priceIncludes ?? null,
          maxClips: job.maxClips,
          outDir: job.resumeRunId ? path.join(rootDir, "runs", job.resumeRunId) : null,
          compose: job.compose,
          headless: job.headless,
          voiceoverScriptApproval: job.voiceoverScriptApproval ?? true,
        },
        env,
      );

      const composeRunDir = job.command === "compose"
        ? requireResumeRunDir("compose run")
        : null;
      const composeExistingReport = job.command === "compose" && composeRunDir
        ? await buildRunReport(composeRunDir, rootDir)
        : null;

      const result = job.command === "compose"
        ? composeExistingReport?.pipeline?.analyze?.done
          ? await composeSavedRun(composeRunDir, config, {
              ...workflowHooks,
              approvedScript: job.approvedScript,
            })
          : await continueWorkflow(composeRunDir, config, {
              ...workflowHooks,
              approvedScript: job.approvedScript,
            })
        : job.command === "end-scene-rerender"
          ? await rerenderRunEndScene(requireResumeRunDir("rerender end scene"), config, workflowHooks)
          : job.resumeRunId
            ? await continueWorkflow(requireResumeRunDir("continue existing run"), config, workflowHooks)
            : await executeWorkflow(config, workflowHooks);
      const report = await buildRunReport(result.runDir, rootDir);

      updateJob(job.id, {
        status: "completed",
        finishedAt: new Date().toISOString(),
        progress: null,
        result: {
          runId: path.basename(result.runDir),
          runDir: result.runDir,
          report,
        },
      });
    }
  } catch (error) {
    updateJob(job.id, {
      status: "failed",
      finishedAt: new Date().toISOString(),
      progress: null,
      error: error?.stack ?? String(error),
    });
  } finally {
    activeJobId = null;
  }
}

function createJob(input) {
  return {
    id: `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
    command: input.command,
    urls: input.urls,
    listingTitle: input.listingTitle ?? "",
    stockId: input.stockId ?? "",
    carDescription: input.carDescription ?? "",
    listingPrice: input.listingPrice ?? "",
    priceIncludes: input.priceIncludes ?? null,
    maxClips: input.maxClips,
    compose: input.compose,
    headless: input.headless,
    status: "queued",
    createdAt: new Date().toISOString(),
    startedAt: null,
    finishedAt: null,
    logs: [],
    result: null,
    error: null,
    resumeRunId: input.resumeRunId ?? null,
    sourcePayload: input.sourcePayload ?? null,
    progress: null,
    voiceoverScriptApproval: Boolean(input.voiceoverScriptApproval),
    approvedScript: typeof input.approvedScript === "string" ? input.approvedScript : null,
  };
}

function updateJob(jobId, patch) {
  const current = jobs.get(jobId);
  jobs.set(jobId, {
    ...current,
    ...patch,
  });
}

function toPublicJob(job) {
  return {
    id: job.id,
    command: job.command,
    urls: job.urls,
    listingTitle: job.listingTitle ?? "",
    stockId: job.stockId ?? "",
    carDescription: job.carDescription ?? "",
    listingPrice: job.listingPrice ?? "",
    priceIncludes: job.priceIncludes ?? null,
    maxClips: job.maxClips,
    compose: job.compose,
    headless: job.headless,
    status: job.status,
    createdAt: job.createdAt,
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
    logs: job.logs,
    result: job.result,
    error: job.error,
    resumeRunId: job.resumeRunId,
    sourcePayload: job.sourcePayload ?? null,
    progress: job.progress ?? null,
    voiceoverScriptApproval: job.voiceoverScriptApproval ?? false,
  };
}

function createVehicleInventoryCache() {
  const state = {
    loaded: false,
    cachedAt: null,
    vehicles: [],
    refreshing: false,
    lastError: "",
    refreshPromise: null,
  };

  async function loadFromDisk() {
    if (state.loaded) {
      return;
    }

    try {
      const raw = await fs.readFile(vehicleInventoryCachePath, "utf8");
      const parsed = JSON.parse(raw);
      const vehicles = Array.isArray(parsed?.vehicles) ? parsed.vehicles : [];
      state.vehicles = vehicles;
      state.cachedAt = typeof parsed?.cachedAt === "string" ? parsed.cachedAt : null;
      state.loaded = true;
      state.lastError = "";
    } catch (error) {
      state.loaded = true;
      state.vehicles = [];
      state.cachedAt = null;
      if (error?.code !== "ENOENT") {
        state.lastError = String(error?.message ?? error);
      }
    }
  }

  function getStatus() {
    return {
      cachePath: path.relative(rootDir, vehicleInventoryCachePath),
      cachedAt: state.cachedAt,
      count: state.vehicles.length,
      refreshing: state.refreshing,
      lastError: state.lastError,
    };
  }

  async function search(query, { limit = 20 } = {}) {
    await loadFromDisk();

    const normalized = normalizeSearchText(query);
    const cacheEmpty = state.vehicles.length === 0;

    if (!normalized) {
      return {
        query,
        cachedAt: state.cachedAt,
        count: state.vehicles.length,
        matches: cacheEmpty ? [] : state.vehicles.slice(0, limit),
        source: cacheEmpty ? "empty" : "cache",
        hint: cacheEmpty ? "Cache empty. Click refresh." : "",
      };
    }

    if (cacheEmpty) {
      return {
        query,
        cachedAt: state.cachedAt,
        count: 0,
        matches: [],
        source: "empty",
        hint: "Cache empty. Click refresh.",
      };
    }

    const matches = [];
    const scored = [];
    for (const vehicle of state.vehicles) {
      const score = scoreVehicleMatch(vehicle, normalized);
      if (score <= 0) {
        continue;
      }
      scored.push({ vehicle, score });
    }
    scored.sort((a, b) => b.score - a.score);
    for (const entry of scored.slice(0, limit)) {
      matches.push(entry.vehicle);
    }

    return {
      query,
      cachedAt: state.cachedAt,
      count: state.vehicles.length,
      matches,
      source: "cache",
      hint: "",
    };
  }

  function refresh() {
    if (state.refreshPromise) {
      return false;
    }

    state.refreshing = true;
    state.lastError = "";
    state.refreshPromise = refreshFromUpstream()
      .catch((error) => {
        state.lastError = String(error?.message ?? error);
      })
      .finally(() => {
        state.refreshing = false;
        state.refreshPromise = null;
      });

    return true;
  }

  async function refreshFromUpstream() {
    const vehicles = [];
    let page = 0;
    let seenEmpty = false;
    const maxPages = 2000;
    let totalPages = null;

    await fs.mkdir(vehicleInventoryCacheDir, { recursive: true });

    while (page < maxPages && !seenEmpty) {
      const data = await fetchVehicleInventoryPage(page);
      const content = Array.isArray(data?.content) ? data.content : [];
      if (!content.length) {
        seenEmpty = true;
        break;
      }

      vehicles.push(...content);

      const parsedTotalPages = Number(data?.page?.totalPages);
      if (Number.isFinite(parsedTotalPages) && parsedTotalPages > 0) {
        totalPages = parsedTotalPages;
      }

      page += 1;
      if (totalPages != null && page >= totalPages) {
        break;
      }
    }

    const cachedAt = new Date().toISOString();
    const payload = {
      cachedAt,
      vehicles,
    };

    const tmpPath = `${vehicleInventoryCachePath}.tmp`;
    await fs.writeFile(tmpPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
    await fs.rename(tmpPath, vehicleInventoryCachePath);

    state.loaded = true;
    state.cachedAt = cachedAt;
    state.vehicles = vehicles;
  }

  async function fetchVehicleInventoryPage(page) {
    const upstreamUrl = new URL(vehicleInventoryApiUrl);
    upstreamUrl.search = new URLSearchParams({
      page: String(page),
      size: "500",
      sort: "id,asc",
      soldStatus: "UnSold",
      // stockIn: "Japan",
    }).toString();

    const upstreamResponse = await fetch(upstreamUrl, {
      headers: { accept: "application/json" },
    });

    if (!upstreamResponse.ok) {
      throw new Error(`Vehicle inventory API failed with ${upstreamResponse.status}.`);
    }

    return upstreamResponse.json();
  }

  function normalizeSearchText(value) {
    return String(value ?? "").trim().toLowerCase();
  }

  function scoreVehicleMatch(vehicle, normalizedQuery) {
    const stock = normalizeSearchText(vehicle?.stockNo);
    const title = normalizeSearchText(vehicle?.title);

    if (!stock && !title) {
      return 0;
    }

    let score = 0;
    if (stock === normalizedQuery) {
      score += 1000;
    } else if (stock.startsWith(normalizedQuery)) {
      score += 350;
    } else if (stock.includes(normalizedQuery)) {
      score += 250;
    }

    if (title.startsWith(normalizedQuery)) {
      score += 140;
    } else if (title.includes(normalizedQuery)) {
      score += 110;
    }

    return score;
  }

  return {
    getStatus,
    search,
    refresh,
  };
}

function normalizeBoundedInteger(value, fallback, { min, max }) {
  const numeric = Number(value);
  if (!Number.isInteger(numeric)) {
    return fallback;
  }
  return Math.min(max, Math.max(min, numeric));
}

async function serveStaticFile(response, filePath, contentType) {
  const content = await fs.readFile(filePath);
  response.writeHead(200, {
    "content-type": contentType,
    "content-length": content.length,
    "cache-control": "no-cache",
  });
  response.end(content);
}

async function serveWorkspaceFile(request, response, relativePath) {
  const normalizedRelative = relativePath.replaceAll("/", path.sep);
  const resolvedPath = path.resolve(rootDir, normalizedRelative);
  const relativeToRoot = path.relative(rootDir, resolvedPath);

  if (relativeToRoot.startsWith("..") || path.isAbsolute(relativeToRoot)) {
    return sendJson(response, 403, { error: "Access denied." });
  }

  let stats;
  try {
    stats = await fs.stat(resolvedPath);
  } catch {
    return sendJson(response, 404, { error: "File not found." });
  }

  if (!stats.isFile()) {
    return sendJson(response, 404, { error: "File not found." });
  }

  const contentType = mimeTypeForPath(resolvedPath);
  const range = request.headers.range;

  if (!range) {
    response.writeHead(200, {
      "content-type": contentType,
      "content-length": stats.size,
      "cache-control": "no-cache",
    });
    createReadStream(resolvedPath).pipe(response);
    return;
  }

  const match = /bytes=(\d+)-(\d*)/u.exec(range);
  if (!match) {
    response.writeHead(416);
    response.end();
    return;
  }

  const start = Number(match[1]);
  let end = match[2] !== "" ? Number(match[2]) : stats.size - 1;

  if (stats.size === 0 || start >= stats.size || start > end) {
    response.writeHead(416, { "content-range": `bytes */${stats.size}` });
    response.end();
    return;
  }

  if (end >= stats.size) {
    end = stats.size - 1;
  }

  response.writeHead(206, {
    "content-type": contentType,
    "content-length": end - start + 1,
    "content-range": `bytes ${start}-${end}/${stats.size}`,
    "accept-ranges": "bytes",
    "cache-control": "no-cache",
  });
  createReadStream(resolvedPath, { start, end }).pipe(response);
}

function parseUrls(value) {
  if (Array.isArray(value)) {
    return value.map((entry) => String(entry).trim()).filter(Boolean);
  }

  return String(value ?? "")
    .split(/\r?\n|,/u)
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function parseSingleAlbumUrl(value) {
  const urls = parseUrls(value);
  if (!urls.length) {
    return { error: "A Google Photos album URL is required." };
  }
  if (urls.length > 1) {
    return { error: "Only one Google Photos album URL is allowed." };
  }
  return { url: urls[0] };
}

function normalizeListingText(value) {
  return String(value ?? "").trim();
}

function normalizePriceIncludesBody(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  if (Array.isArray(value)) {
    const list = value.map((item) => normalizeListingText(item)).filter(Boolean);
    return list.length ? list : null;
  }
  const lines = String(value)
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter(Boolean);
  return lines.length ? lines : null;
}

function normalizeCommand(value) {
  return ["download", "prepare", "run"].includes(value) ? value : "prepare";
}

function normalizeOptionalNumber(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }

  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

async function buildEndSceneSamplePayload(sample) {
  const sampleStats = await fs.stat(sample.videoPath);
  return {
    videoUrl: toPublicFileUrl(sample.videoPath, rootDir),
    videoVersion: String(Math.trunc(sampleStats.mtimeMs)),
    renderer: sample.renderer,
    durationSeconds: sample.durationSeconds,
    meta: sample.meta,
    debug: sample.debug ?? null,
  };
}

function mimeTypeForPath(filePath) {
  const extension = path.extname(filePath).toLowerCase();
  switch (extension) {
    case ".html":
      return "text/html; charset=utf-8";
    case ".css":
      return "text/css; charset=utf-8";
    case ".js":
      return "text/javascript; charset=utf-8";
    case ".json":
      return "application/json; charset=utf-8";
    case ".jpg":
    case ".jpeg":
      return "image/jpeg";
    case ".png":
      return "image/png";
    case ".webm":
      return "video/webm";
    case ".mov":
      return "video/quicktime";
    case ".mp4":
      return "video/mp4";
    default:
      return "application/octet-stream";
  }
}

async function readJsonBody(request) {
  const chunks = [];

  for await (const chunk of request) {
    chunks.push(chunk);
  }

  const raw = Buffer.concat(chunks).toString("utf8");
  return raw ? JSON.parse(raw) : {};
}

function sendJson(response, statusCode, payload) {
  const body = Buffer.from(`${JSON.stringify(payload, null, 2)}\n`);
  response.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "content-length": body.length,
    "cache-control": "no-cache",
  });
  response.end(body);
}

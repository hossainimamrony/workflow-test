import fs from "node:fs/promises";
import path from "node:path";

import { launchWorkflowBrowser } from "./browser.mjs";
import { composeSelectedClips } from "./composer.mjs";
import { resolveReelDurations } from "./config.mjs";
import { ensureDir, makeRunDirectory, writeJson } from "./fs-utils.mjs";
import { publishFinalReelMp4 } from "./final-reel-output.mjs";
import { classifyClipFrames } from "./gemini.mjs";
import { downloadAlbumVideos } from "./google-photos-downloads.mjs";
import { buildReelPlan, renderPlanSummary } from "./planner.mjs";
import { extractFirstFramesWithPython } from "./python-frames.mjs";
import { appendEndSceneToReel } from "./end-scene.mjs";
import { renderLockedReelOrder } from "./reel-rules.mjs";
import {
  applyVoiceoverToReel,
  draftVoiceoverScripts,
  reapplySavedVoiceoverToReel,
  resetVoiceoverStateForSilentRebuild,
} from "./voiceover.mjs";

export async function executeWorkflow(config, hooks = {}) {
  validateWorkflowConfig(config);

  const log = hooks.log ?? (() => {});
  const progress = hooks.onProgress ?? (() => {});
  const approvedScript = String(hooks.approvedScript ?? "").trim();
  const runDir = config.outDir ?? makeRunDirectory(process.cwd());
  const framesDir = path.join(runDir, "frames");
  const downloadsDir = path.join(runDir, "downloads");

  await Promise.all([ensureDir(runDir), ensureDir(framesDir), ensureDir(downloadsDir)]);

  await writeJson(path.join(runDir, "downloads-manifest.json"), {
    createdAt: new Date().toISOString(),
    albumUrls: config.urls,
    listingTitle: config.listingTitle ?? "",
    stockId: config.stockId ?? "",
    carDescription: config.carDescription ?? "",
    listingPrice: config.listingPrice ?? "",
    priceIncludes: config.priceIncludes ?? null,
    videos: [],
  });

  const reuse = await hydrateRunFromUrlCache({
    runDir,
    config,
    log,
  });

  // Do not short-circuit "run" into script-only mode.
  // For Django workflow parity, run must execute download -> frames -> analyze first,
  // then draft script options (when approval is enabled) near the end.

  progress({ phase: "download", percent: 4, label: "Download" });
  log(`Run directory: ${runDir}`);
  log(`Locked reel pattern: ${renderLockedReelOrder()}`);

  if (reuse?.downloadedVideos?.length) {
    progress({ phase: "download", percent: 18, label: "Downloaded" });
    log(`Reused ${reuse.downloadedVideos.length} cached video(s) from a previous run (same album URL).`);

    if (config.command === "download") {
      progress({ phase: "download", percent: 100, label: "Done" });
      return {
        command: config.command,
        runDir,
        downloadedVideos: reuse.downloadedVideos,
      };
    }

    if (reuse.framedVideos?.length) {
      progress({ phase: "frames", percent: 38, label: "Frames done" });
      log(`Reused ${reuse.framedVideos.length} cached frame set(s).`);
    }

    if (config.command === "prepare" && reuse.framedVideos?.length) {
      progress({ phase: "frames", percent: 100, label: "Done" });
      return {
        command: config.command,
        runDir,
        downloadedVideos: reuse.downloadedVideos,
        framedVideos: reuse.framedVideos,
      };
    }

    if (config.compose && reuse.hasAnalysis) {
      const composed = await composeSavedRun(runDir, config, {
        log,
        onProgress: progress,
        approvedScript,
      });
      return {
        command: config.command,
        runDir,
        downloadedVideos: reuse.downloadedVideos,
        framedVideos: reuse.framedVideos ?? [],
        classifiedClips: composed.classifiedClips ?? [],
        reelPlan: composed.reelPlan ?? null,
        outputPath: composed.outputPath ?? null,
      };
    }
  }

  const browserSession = await launchWorkflowBrowser(config);

  try {
    const downloadedVideos = [];

    for (const url of config.urls) {
      log(`Downloading album videos: ${url}`);
      const albumVideos = await downloadAlbumVideos(browserSession.context, url, downloadsDir, {
        maxClips: config.maxClips,
        log,
      });
      downloadedVideos.push(...albumVideos);
    }

    progress({ phase: "download", percent: 18, label: "Downloaded" });

    if (!downloadedVideos.length) {
      throw new Error(
        "No downloadable video clips were found in the provided album URLs. The shared album may contain photos only, or Google Photos did not expose direct video files for those items.",
      );
    }

    await writeJson(path.join(runDir, "downloads-manifest.json"), {
      createdAt: new Date().toISOString(),
      albumUrls: config.urls,
      listingTitle: config.listingTitle ?? "",
      stockId: config.stockId ?? "",
      carDescription: config.carDescription ?? "",
      listingPrice: config.listingPrice ?? "",
      priceIncludes: config.priceIncludes ?? null,
      videos: downloadedVideos,
    });

    if (config.command === "download") {
      progress({ phase: "download", percent: 100, label: "Done" });
      log(`Downloaded ${downloadedVideos.length} video(s).`);
      return {
        command: config.command,
        runDir,
        downloadedVideos,
      };
    }

    progress({ phase: "frames", percent: 22, label: "Frames" });
    log(
      `Downloaded ${downloadedVideos.length} video(s). Extracting ${config.shotsPerClip} sampled frame(s) per clip after the first ${config.clipStartSkipSeconds}s...`,
    );
    const framedVideos = await extractFirstFramesWithPython(config, downloadedVideos, runDir);
    progress({ phase: "frames", percent: 38, label: "Frames done" });

    if (config.command === "prepare") {
      progress({ phase: "frames", percent: 100, label: "Done" });
      log(
        `Prepared ${framedVideos.length} video(s) with sampled frame sets (${config.shotsPerClip} target frame(s) per clip).`,
      );
      return {
        command: config.command,
        runDir,
        downloadedVideos,
        framedVideos,
      };
    }

    const { classifiedClips, reelPlan, outputPath } = await analyzeAndMaybeCompose(
      {
        config,
        runDir,
        albumUrls: config.urls,
        framedVideos,
        log,
        onProgress: progress,
        approvedScript,
      },
      browserSession.context,
    );

    progress({ phase: "done", percent: 100, label: "Done" });
    return {
      command: config.command,
      runDir,
      downloadedVideos,
      framedVideos,
      classifiedClips,
      reelPlan,
      outputPath,
    };
  } finally {
    await browserSession.close();
  }
}

export async function continueWorkflow(runDir, config, hooks = {}) {
  validateWorkflowConfig(config, { requireUrls: false });

  const log = hooks.log ?? (() => {});
  const progress = hooks.onProgress ?? (() => {});
  const approvedScript = String(hooks.approvedScript ?? "").trim();
  const normalizedRunDir = path.resolve(runDir);
  const downloadsManifest =
    (await readJsonIfExists(path.join(normalizedRunDir, "downloads-manifest.json"))) ??
    (await readJsonIfExists(path.join(normalizedRunDir, "downloads", "downloads-manifest.json")));

  if (!downloadsManifest) {
    throw new Error("Could not resume this run because downloads-manifest.json was not found.");
  }

  let mergedDownloads = downloadsManifest;
  const cfgCarDesc = String(config.carDescription ?? "").trim();
  const manCarDesc = String(mergedDownloads.carDescription ?? "").trim();
  if (cfgCarDesc && !manCarDesc) {
    mergedDownloads = { ...mergedDownloads, carDescription: cfgCarDesc };
    await writeJson(path.join(normalizedRunDir, "downloads-manifest.json"), mergedDownloads);
    log("Saved car description into downloads-manifest.json (was missing).");
  }
  const cfgPrice = String(config.listingPrice ?? "").trim();
  const manPrice = String(mergedDownloads.listingPrice ?? "").trim();
  if (cfgPrice && !manPrice) {
    mergedDownloads = { ...mergedDownloads, listingPrice: cfgPrice };
    await writeJson(path.join(normalizedRunDir, "downloads-manifest.json"), mergedDownloads);
    log("Saved listing price into downloads-manifest.json (was missing).");
  }

  const albumUrls = Array.isArray(mergedDownloads.albumUrls) ? mergedDownloads.albumUrls : [];
  let downloadedVideos = Array.isArray(mergedDownloads.videos) ? mergedDownloads.videos : [];

  if (!downloadedVideos.length) {
    if (!albumUrls.length) {
      throw new Error("This run has no saved album URL, so the full video cannot be generated.");
    }

    progress({ phase: "download", percent: 6, label: "Download" });
    log("No downloaded clips found yet. Downloading footage now...");
    const browserSession = await launchWorkflowBrowser(config);
    try {
      for (const url of albumUrls) {
        log(`Downloading album videos: ${url}`);
        const albumVideos = await downloadAlbumVideos(browserSession.context, url, path.join(normalizedRunDir, "downloads"), {
          maxClips: config.maxClips,
          log,
        });
        downloadedVideos.push(...albumVideos);
      }
    } finally {
      await browserSession.close();
    }

    if (!downloadedVideos.length) {
      throw new Error("No downloadable video clips were found for this saved run.");
    }

    mergedDownloads = {
      ...mergedDownloads,
      videos: downloadedVideos,
    };
    await writeJson(path.join(normalizedRunDir, "downloads-manifest.json"), mergedDownloads);
    progress({ phase: "download", percent: 18, label: "Downloaded" });
  }

  let framedVideos = (await readJsonIfExists(path.join(normalizedRunDir, "frames-manifest.json")))?.videos ?? null;

  progress({ phase: "frames", percent: 24, label: "Frames" });
  if (!framedVideos?.length) {
    log("Existing run has downloads but no frame manifest. Extracting sampled frames...");
    framedVideos = await extractFirstFramesWithPython(config, downloadedVideos, normalizedRunDir);
  } else if (!hasPreparedFrameSets(framedVideos, config.shotsPerClip, config.clipStartSkipSeconds)) {
    log(
      `Existing run frame samples do not match the current ${config.clipStartSkipSeconds}s start skip. Refreshing sampled frames...`,
    );
    framedVideos = await extractFirstFramesWithPython(config, downloadedVideos, normalizedRunDir);
  } else {
    log(
      `Found ${framedVideos.length} prepared video frame set(s). Reusing existing sampled frames.`,
    );
  }
  progress({ phase: "frames", percent: 38, label: "Frames done" });

  if (config.command === "prepare") {
    progress({ phase: "frames", percent: 100, label: "Done" });
    log(
      `Prepared ${framedVideos.length} video(s) with sampled frame sets (${config.shotsPerClip} target frame(s) per clip).`,
    );
    return {
      command: config.command,
      runDir: normalizedRunDir,
      downloadedVideos,
      framedVideos,
    };
  }

  const { classifiedClips, reelPlan, outputPath } = await analyzeAndMaybeCompose(
    {
      config,
      runDir: normalizedRunDir,
      albumUrls,
      framedVideos,
      log,
      onProgress: progress,
      approvedScript,
    },
    null,
  );

  progress({ phase: "done", percent: 100, label: "Done" });
  return {
    command: config.command,
    runDir: normalizedRunDir,
    downloadedVideos,
    framedVideos,
    classifiedClips,
    reelPlan,
    outputPath,
  };
}

export async function composeSavedRun(runDir, config, hooks = {}) {
  const log = hooks.log ?? (() => {});
  const progress = hooks.onProgress ?? (() => {});
  const approvedScript = String(hooks.approvedScript ?? "").trim();
  const normalizedRunDir = path.resolve(runDir);
  const downloadsManifest =
    (await readJsonIfExists(path.join(normalizedRunDir, "downloads-manifest.json"))) ??
    (await readJsonIfExists(path.join(normalizedRunDir, "downloads", "downloads-manifest.json")));
  let mergedDownloads = downloadsManifest;
  if (mergedDownloads) {
    const cfgCarDesc = String(config.carDescription ?? "").trim();
    const manCarDesc = String(mergedDownloads.carDescription ?? "").trim();
    if (cfgCarDesc && !manCarDesc) {
      mergedDownloads = { ...mergedDownloads, carDescription: cfgCarDesc };
      await writeJson(path.join(normalizedRunDir, "downloads-manifest.json"), mergedDownloads);
      log("Saved car description into downloads-manifest.json (was missing).");
    }
    const cfgPrice = String(config.listingPrice ?? "").trim();
    const manPrice = String(mergedDownloads.listingPrice ?? "").trim();
    if (cfgPrice && !manPrice) {
      mergedDownloads = { ...mergedDownloads, listingPrice: cfgPrice };
      await writeJson(path.join(normalizedRunDir, "downloads-manifest.json"), mergedDownloads);
      log("Saved listing price into downloads-manifest.json (was missing).");
    }
  }

  const analysisManifest = await readJsonIfExists(path.join(normalizedRunDir, "analysis.json"));

  if (!analysisManifest?.clips?.length) {
    throw new Error("Could not compose this run because analysis.json was not found.");
  }

  progress({ phase: "compose", percent: 55, label: "Plan" });
  const mainSec = config.composeMainDurationSeconds ?? config.composeDurationSeconds ?? 14;
  logReservedEndSceneTiming(config, log);
  log(`Loaded analysis for ${analysisManifest.clips.length} clip(s). Rebuilding ${mainSec}s main montage + end scene...`);
  log(`Locked reel pattern: ${renderLockedReelOrder()}`);

  const reelPlan = buildReelPlan(analysisManifest.clips, config.targetSequence, {
    totalDurationSeconds: mainSec,
  });
  await writeJson(path.join(normalizedRunDir, "reel-plan.json"), reelPlan);

  log(renderPlanSummary(reelPlan));
  progress({ phase: "compose", percent: 72, label: "Render" });
  log("Composing the selected local clips...");

  const mainReelPath = path.join(normalizedRunDir, "main-reel.webm");
  await composeSelections(null, reelPlan, mainReelPath, config);

  const manifestForEnd = await readDownloadsManifest(normalizedRunDir);
  await appendEndSceneToReel(normalizedRunDir, config, manifestForEnd, log);
  await resetVoiceoverStateForSilentRebuild(normalizedRunDir);

  const outputPath = path.join(normalizedRunDir, "final-reel.webm");

  progress({ phase: "compose", percent: 92, label: "Voice" });
  if (approvedScript) {
    await applyVoiceoverToReel(normalizedRunDir, config, log, {
      approvedScript,
      failOnTtsError: true,
    });
  } else {
    await maybeApplyVoiceover(normalizedRunDir, config, log);
  }

  await publishFinalReelMp4(normalizedRunDir, config.ffmpegPath, log);

  progress({ phase: "compose", percent: 100, label: "Done" });
  log(`Composed reel: ${outputPath}`);

  return {
    command: "compose",
    runDir: normalizedRunDir,
    classifiedClips: analysisManifest.clips,
    reelPlan,
    outputPath,
  };
}

export async function rerenderRunEndScene(runDir, config, hooks = {}) {
  const log = hooks.log ?? (() => {});
  const progress = hooks.onProgress ?? (() => {});
  const normalizedRunDir = path.resolve(runDir);
  const mainReelPath = path.join(normalizedRunDir, "main-reel.webm");

  await fs.access(mainReelPath).catch(() => {
    throw new Error("main-reel.webm not found; build the reel once before rerendering only the ending.");
  });

  progress({ phase: "compose", percent: 58, label: "End scene" });
  logReservedEndSceneTiming(config, log);
  log("Rerendering the reel ending using the existing main montage...");

  const manifestForEnd = await readDownloadsManifest(normalizedRunDir);
  await appendEndSceneToReel(normalizedRunDir, config, manifestForEnd, log);

  progress({ phase: "compose", percent: 84, label: "Finalize" });
  const restoredVoiceover = await reapplySavedVoiceoverToReel(normalizedRunDir, config, log);
  if (!restoredVoiceover) {
    log("No saved voice-over assets were reapplied. Final reel remains silent unless you stitch voice-over again.");
  }
  await publishFinalReelMp4(normalizedRunDir, config.ffmpegPath, log);

  const analysisManifest = await readJsonIfExists(path.join(normalizedRunDir, "analysis.json"));
  const reelPlan = await readJsonIfExists(path.join(normalizedRunDir, "reel-plan.json"));
  const outputPath = path.join(normalizedRunDir, "final-reel.webm");

  progress({ phase: "compose", percent: 100, label: "Done" });
  log(`Updated reel ending: ${outputPath}`);

  return {
    command: "end-scene-rerender",
    runDir: normalizedRunDir,
    classifiedClips: analysisManifest?.clips ?? [],
    reelPlan,
    outputPath,
  };
}

async function analyzeAndMaybeCompose(input, browserContext) {
  const { config, runDir, albumUrls, framedVideos, log, onProgress, approvedScript = "" } = input;
  const progress = onProgress ?? (() => {});
  const totalFrameCount = framedVideos.reduce(
    (sum, video) => sum + resolveFramePaths(video).length,
    0,
  );

  progress({ phase: "analyze", percent: 42, label: "Classify" });
  log(
    `Prepared ${totalFrameCount} sampled frame image(s) across ${framedVideos.length} clip(s). Sending them to Gemini...`,
  );

  const classifiedClips = [];
  const n = Math.max(framedVideos.length, 1);
  let index = 0;
  for (const video of framedVideos) {
    const framePaths = resolveFramePaths(video);
    const analysis = await classifyClipFrames(config, {
      clipId: video.clipId,
      durationSeconds: 0,
      framePaths,
    });

    classifiedClips.push({
      ...video,
      title: video.ariaLabel,
      framePaths,
      analysis,
    });

    index += 1;
    const pct = 42 + Math.round((index / n) * 40);
    progress({ phase: "analyze", percent: Math.min(pct, 82), label: video.clipId });

    log(
      `Classified ${video.clipId}: ${analysis.primaryLabel} (confidence ${analysis.confidence})`,
    );
  }

  const reelPlan = buildReelPlan(classifiedClips, config.targetSequence, {
    totalDurationSeconds: config.composeMainDurationSeconds ?? config.composeDurationSeconds ?? 14,
  });

  await writeJson(path.join(runDir, "analysis.json"), {
    createdAt: new Date().toISOString(),
    albumUrls,
    clips: classifiedClips,
  });
  await writeJson(path.join(runDir, "reel-plan.json"), reelPlan);

  log(renderPlanSummary(reelPlan));

  let outputPath = null;
  if (config.compose) {
    progress({ phase: "compose", percent: 85, label: "Main reel" });
    logReservedEndSceneTiming(config, log);
    log("Composing the selected local clips...");

    const mainReelPath = path.join(runDir, "main-reel.webm");
    if (browserContext) {
      await composeSelections(browserContext, reelPlan, mainReelPath, config, { log });
    } else {
      const browserSession = await launchWorkflowBrowser(config);
      try {
        await composeSelections(browserSession.context, reelPlan, mainReelPath, config, { log });
      } finally {
        await browserSession.close();
      }
    }

    progress({ phase: "compose", percent: 89, label: "End scene" });
    log("Main reel rendered. Building branded end scene...");
    const manifestForEnd = await readDownloadsManifest(runDir);
    await appendEndSceneToReel(runDir, config, manifestForEnd, log, {
      browserContext,
    });
    await resetVoiceoverStateForSilentRebuild(runDir);

    outputPath = path.join(runDir, "final-reel.webm");

    progress({ phase: "compose", percent: 92, label: "Voice" });
    if (approvedScript) {
      await applyVoiceoverToReel(runDir, config, log, {
        approvedScript,
        failOnTtsError: true,
      });
    } else {
      await maybeApplyVoiceover(runDir, config, log);
    }

    await publishFinalReelMp4(runDir, config.ffmpegPath, log);

    log(`Composed reel: ${outputPath}`);
    progress({ phase: "compose", percent: 100, label: "Done" });
  } else if (config.command === "run" && config.voiceoverScriptApproval) {
    progress({ phase: "voiceover", percent: 88, label: "Scripts" });
    await draftVoiceoverScripts(runDir, config, log, { strict: true });
    progress({ phase: "voiceover", percent: 100, label: "Done" });
  }

  return {
    classifiedClips,
    reelPlan,
    outputPath,
  };
}

async function maybeApplyVoiceover(runDir, config, log) {
  try {
    if (config.voiceoverScriptApproval) {
      const draft = await draftVoiceoverScripts(runDir, config, log);
      if (draft?.variants?.length) {
        log("Voice-over: script drafts saved. Open Runs → pick a script and stitch audio when ready.");
      }
      return;
    }
    await applyVoiceoverToReel(runDir, config, log, { failOnTtsError: false });
  } catch (error) {
    log(`Voice-over skipped or failed (silent reel kept): ${error?.message ?? error}`);
  }
}

async function composeSelections(browserContext, reelPlan, outputPath, config, hooks = {}) {
  const mainSeconds = config.composeMainDurationSeconds ?? config.composeDurationSeconds ?? 14;
  await composeSelectedClips(
    browserContext,
    (reelPlan.composition?.segments?.length ? reelPlan.composition.segments : reelPlan.sequence).map(
      (selection) => ({
        ...selection,
        filePath: selection.videoPath,
      }),
    ),
    outputPath,
    {
      width: config.composeWidth,
      height: config.composeHeight,
      fps: config.composeFps,
      durationSeconds: mainSeconds,
      ffmpegPath: config.ffmpegPath,
      clipStartSkipSeconds: config.clipStartSkipSeconds,
      webmCodec: config.webmCodec,
      webmDeadline: config.webmDeadline,
      webmCpuUsed: config.webmCpuUsed,
      webmCrf: config.webmCrf,
      webmThreads: config.webmThreads,
      log: hooks.log,
    },
  );
}

function logReservedEndSceneTiming(config, log) {
  if (typeof log !== "function") {
    return;
  }
  const reelDurations = resolveReelDurations(config);
  if (!reelDurations.mainDurationShortened) {
    return;
  }
  log(
    `Reserved ${reelDurations.endSceneDurationSeconds.toFixed(1)}s for the end scene, so the main montage runs ${reelDurations.composeMainDurationSeconds.toFixed(1)}s within the ${reelDurations.maxTotalReelDurationSeconds.toFixed(1)}s cap.`,
  );
}

function validateWorkflowConfig(config, options = {}) {
  if (options.requireUrls !== false && !config.urls?.length) {
    throw new Error("At least one URL is required.");
  }

  if (config.command === "run" && !config.geminiApiKey) {
    throw new Error("Missing or placeholder GEMINI_API_KEY. Add a real key to .env.");
  }
}

async function readJsonIfExists(filePath) {
  try {
    return JSON.parse(await fs.readFile(filePath, "utf8"));
  } catch {
    return null;
  }
}

async function readDownloadsManifest(runDir) {
  return (
    (await readJsonIfExists(path.join(runDir, "downloads-manifest.json"))) ??
    (await readJsonIfExists(path.join(runDir, "downloads", "downloads-manifest.json"))) ??
    {}
  );
}

async function hydrateRunFromUrlCache({ runDir, config, log }) {
  const requestedUrls = normalizeAlbumUrlList(config.urls);
  if (!requestedUrls.length) {
    return null;
  }

  const runsRoot = path.join(process.cwd(), "runs");
  const currentRunDir = path.resolve(runDir);
  let entries;
  try {
    entries = await fs.readdir(runsRoot, { withFileTypes: true });
  } catch {
    return null;
  }

  const candidateDirs = entries
    .filter((entry) => entry.isDirectory())
    .map((entry) => path.join(runsRoot, entry.name))
    .filter((dir) => path.resolve(dir) !== currentRunDir)
    .sort((left, right) => right.localeCompare(left));

  for (const cachedRunDir of candidateDirs) {
    const cachedDownloads =
      (await readJsonIfExists(path.join(cachedRunDir, "downloads-manifest.json"))) ??
      (await readJsonIfExists(path.join(cachedRunDir, "downloads", "downloads-manifest.json")));
    if (!cachedDownloads) {
      continue;
    }
    const cachedUrls = normalizeAlbumUrlList(cachedDownloads.albumUrls ?? cachedDownloads.albumUrl ?? []);
    if (!areSameUrlLists(requestedUrls, cachedUrls)) {
      continue;
    }

    const cachedVideos = Array.isArray(cachedDownloads.videos) ? cachedDownloads.videos : [];
    if (!cachedVideos.length) {
      continue;
    }

    await fs.cp(path.join(cachedRunDir, "downloads"), path.join(runDir, "downloads"), {
      recursive: true,
      force: true,
    }).catch(() => {});
    await fs.cp(path.join(cachedRunDir, "frames"), path.join(runDir, "frames"), {
      recursive: true,
      force: true,
    }).catch(() => {});

    const framesManifest = await readJsonIfExists(path.join(cachedRunDir, "frames-manifest.json"));
    const analysisManifest = await readJsonIfExists(path.join(cachedRunDir, "analysis.json"));
    const reelPlan = await readJsonIfExists(path.join(cachedRunDir, "reel-plan.json"));

    const rewrittenDownloads = rewriteRunPaths(cachedDownloads, cachedRunDir, runDir);
    await writeJson(path.join(runDir, "downloads-manifest.json"), rewrittenDownloads);
    const rewrittenVideos = Array.isArray(rewrittenDownloads.videos) ? rewrittenDownloads.videos : [];

    let rewrittenFrames = null;
    if (framesManifest?.videos?.length) {
      rewrittenFrames = rewriteRunPaths(framesManifest, cachedRunDir, runDir);
      await writeJson(path.join(runDir, "frames-manifest.json"), rewrittenFrames);
    }

    let hasAnalysis = false;
    if (analysisManifest?.clips?.length) {
      hasAnalysis = true;
      await writeJson(path.join(runDir, "analysis.json"), rewriteRunPaths(analysisManifest, cachedRunDir, runDir));
    }
    if (reelPlan) {
      await writeJson(path.join(runDir, "reel-plan.json"), rewriteRunPaths(reelPlan, cachedRunDir, runDir));
    }

    log(`Cache hit: reused assets from ${path.basename(cachedRunDir)} for matching album URL(s).`);
    return {
      downloadedVideos: rewrittenVideos,
      framedVideos: rewrittenFrames?.videos ?? null,
      hasAnalysis,
    };
  }

  return null;
}

function rewriteRunPaths(value, fromRunDir, toRunDir) {
  if (Array.isArray(value)) {
    return value.map((entry) => rewriteRunPaths(entry, fromRunDir, toRunDir));
  }
  if (!value || typeof value !== "object") {
    if (typeof value === "string") {
      const from = path.resolve(fromRunDir);
      const to = path.resolve(toRunDir);
      return value.startsWith(from) ? `${to}${value.slice(from.length)}` : value;
    }
    return value;
  }
  const out = {};
  for (const [key, entryValue] of Object.entries(value)) {
    out[key] = rewriteRunPaths(entryValue, fromRunDir, toRunDir);
  }
  return out;
}

function normalizeAlbumUrlList(value) {
  const urls = Array.isArray(value) ? value : [value];
  return urls
    .map((entry) => String(entry ?? "").trim())
    .filter(Boolean)
    .sort();
}

function areSameUrlLists(left, right) {
  if (left.length !== right.length) {
    return false;
  }
  return left.every((entry, index) => entry === right[index]);
}

function hasPreparedFrameSets(videos, shotsPerClip, clipStartSkipSeconds = 0) {
  return videos.every(
    (video) =>
      resolveFramePaths(video).length >= Math.max(shotsPerClip, 1) &&
      hasExpectedSampleStart(video, clipStartSkipSeconds),
  );
}

function hasExpectedSampleStart(video, clipStartSkipSeconds) {
  const skip = Math.max(0, Number(clipStartSkipSeconds) || 0);
  const toleranceSeconds = 0.2;
  if (skip <= 0.05) {
    return true;
  }

  const offsets = Array.isArray(video.sampledAtSeconds) ? video.sampledAtSeconds : [];
  if (!offsets.length) {
    return false;
  }
  const actualStart = Number(offsets[0]);
  if (!Number.isFinite(actualStart)) {
    return false;
  }

  const durationValue = Number(video.durationSeconds);
  if (!Number.isFinite(durationValue) || durationValue <= 0) {
    return Math.abs(actualStart - skip) <= toleranceSeconds;
  }

  const duration = Math.max(0, durationValue);
  const effectiveEnd = Math.max(duration - Math.min(0.15, duration * 0.1), 0);
  const expectedStart = Math.min(skip, effectiveEnd);
  if (expectedStart <= 0.05) {
    return true;
  }

  return Math.abs(actualStart - expectedStart) <= toleranceSeconds;
}

function resolveFramePaths(video) {
  if (Array.isArray(video.framePaths) && video.framePaths.length) {
    return video.framePaths;
  }

  return video.framePath ? [video.framePath] : [];
}

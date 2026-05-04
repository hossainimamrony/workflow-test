import fs from "node:fs/promises";
import path from "node:path";

export async function listRunReports(rootDir, limit = 48) {
  const runsDir = path.join(rootDir, "runs");

  let entries;
  try {
    entries = await fs.readdir(runsDir, { withFileTypes: true });
  } catch {
    return [];
  }

  const directories = entries
    .filter((entry) => entry.isDirectory())
    .map((entry) => path.join(runsDir, entry.name))
    .sort((left, right) => right.localeCompare(left))
    .slice(0, limit);

  const reports = await Promise.all(directories.map((runDir) => buildRunReport(runDir, rootDir)));
  return reports.filter(Boolean);
}

export async function buildRunReport(runDir, rootDir) {
  const downloadsManifest =
    (await readJsonIfExists(path.join(runDir, "downloads-manifest.json"))) ??
    (await readJsonIfExists(path.join(runDir, "downloads", "downloads-manifest.json")));
  const framesManifest = await readJsonIfExists(path.join(runDir, "frames-manifest.json"));
  const analysisManifest = await readJsonIfExists(path.join(runDir, "analysis.json"));
  const reelPlan = await readJsonIfExists(path.join(runDir, "reel-plan.json"));
  const voiceoverManifest = await readJsonIfExists(path.join(runDir, "voiceover-manifest.json"));
  const voiceoverStatusManifest = await readJsonIfExists(path.join(runDir, "voiceover-status.json"));
  const voiceoverScriptDraft = await readJsonIfExists(path.join(runDir, "voiceover-script-draft.json"));
  const endSceneManifest = await readJsonIfExists(path.join(runDir, "end-scene-manifest.json"));

  if (!downloadsManifest && !framesManifest && !analysisManifest && !reelPlan) {
    return null;
  }

  const downloads = downloadsManifest?.videos ?? [];
  const downloadsByClipId = new Map(downloads.map((video) => [video.clipId, video]));
  const frames = new Map((framesManifest?.videos ?? []).map((video) => [video.clipId, video]));
  const analysis = new Map((analysisManifest?.clips ?? []).map((clip) => [clip.clipId, clip]));
  const runId = path.basename(runDir);
  const finalReelWebmPath = await fileIfExists(path.join(runDir, "final-reel.webm"));
  const finalReelMp4Path = await fileIfExists(path.join(runDir, "final-reel.mp4"));
  const finalReelPath = finalReelMp4Path ?? finalReelWebmPath;
  const finalReelVersion = finalReelPath ? await fileVersionIfExists(finalReelPath) : null;
  const finalReelWebmVersion = finalReelWebmPath ? await fileVersionIfExists(finalReelWebmPath) : null;
  const hasVoiceover = Boolean(voiceoverManifest);
  const voiceoverStatus = hasVoiceover
    ? "applied"
    : normalizeText(voiceoverStatusManifest?.status ?? "");

  const frameVideos = framesManifest?.videos ?? [];
  const pipeline = {
    download: { done: downloads.length > 0 },
    frames: { done: frameVideos.length > 0 },
    analyze: { done: Boolean(analysisManifest) },
    render: { done: Boolean(finalReelPath) },
  };

  const videos = downloads.map((video) => {
    const frameData = frames.get(video.clipId) ?? {};
    const clipAnalysis = analysis.get(video.clipId) ?? null;

    return {
      clipId: video.clipId,
      ariaLabel: normalizeText(frameData.ariaLabel ?? video.ariaLabel ?? ""),
      title: normalizeText(clipAnalysis?.title ?? frameData.ariaLabel ?? video.ariaLabel ?? ""),
      mediaKey: video.mediaKey,
      sizeBytes: video.sizeBytes ?? 0,
      videoPath: video.videoPath,
      videoUrl: toPublicFileUrl(video.videoPath, rootDir),
      framePath: frameData.framePath ?? null,
      framePaths: frameData.framePaths ?? (frameData.framePath ? [frameData.framePath] : []),
      frameUrl: frameData.framePath ? toPublicFileUrl(frameData.framePath, rootDir) : null,
      analysis: clipAnalysis?.analysis ?? null,
    };
  });

  return {
    runId,
    runDir,
    pipeline,
    listingTitle: normalizeText(downloadsManifest?.listingTitle ?? ""),
    stockId: normalizeText(downloadsManifest?.stockId ?? ""),
    carDescription: normalizeText(downloadsManifest?.carDescription ?? ""),
    listingPrice: normalizeText(downloadsManifest?.listingPrice ?? ""),
    priceIncludes: Array.isArray(downloadsManifest?.priceIncludes) ? downloadsManifest.priceIncludes : [],
    hasEndScene: Boolean(endSceneManifest),
    mainMontageDurationSeconds: endSceneManifest?.mainDurationSeconds ?? voiceoverManifest?.mainMontageDurationSeconds ?? null,
    endSceneDurationSeconds: endSceneManifest?.endDurationSeconds ?? null,
    totalReelDurationSeconds: endSceneManifest?.totalDurationSeconds ?? voiceoverManifest?.totalVideoDurationSeconds ?? null,
    hasVoiceover,
    voiceoverStatus: voiceoverStatus || null,
    voiceoverLastError: hasVoiceover ? "" : normalizeText(voiceoverStatusManifest?.lastError ?? ""),
    voiceoverLastAttemptAt: hasVoiceover
      ? voiceoverManifest?.createdAt ?? null
      : voiceoverStatusManifest?.lastAttemptAt ?? null,
    voiceoverRetryable: hasVoiceover ? false : Boolean(voiceoverStatusManifest?.retryable),
    voiceoverDraft: voiceoverScriptDraft
      ? {
          status: String(voiceoverScriptDraft.status ?? "pending"),
          createdAt: voiceoverScriptDraft.createdAt ?? null,
          appliedAt: voiceoverScriptDraft.appliedAt ?? null,
          variants: Array.isArray(voiceoverScriptDraft.variants) ? voiceoverScriptDraft.variants : [],
        }
      : null,
    voiceoverScript: voiceoverManifest ? normalizeText(voiceoverManifest.script ?? "") : "",
    createdAt:
      analysisManifest?.createdAt ??
      downloadsManifest?.createdAt ??
      framesManifest?.createdAt ??
      null,
    hasAnalysis: Boolean(analysisManifest),
    hasPlan: Boolean(reelPlan),
    finalReelUrl: finalReelPath ? toPublicFileUrl(finalReelPath, rootDir) : null,
    finalReelVersion,
    finalReelWebmUrl: finalReelWebmPath ? toPublicFileUrl(finalReelWebmPath, rootDir) : null,
    finalReelWebmVersion,
    stats: {
      downloads: downloads.length,
      frames: frames.size,
      analyzed: analysis.size,
      planned: reelPlan?.composition?.segments?.length ?? reelPlan?.sequence?.length ?? 0,
    },
    videos,
    plan: reelPlan
      ? {
          ...reelPlan,
          sequence: Array.isArray(reelPlan.sequence)
            ? reelPlan.sequence.map((item) => enrichPlanItem(item, { downloadsByClipId, frames, analysis, rootDir }))
            : [],
          composition: reelPlan.composition
            ? {
                ...reelPlan.composition,
                segments: Array.isArray(reelPlan.composition.segments)
                  ? reelPlan.composition.segments.map((item) =>
                      enrichPlanItem(item, { downloadsByClipId, frames, analysis, rootDir }),
                    )
                  : [],
              }
            : null,
        }
      : null,
  };
}

function enrichPlanItem(item, context) {
  const frameData = context.frames.get(item.clipId) ?? {};
  const clipAnalysis = context.analysis.get(item.clipId) ?? null;
  const download = context.downloadsByClipId.get(item.clipId) ?? {};
  const videoPath = item.videoPath ?? download.videoPath ?? null;
  const framePath = item.framePath ?? frameData.framePath ?? frameData.framePaths?.[0] ?? null;

  return {
    ...item,
    role: item.role ?? item.purpose ?? "",
    title: normalizeText(item.title ?? clipAnalysis?.title ?? frameData.ariaLabel ?? download.ariaLabel ?? ""),
    analysis: item.analysis ?? clipAnalysis?.analysis ?? null,
    videoPath,
    framePath,
    videoUrl: videoPath ? toPublicFileUrl(videoPath, context.rootDir) : null,
    frameUrl: framePath ? toPublicFileUrl(framePath, context.rootDir) : null,
  };
}

export function toPublicFileUrl(filePath, rootDir) {
  const relativePath = path.relative(rootDir, filePath);
  const normalized = relativePath.split(path.sep).join("/");
  return `/api/file?path=${encodeURIComponent(normalized)}`;
}

async function readJsonIfExists(filePath) {
  try {
    const content = await fs.readFile(filePath, "utf8");
    return JSON.parse(content);
  } catch {
    return null;
  }
}

async function fileIfExists(filePath) {
  try {
    await fs.access(filePath);
    return filePath;
  } catch {
    return null;
  }
}

async function fileVersionIfExists(filePath) {
  try {
    const stats = await fs.stat(filePath);
    return String(Math.trunc(stats.mtimeMs));
  } catch {
    return null;
  }
}

function normalizeText(value) {
  return String(value ?? "")
    .replaceAll("\u202f", " ")
    .replaceAll("â€¯", " ")
    .replaceAll("Ã¢â‚¬Â¯", " ")
    .trim();
}

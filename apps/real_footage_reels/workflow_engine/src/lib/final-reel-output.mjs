import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { spawn } from "node:child_process";
import { createHash, createHmac } from "node:crypto";

const PREVIEW_FILE_NAME = "final-reel-preview.mp4";
const FINAL_REEL_PUBLISH_MANIFEST = "final-reel-publish.json";

export async function publishFinalReelMp4(runDir, ffmpegPath, log = () => {}, config = {}) {
  const normalizedDir = path.resolve(runDir);
  const mp4Path = path.join(normalizedDir, "final-reel.mp4");
  const webmPath = path.join(normalizedDir, "final-reel.webm");

  const mp4Stats = await statIfExists(mp4Path);
  const webmStats = await statIfExists(webmPath);

  let publishedPath = mp4Path;
  if (!mp4Stats) {
    if (!webmStats) {
      return null;
    }
    log("Publishing MP4 final reel from WebM source...");
    await runProcess(
      ffmpegPath,
      [
        "-y",
        "-i",
        "final-reel.webm",
        "-map",
        "0:v:0",
        "-map",
        "0:a:0?",
        "-c:v",
        String(config.mp4VideoCodec || "libx264"),
        "-preset",
        String(config.mp4Preset || "medium"),
        "-crf",
        String(Number.isFinite(Number(config.mp4Crf)) ? Number(config.mp4Crf) : 18),
        "-pix_fmt",
        "yuv420p",
        "-movflags",
        "+faststart",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "final-reel.mp4",
      ],
      { cwd: normalizedDir },
    );
  } else if (webmStats && webmStats.mtimeMs > mp4Stats.mtimeMs) {
    log("Refreshing MP4 final reel from newer WebM source...");
    await runProcess(
      ffmpegPath,
      [
        "-y",
        "-i",
        "final-reel.webm",
        "-map",
        "0:v:0",
        "-map",
        "0:a:0?",
        "-c:v",
        String(config.mp4VideoCodec || "libx264"),
        "-preset",
        String(config.mp4Preset || "medium"),
        "-crf",
        String(Number.isFinite(Number(config.mp4Crf)) ? Number(config.mp4Crf) : 18),
        "-pix_fmt",
        "yuv420p",
        "-movflags",
        "+faststart",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "final-reel.mp4",
      ],
      { cwd: normalizedDir },
    );
  }

  const previewEnabled = parseBooleanLike(
    config.previewEnabled ?? process.env.REAL_FOOTAGE_PREVIEW_ENABLED,
    true,
  );
  if (previewEnabled) {
    await publishPreviewMp4(normalizedDir, ffmpegPath, log, config);
  }

  const keepWebm = parseBooleanLike(
    config.publishWebm ?? process.env.FINAL_REEL_PUBLISH_WEBM,
    false,
  );
  if (keepWebm) {
    await publishWebmFromMp4(normalizedDir, ffmpegPath, log, config);
  }

  const remoteUploadEnabled = parseBooleanLike(
    config.finalReelRemoteUploadEnabled ?? process.env.FINAL_REEL_REMOTE_UPLOAD_ENABLED,
    false,
  );
  const remoteUploadRequired = parseBooleanLike(
    config.finalReelRemoteUploadRequired ?? process.env.FINAL_REEL_REMOTE_UPLOAD_REQUIRED,
    false,
  );
  if (remoteUploadEnabled) {
    try {
      await publishFinalReelToRemote(normalizedDir, log, config);
    } catch (error) {
      if (remoteUploadRequired) {
        throw error;
      }
      log(`Remote final-reel upload skipped: ${String(error?.message || error || "unknown error")}`);
    }
  }

  return publishedPath;
}

async function publishPreviewMp4(runDir, ffmpegPath, log = () => {}, config = {}) {
  const sourcePath = path.join(runDir, "final-reel.mp4");
  const outPath = path.join(runDir, PREVIEW_FILE_NAME);
  const [sourceStats, outStats] = await Promise.all([
    statIfExists(sourcePath),
    statIfExists(outPath),
  ]);
  if (!sourceStats) {
    return null;
  }
  if (outStats && outStats.mtimeMs >= sourceStats.mtimeMs) {
    return outPath;
  }

  log("Building preview MP4 for faster playback...");
  const maxWidth = Math.max(360, Number(config.previewMaxWidth ?? process.env.REAL_FOOTAGE_PREVIEW_MAX_WIDTH) || 720);
  const maxHeight = Math.max(640, Number(config.previewMaxHeight ?? process.env.REAL_FOOTAGE_PREVIEW_MAX_HEIGHT) || 1280);
  const previewCrf = Number(config.previewCrf ?? process.env.REAL_FOOTAGE_PREVIEW_CRF);
  const previewPreset = String(config.previewPreset ?? process.env.REAL_FOOTAGE_PREVIEW_PRESET ?? "veryfast").trim() || "veryfast";
  const previewFps = Number(config.previewFps ?? process.env.REAL_FOOTAGE_PREVIEW_FPS);

  const vf = [
    `scale=w='min(${maxWidth},iw)':h='min(${maxHeight},ih)':force_original_aspect_ratio=decrease`,
    "pad=w=ceil(iw/2)*2:h=ceil(ih/2)*2",
    "format=yuv420p",
  ].join(",");

  const args = [
    "-y",
    "-i",
    "final-reel.mp4",
    "-vf",
    vf,
    "-c:v",
    "libx264",
    "-preset",
    previewPreset,
    "-crf",
    String(Number.isFinite(previewCrf) ? previewCrf : 30),
    "-movflags",
    "+faststart",
    "-c:a",
    "aac",
    "-b:a",
    String(config.previewAudioBitrate || process.env.REAL_FOOTAGE_PREVIEW_AUDIO_BITRATE || "96k"),
  ];
  if (Number.isFinite(previewFps) && previewFps > 0) {
    args.push("-r", String(Math.max(12, Math.min(30, Math.round(previewFps)))));
  }
  const threads = Number(config.mp4Threads);
  if (Number.isFinite(threads) && threads > 0) {
    args.push("-threads", String(threads));
  }
  args.push(PREVIEW_FILE_NAME);
  await runProcess(ffmpegPath, args, { cwd: runDir });
  return outPath;
}

async function publishWebmFromMp4(runDir, ffmpegPath, log = () => {}, config = {}) {
  const sourcePath = path.join(runDir, "final-reel.mp4");
  const outPath = path.join(runDir, "final-reel.webm");
  const [sourceStats, outStats] = await Promise.all([
    statIfExists(sourcePath),
    statIfExists(outPath),
  ]);
  if (!sourceStats) {
    return null;
  }
  if (outStats && outStats.mtimeMs >= sourceStats.mtimeMs) {
    return outPath;
  }

  log("Publishing optional WebM derivative from MP4 master...");
  const codec = String(config.webmCodec || "libvpx-vp9").trim() || "libvpx-vp9";
  const deadline = String(config.webmDeadline || "").trim();
  const cpuUsed = Number(config.webmCpuUsed);
  const crf = Number(config.webmCrf);
  const threads = Number(config.webmThreads);
  const args = [
    "-y",
    "-i",
    "final-reel.mp4",
    "-map",
    "0:v:0",
    "-map",
    "0:a:0?",
    "-c:v",
    codec,
  ];
  if (codec.includes("vp9")) {
    args.push("-row-mt", "1");
  }
  if (deadline) {
    args.push("-deadline", deadline);
  }
  if (Number.isFinite(cpuUsed)) {
    args.push("-cpu-used", String(cpuUsed));
  }
  if (Number.isFinite(threads) && threads > 0) {
    args.push("-threads", String(threads));
  }
  args.push(
    "-crf",
    String(Number.isFinite(crf) ? crf : 30),
    "-b:v",
    "0",
    "-c:a",
    "libopus",
    "-b:a",
    "128k",
    "final-reel.webm",
  );
  await runProcess(ffmpegPath, args, { cwd: runDir });
  return outPath;
}

async function publishFinalReelToRemote(runDir, log = () => {}, config = {}) {
  const sourcePath = path.join(runDir, "final-reel.mp4");
  const previewPath = path.join(runDir, PREVIEW_FILE_NAME);
  const sourceStats = await statIfExists(sourcePath);
  const previewStats = await statIfExists(previewPath);
  if (!sourceStats) {
    return null;
  }

  const endpoint = String(
    config.finalReelUploadEndpoint ??
      process.env.FINAL_REEL_UPLOAD_ENDPOINT ??
      process.env.S3_UPLOAD_URL ??
      "https://www.cbs.s1.carbarn.com.au/carbarnau/s3/uploadfiles",
  ).trim();
  const directory = String(
    config.finalReelUploadDirectory ??
      process.env.FINAL_REEL_UPLOAD_DIRECTORY ??
      process.env.S3_UPLOAD_DIRECTORY ??
      "social-media-content/reels",
  ).trim().replace(/^\/+|\/+$/gu, "");
  const cdnBase = String(
    config.finalReelCdnBase ??
      process.env.FINAL_REEL_CDN_BASE ??
      process.env.S3_CDN_BASE_URL ??
      "https://www.storage.importautos.com.au/social-media-content/reels",
  ).trim().replace(/\/+$/gu, "");
  const providerFromConfig = String(
    config.finalReelRemoteProvider ??
      process.env.FINAL_REEL_REMOTE_PROVIDER ??
      "",
  ).trim().toLowerCase();
  // Prefer multipart endpoint uploads whenever an endpoint is configured.
  const provider = endpoint
    ? "multipart"
    : ((providerFromConfig === "s3" || providerFromConfig === "multipart") ? providerFromConfig : "multipart");

  if (!directory) {
    throw new Error("Remote upload directory is empty. Set FINAL_REEL_UPLOAD_DIRECTORY.");
  }

  const sourceMtime = Math.trunc(sourceStats.mtimeMs);
  const previewMtime = previewStats ? Math.trunc(previewStats.mtimeMs) : 0;
  const manifestPath = path.join(runDir, FINAL_REEL_PUBLISH_MANIFEST);
  const existing = await readJsonIfExists(manifestPath);
  if (
    existing &&
    String(existing.provider || "") === provider &&
    Number(existing.sourceMtimeMs) === sourceMtime &&
    Number(existing.previewSourceMtimeMs || 0) === previewMtime &&
    String(existing.cdnUrl || "").startsWith("http")
  ) {
    return existing.cdnUrl;
  }

  const runId = path.basename(path.resolve(runDir));
  const fileName = sanitizeRemoteFileName(`${runId}-final-reel.mp4`);
  const previewFileName = sanitizeRemoteFileName(`${runId}-final-reel-preview.mp4`);
  const timeoutMs = Number(
    config.finalReelUploadTimeoutMs ?? process.env.FINAL_REEL_UPLOAD_TIMEOUT_MS ?? 180000,
  );
  const manifestBase = {
    createdAt: new Date().toISOString(),
    sourcePath,
    sourceMtimeMs: sourceMtime,
    previewSourcePath: previewStats ? previewPath : "",
    previewSourceMtimeMs: previewMtime,
    fileName,
    previewFileName: previewStats ? previewFileName : "",
    directory,
    provider,
    endpoint: endpoint || "",
    cdnBase,
  };

  try {
    const uploadResult =
      provider === "s3"
        ? await publishFinalReelToS3({
            runDir,
            sourcePath,
            previewPath: previewStats ? previewPath : "",
            fileName,
            previewFileName,
            directory,
            cdnBase,
            timeoutMs,
            config,
            log,
          })
        : await publishFinalReelToMultipartApi({
            sourcePath,
            fileName,
            directory,
            endpoint,
            cdnBase,
            timeoutMs,
            log,
          });

    await writePublishManifest(manifestPath, {
      ...manifestBase,
      ok: true,
      ...uploadResult,
    });
    log(`Remote final reel ready: ${uploadResult.cdnUrl}`);
    return uploadResult.cdnUrl;
  } catch (error) {
    const uploadDebug =
      error && typeof error === "object" && error.uploadDebug && typeof error.uploadDebug === "object"
        ? error.uploadDebug
        : {};
    await writePublishManifest(manifestPath, {
      ...manifestBase,
      ok: false,
      error: String(error?.message || error || "Remote upload failed."),
      ...uploadDebug,
    });
    throw error;
  }
}

async function publishFinalReelToMultipartApi({
  sourcePath,
  fileName,
  directory,
  endpoint,
  cdnBase,
  timeoutMs,
  log = () => {},
}) {
  if (!endpoint) {
    throw new Error("Remote upload endpoint is empty. Set FINAL_REEL_UPLOAD_ENDPOINT or S3_UPLOAD_URL.");
  }
  if (!cdnBase) {
    throw new Error("Remote CDN base is empty. Set FINAL_REEL_CDN_BASE or S3_CDN_BASE_URL.");
  }

  log(`Uploading final reel to remote API (${directory}/${fileName})...`);
  const bytes = await fs.readFile(sourcePath);
  const form = new FormData();
  form.set("directory", directory);
  form.set("file", new Blob([bytes], { type: "video/mp4" }), fileName);

  const url = new URL(endpoint);
  url.searchParams.set("directory", directory);

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.max(5000, timeoutMs || 180000));
  let response;
  let responseText = "";
  let responseHeaders = {};
  try {
    response = await fetch(url, {
      method: "POST",
      body: form,
      signal: controller.signal,
    });
    responseText = await response.text();
    responseHeaders = pickResponseHeaders(response);
  } finally {
    clearTimeout(timer);
  }

  if (!response?.ok) {
    throw new Error(
      `Remote upload failed (${response?.status || "no-status"}): ${String(responseText || "").slice(0, 500)}`,
    );
  }

  const remoteResponse = tryParseJson(responseText) ?? responseText.slice(0, 4000);
  let cdnUrl = resolveUploadCdnUrl({
    remoteResponse,
    responseText,
    directory,
    fileName,
    cdnBase,
  });
  const fallbackCandidates = buildMultipartCdnFallbackCandidates({ cdnBase, directory, fileName });
  if (!cdnUrl) {
    cdnUrl = await findFirstReachableVideoUrl(fallbackCandidates, timeoutMs);
  }
  if (!cdnUrl) {
    const error = new Error(
      "Remote upload accepted (HTTP 200), but no valid public video URL could be derived. " +
        "The upload API returned no URL metadata and fallback CDN candidates were not reachable as video.",
    );
    error.uploadDebug = {
      uploadAccepted: true,
      uploadResponseStatus: response.status,
      uploadResponseHeaders: responseHeaders,
      uploadResponseText: String(responseText || "").slice(0, 4000),
      candidateUrls: fallbackCandidates,
      provider: "multipart",
    };
    throw error;
  }

  try {
    await validateRemoteVideoUrl(cdnUrl, timeoutMs);
  } catch (validationError) {
    const error = new Error(
      `Remote upload accepted (HTTP 200), but CDN download validation failed for ${cdnUrl}: ` +
        `${String(validationError?.message || validationError || "unknown error")}`,
    );
    error.uploadDebug = {
      uploadAccepted: true,
      uploadResponseStatus: response.status,
      uploadResponseHeaders: responseHeaders,
      uploadResponseText: String(responseText || "").slice(0, 4000),
      candidateUrls: [cdnUrl, ...fallbackCandidates.filter((url) => url !== cdnUrl)],
      provider: "multipart",
    };
    throw error;
  }
  return {
    cdnUrl,
    previewCdnUrl: "",
    remoteResponse,
    uploadResponseStatus: response.status,
    uploadResponseHeaders: responseHeaders,
    uploadResponseText: String(responseText || "").slice(0, 4000),
  };
}

function buildMultipartCdnFallbackCandidates({ cdnBase, directory, fileName }) {
  const base = String(cdnBase || "").trim().replace(/\/+$/gu, "");
  const cleanDir = String(directory || "").trim().replace(/^\/+|\/+$/gu, "");
  const encodedFile = encodeURIComponent(String(fileName || "").trim());
  if (!base || !encodedFile) return [];

  const candidates = [];
  candidates.push(`${base}/${encodedFile}`);
  if (cleanDir) {
    const dirEncoded = cleanDir
      .split("/")
      .map((segment) => encodeURIComponent(decodeURIComponentSafe(segment)))
      .join("/");
    candidates.push(`${base}/${dirEncoded}/${encodedFile}`);
  }
  return [...new Set(candidates)];
}

async function findFirstReachableVideoUrl(candidates, timeoutMs) {
  for (const candidate of candidates) {
    const url = String(candidate || "").trim();
    if (!url) continue;
    try {
      await validateRemoteVideoUrl(url, timeoutMs);
      return url;
    } catch {
      // Continue trying other candidate URLs.
    }
  }
  return "";
}

async function publishFinalReelToS3({
  runDir,
  sourcePath,
  previewPath,
  fileName,
  previewFileName,
  directory,
  cdnBase,
  timeoutMs,
  config,
  log = () => {},
}) {
  const bucket = String(
    config.finalReelS3Bucket ??
      process.env.FINAL_REEL_S3_BUCKET ??
      process.env.S3_BUCKET ??
      process.env["cloud.aws.s3.bucket"] ??
      "",
  ).trim();
  const region = String(
    config.finalReelS3Region ??
      process.env.FINAL_REEL_S3_REGION ??
      process.env.AWS_REGION ??
      process.env.AWS_DEFAULT_REGION ??
      process.env["cloud.aws.region.static"] ??
      "ap-southeast-2",
  ).trim() || "ap-southeast-2";
  const accessKeyId = String(
    config.finalReelS3AccessKeyId ??
      process.env.FINAL_REEL_S3_ACCESS_KEY_ID ??
      process.env.AWS_ACCESS_KEY_ID ??
      process.env["cloud.aws.credentials.accessKey"] ??
      "",
  ).trim();
  const secretAccessKey = String(
    config.finalReelS3SecretAccessKey ??
      process.env.FINAL_REEL_S3_SECRET_ACCESS_KEY ??
      process.env.AWS_SECRET_ACCESS_KEY ??
      process.env["cloud.aws.credentials.secretKey"] ??
      "",
  ).trim();
  const sessionToken = String(
    config.finalReelS3SessionToken ??
      process.env.FINAL_REEL_S3_SESSION_TOKEN ??
      process.env.AWS_SESSION_TOKEN ??
      "",
  ).trim();
  const acl = String(
    config.finalReelS3Acl ??
      process.env.FINAL_REEL_S3_ACL ??
      "",
  ).trim();
  const explicitPublicBaseUrl = String(
    config.finalReelS3PublicBaseUrl ??
      process.env.FINAL_REEL_S3_PUBLIC_BASE_URL ??
      "",
  ).trim().replace(/\/+$/gu, "");
  const uploadPrefix = String(
    config.finalReelS3Prefix ?? process.env.FINAL_REEL_S3_PREFIX ?? directory,
  ).trim().replace(/^\/+|\/+$/gu, "");

  if (!bucket) {
    throw new Error("Missing FINAL_REEL_S3_BUCKET for S3 remote provider.");
  }
  if (!accessKeyId || !secretAccessKey) {
    throw new Error("Missing S3 credentials (access key / secret key) for S3 remote provider.");
  }

  const reelKey = buildS3Key(uploadPrefix, fileName);
  const previewKey = previewPath ? buildS3Key(uploadPrefix, previewFileName) : "";
  log(`Uploading final reel to S3 (s3://${bucket}/${reelKey})...`);
  await uploadBytesToS3({
    sourcePath,
    bucket,
    key: reelKey,
    region,
    accessKeyId,
    secretAccessKey,
    sessionToken,
    contentType: "video/mp4",
    acl,
    timeoutMs,
    cacheControl: "public, max-age=31536000, immutable",
  });
  if (previewPath && previewKey) {
    log(`Uploading preview reel to S3 (s3://${bucket}/${previewKey})...`);
    await uploadBytesToS3({
      sourcePath: previewPath,
      bucket,
      key: previewKey,
      region,
      accessKeyId,
      secretAccessKey,
      sessionToken,
      contentType: "video/mp4",
      acl,
      timeoutMs,
      cacheControl: "public, max-age=31536000, immutable",
    });
  }

  const publicBase = explicitPublicBaseUrl || (cdnBase && !isKnownImageOnlyCdn(cdnBase) ? cdnBase : "");
  const s3ObjectBase = `https://${bucket}.s3.${region}.amazonaws.com`;
  const s3PathStyleBase = `https://s3.${region}.amazonaws.com/${encodeURIComponent(bucket)}`;
  const shouldUsePathStyle = bucket.includes(".");
  const cdnUrl = publicBase
    ? `${publicBase}/${encodeURIComponent(fileName)}`
    : `${shouldUsePathStyle ? s3PathStyleBase : s3ObjectBase}/${encodeS3Key(reelKey)}`;
  const previewCdnUrl = previewKey
    ? (publicBase
        ? `${publicBase}/${encodeURIComponent(previewFileName)}`
        : `${shouldUsePathStyle ? s3PathStyleBase : s3ObjectBase}/${encodeS3Key(previewKey)}`)
    : "";

  await validateRemoteVideoUrl(cdnUrl, timeoutMs);
  if (previewCdnUrl) {
    await validateRemoteVideoUrl(previewCdnUrl, timeoutMs);
  }

  return {
    cdnUrl,
    previewCdnUrl,
    objectKey: reelKey,
    previewObjectKey: previewKey,
    bucket,
    region,
    publicBaseUrl: publicBase,
    provider: "s3",
  };
}

function parseBooleanLike(value, fallback = false) {
  const normalized = String(value ?? "").trim().toLowerCase();
  if (!normalized) return fallback;
  if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
  return fallback;
}

function resolveUploadCdnUrl({ remoteResponse, responseText, directory, fileName, cdnBase }) {
  const responseCandidates = [];
  const addCandidate = (value) => {
    const text = String(value ?? "").trim();
    if (!text) return;
    responseCandidates.push(text);
  };

  const walk = (node) => {
    if (node === null || node === undefined) return;
    if (typeof node === "string") {
      addCandidate(node);
      return;
    }
    if (Array.isArray(node)) {
      for (const item of node) walk(item);
      return;
    }
    if (typeof node === "object") {
      for (const [key, value] of Object.entries(node)) {
        const low = String(key).toLowerCase();
        if (
          low.includes("url") ||
          low.includes("path") ||
          low.includes("file") ||
          low.includes("key") ||
          low.includes("location")
        ) {
          addCandidate(value);
        }
        walk(value);
      }
    }
  };

  walk(remoteResponse);
  const textMatches = String(responseText || "").match(/https?:\/\/[^\s"']+/giu) || [];
  for (const match of textMatches) addCandidate(match);

  const preferred = selectBestUrlCandidate(responseCandidates, fileName);
  if (preferred) {
    return preferred;
  }

  const relPath = selectBestPathCandidate(responseCandidates, directory, fileName);
  if (relPath) {
    return joinCdnUrl(cdnBase, relPath);
  }

  return null;
}

function selectBestUrlCandidate(candidates, fileName) {
  const wanted = String(fileName || "").toLowerCase();
  const urls = candidates
    .map((v) => String(v || "").trim())
    .filter((v) => /^https?:\/\//iu.test(v));
  if (!urls.length) return "";
  const mp4Url = urls.find((u) => u.toLowerCase().includes(".mp4"));
  if (mp4Url) return mp4Url;
  const named = urls.find((u) => wanted && decodeURIComponent(u).toLowerCase().includes(wanted));
  if (named) return named;
  return urls[0];
}

function selectBestPathCandidate(candidates, directory, fileName) {
  const cleanDir = String(directory || "").replace(/^\/+|\/+$/gu, "");
  const wanted = String(fileName || "").toLowerCase();
  const values = candidates.map((v) => String(v || "").trim()).filter(Boolean);
  const pathLike = values.filter(
    (v) =>
      !/^https?:\/\//iu.test(v) &&
      (v.includes("/") || v.toLowerCase().endsWith(".mp4")),
  );
  if (!pathLike.length) return "";

  const named = pathLike.find((p) => decodeURIComponent(p).toLowerCase().includes(wanted));
  if (named) return normalizeRelativePath(named);
  const inDir = pathLike.find((p) => normalizeRelativePath(p).startsWith(`${cleanDir}/`));
  if (inDir) return normalizeRelativePath(inDir);
  return normalizeRelativePath(pathLike[0]);
}

function normalizeRelativePath(value) {
  return String(value || "")
    .replace(/^https?:\/\/[^/]+/iu, "")
    .replace(/^\/+/u, "")
    .trim();
}

function joinCdnUrl(base, relPath) {
  const cleanBase = String(base || "").replace(/\/+$/gu, "");
  const cleanRel = String(relPath || "").replace(/^\/+/gu, "");
  return `${cleanBase}/${cleanRel
    .split("/")
    .map((segment) => encodeURIComponent(decodeURIComponentSafe(segment)))
    .join("/")}`;
}

function decodeURIComponentSafe(value) {
  try {
    return decodeURIComponent(String(value || ""));
  } catch {
    return String(value || "");
  }
}

async function validateRemoteVideoUrl(url, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.max(5000, timeoutMs || 180000));
  try {
    let response = await fetch(url, {
      method: "HEAD",
      redirect: "follow",
      signal: controller.signal,
    });
    if (response.status === 405 || response.status === 501) {
      response = await fetch(url, {
        method: "GET",
        headers: { Range: "bytes=0-0" },
        redirect: "follow",
        signal: controller.signal,
      });
    }

    const contentType = String(response.headers.get("content-type") || "").toLowerCase();
    if (!response.ok) {
      const body = await safeReadBody(response);
      throw new Error(`CDN URL returned ${response.status}. Body: ${body.slice(0, 300)}`);
    }
    if (contentType.includes("application/json") || contentType.includes("text/html")) {
      const body = await safeReadBody(response);
      throw new Error(
        `CDN URL is not serving a video payload (content-type: ${contentType || "unknown"}). ` +
          `Body: ${body.slice(0, 300)}`,
      );
    }
  } finally {
    clearTimeout(timer);
  }
}

async function safeReadBody(response) {
  try {
    return String(await response.text());
  } catch {
    return "";
  }
}

async function uploadBytesToS3({
  sourcePath,
  bucket,
  key,
  region,
  accessKeyId,
  secretAccessKey,
  sessionToken = "",
  contentType = "application/octet-stream",
  acl = "",
  timeoutMs = 180000,
  cacheControl = "",
}) {
  const body = await fs.readFile(sourcePath);
  const amzDate = toAmzDate(new Date());
  const shortDate = amzDate.slice(0, 8);
  const forcePathStyle = bucket.includes(".");
  const host = forcePathStyle ? `s3.${region}.amazonaws.com` : `${bucket}.s3.${region}.amazonaws.com`;
  const uriPath = forcePathStyle
    ? `/${encodeURIComponent(bucket)}/${encodeS3Key(key)}`
    : `/${encodeS3Key(key)}`;
  const payloadHash = sha256Hex(body);
  const credentialScope = `${shortDate}/${region}/s3/aws4_request`;

  const headers = {
    host,
    "content-type": contentType,
    "x-amz-content-sha256": payloadHash,
    "x-amz-date": amzDate,
  };
  if (cacheControl) {
    headers["cache-control"] = cacheControl;
  }
  if (sessionToken) {
    headers["x-amz-security-token"] = sessionToken;
  }
  if (acl) {
    headers["x-amz-acl"] = acl;
  }

  const signedHeaderKeys = Object.keys(headers).sort();
  const canonicalHeaders = signedHeaderKeys
    .map((name) => `${name}:${String(headers[name]).trim().replace(/\s+/gu, " ")}`)
    .join("\n");
  const signedHeaders = signedHeaderKeys.join(";");
  const canonicalRequest = [
    "PUT",
    uriPath,
    "",
    `${canonicalHeaders}\n`,
    signedHeaders,
    payloadHash,
  ].join("\n");
  const stringToSign = [
    "AWS4-HMAC-SHA256",
    amzDate,
    credentialScope,
    sha256Hex(Buffer.from(canonicalRequest, "utf8")),
  ].join("\n");
  const signature = hmacHex(
    getSignatureKey(secretAccessKey, shortDate, region, "s3"),
    stringToSign,
  );
  const authorization = `AWS4-HMAC-SHA256 Credential=${accessKeyId}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;

  const requestHeaders = {
    ...headers,
    Authorization: authorization,
    "Content-Length": String(body.byteLength),
  };

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.max(5000, timeoutMs || 180000));
  let response;
  try {
    response = await fetch(`https://${host}${uriPath}`, {
      method: "PUT",
      headers: requestHeaders,
      body,
      redirect: "manual",
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timer);
  }

  if (!response?.ok) {
    const bodyText = await safeReadBody(response);
    const awsError = parseAwsXmlError(bodyText);
    const bucketRegionHint = String(response.headers.get("x-amz-bucket-region") || "").trim();
    const requestId = String(response.headers.get("x-amz-request-id") || "").trim();
    const hostId = String(response.headers.get("x-amz-id-2") || "").trim();
    const details = [
      awsError.code ? `code=${awsError.code}` : "",
      awsError.message ? `message=${awsError.message}` : "",
      awsError.bucketName ? `bucket=${awsError.bucketName}` : "",
      awsError.resource ? `resource=${awsError.resource}` : "",
      bucketRegionHint ? `bucket-region=${bucketRegionHint}` : "",
      requestId ? `request-id=${requestId}` : "",
      hostId ? `host-id=${hostId}` : "",
    ].filter(Boolean).join(", ");
    throw new Error(
      `S3 upload failed for ${key} (${response?.status || "no-status"})` +
        (details ? ` [${details}]` : "") +
        `: ${String(bodyText || "").slice(0, 500)}`,
    );
  }
}

function parseAwsXmlError(xmlText) {
  const text = String(xmlText || "");
  const getTag = (tagName) => {
    const re = new RegExp(`<${tagName}>([\\s\\S]*?)</${tagName}>`, "i");
    const match = text.match(re);
    return match ? String(match[1] || "").trim() : "";
  };
  return {
    code: getTag("Code"),
    message: getTag("Message"),
    resource: getTag("Resource"),
    bucketName: getTag("BucketName"),
  };
}

function buildS3Key(prefix, fileName) {
  const cleanPrefix = String(prefix || "").replace(/^\/+|\/+$/gu, "");
  const cleanFile = String(fileName || "").replace(/^\/+/gu, "");
  if (!cleanPrefix) return cleanFile;
  return `${cleanPrefix}/${cleanFile}`;
}

function encodeS3Key(key) {
  return String(key || "")
    .split("/")
    .map((segment) => encodeURIComponent(decodeURIComponentSafe(segment)))
    .join("/");
}

function toAmzDate(date) {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, "0");
  const d = String(date.getUTCDate()).padStart(2, "0");
  const hh = String(date.getUTCHours()).padStart(2, "0");
  const mm = String(date.getUTCMinutes()).padStart(2, "0");
  const ss = String(date.getUTCSeconds()).padStart(2, "0");
  return `${y}${m}${d}T${hh}${mm}${ss}Z`;
}

function sha256Hex(value) {
  return createHash("sha256").update(value).digest("hex");
}

function hmac(key, value, encoding = undefined) {
  return createHmac("sha256", key).update(value, encoding).digest();
}

function hmacHex(key, value) {
  return createHmac("sha256", key).update(value, "utf8").digest("hex");
}

function getSignatureKey(secretAccessKey, dateStamp, regionName, serviceName) {
  const kDate = hmac(Buffer.from(`AWS4${secretAccessKey}`, "utf8"), dateStamp, "utf8");
  const kRegion = hmac(kDate, regionName, "utf8");
  const kService = hmac(kRegion, serviceName, "utf8");
  return hmac(kService, "aws4_request", "utf8");
}

function isKnownImageOnlyCdn(url) {
  const text = String(url || "").toLowerCase();
  return text.includes("storage.importautos.com.au");
}

async function writePublishManifest(manifestPath, payload) {
  await fs.writeFile(manifestPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

function pickResponseHeaders(response) {
  if (!response?.headers || typeof response.headers.entries !== "function") {
    return {};
  }
  const picked = {};
  for (const [key, value] of response.headers.entries()) {
    if (!key) continue;
    picked[String(key).toLowerCase()] = String(value ?? "");
  }
  return picked;
}

function sanitizeRemoteFileName(value) {
  return String(value || "final-reel.mp4")
    .trim()
    .replace(/[^a-z0-9._-]+/giu, "-")
    .replace(/-+/gu, "-")
    .replace(/^-|-$/gu, "") || "final-reel.mp4";
}

function tryParseJson(value) {
  const text = String(value || "").trim();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

async function readJsonIfExists(filePath) {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function statIfExists(filePath) {
  try {
    return await fs.stat(filePath);
  } catch {
    return null;
  }
}

function runProcess(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd ?? process.cwd(),
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stderr = "";
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(stderr.trim() || `Command failed (${code}): ${command} ${args.join(" ")}`));
    });
  });
}

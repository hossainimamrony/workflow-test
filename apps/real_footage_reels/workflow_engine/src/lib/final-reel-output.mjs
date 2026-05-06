import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { spawn } from "node:child_process";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

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

  const autoDeleteSourceFootage = parseBooleanLike(
    config.finalReelAutoDeleteSourceFootage ??
      process.env.FINAL_REEL_AUTO_DELETE_SOURCE_FOOTAGE,
    true,
  );
  if (autoDeleteSourceFootage) {
    await cleanupSourceFootageAfterFinalized(normalizedDir, log);
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
      "",
  ).trim();
  const directory = String(
    config.finalReelUploadDirectory ??
      process.env.FINAL_REEL_UPLOAD_DIRECTORY ??
      process.env.S3_UPLOAD_DIRECTORY ??
      "social-media-content/reels",
  ).trim().replace(/^\/+|\/+$/gu, "");
  const rawBaseUrl = String(
    config.finalReelRawBaseUrl ??
      config.finalReelS3RawBaseUrl ??
      process.env.FINAL_REEL_RAW_BASE_URL ??
      process.env.S3_RAW_BASE_URL ??
      config.finalReelS3PublicBaseUrl ??
      process.env.FINAL_REEL_S3_PUBLIC_BASE_URL ??
      "",
  ).trim().replace(/\/+$/gu, "");
  const s3Bucket = String(
    config.finalReelS3Bucket ??
      process.env.FINAL_REEL_S3_BUCKET ??
      process.env.S3_BUCKET ??
      process.env["cloud.aws.s3.bucket"] ??
      "",
  ).trim();
  const s3Region = String(
    config.finalReelS3Region ??
      process.env.FINAL_REEL_S3_REGION ??
      process.env.AWS_REGION ??
      process.env.AWS_DEFAULT_REGION ??
      process.env["cloud.aws.region.static"] ??
      "ap-southeast-2",
  ).trim() || "ap-southeast-2";
  const providerFromConfig = String(
    config.finalReelRemoteProvider ??
      process.env.FINAL_REEL_REMOTE_PROVIDER ??
      "",
  ).trim().toLowerCase();
  const provider =
    (providerFromConfig === "s3" || providerFromConfig === "multipart")
      ? providerFromConfig
      : (s3Bucket ? "s3" : (endpoint ? "multipart" : "s3"));

  if (!directory) {
    throw new Error("Remote upload directory is empty. Set FINAL_REEL_UPLOAD_DIRECTORY.");
  }

  const sourceMtime = Math.trunc(sourceStats.mtimeMs);
  const previewMtime = previewStats ? Math.trunc(previewStats.mtimeMs) : 0;
  const manifestPath = path.join(runDir, FINAL_REEL_PUBLISH_MANIFEST);
  const existing = await readJsonIfExists(manifestPath);
  const forceRemoteUpload = parseBooleanLike(
    config.finalReelForceRemoteUpload ?? process.env.FINAL_REEL_FORCE_REMOTE_UPLOAD,
    true,
  );
  if (
    !forceRemoteUpload &&
    existing &&
    String(existing.provider || "") === provider &&
    Number(existing.sourceMtimeMs) === sourceMtime &&
    Number(existing.previewSourceMtimeMs || 0) === previewMtime &&
    String(existing.remoteUrl || existing.cdnUrl || "").startsWith("http")
  ) {
    return String(existing.remoteUrl || existing.cdnUrl || "");
  }

  const runId = path.basename(path.resolve(runDir));
  const namingContext = await buildVideoNamingContext(runDir, config, runId);
  const fileName = sanitizeRemoteFileName(`${namingContext.baseName}-final-reel.mp4`);
  const previewFileName = sanitizeRemoteFileName(`${namingContext.baseName}-final-reel-preview.mp4`);
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
    rawBaseUrl,
    s3Bucket,
    s3Region,
    stockId: namingContext.stockId,
    make: namingContext.make,
    model: namingContext.model,
    runId,
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
            timeoutMs,
            config,
            log,
          })
        : await publishFinalReelToMultipartApi({
            sourcePath,
            fileName,
            directory,
            endpoint,
            rawBaseUrl,
            s3Bucket,
            s3Region,
            timeoutMs,
            log,
          });

    await writePublishManifest(manifestPath, {
      ...manifestBase,
      ok: true,
      ...uploadResult,
    });
    log(`Remote final reel ready: ${uploadResult.remoteUrl || uploadResult.cdnUrl || "(no-url)"}`);
    return String(uploadResult.remoteUrl || uploadResult.cdnUrl || "");
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
  rawBaseUrl,
  s3Bucket,
  s3Region,
  timeoutMs,
  log = () => {},
}) {
  if (!endpoint) {
    throw new Error("Remote upload endpoint is empty. Set FINAL_REEL_UPLOAD_ENDPOINT or S3_UPLOAD_URL.");
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
  const resolvedUrl = resolveUploadPublicUrl({
    remoteResponse,
    responseText,
    directory,
    fileName,
    rawBaseUrl,
    s3Bucket,
    s3Region,
  });
  const fallbackCandidates = buildMultipartPublicFallbackCandidates({
    rawBaseUrl,
    s3Bucket,
    s3Region,
    directory,
    fileName,
  });
  const candidateUrls = uniqueNonEmpty([resolvedUrl, ...fallbackCandidates]);
  const reachableRemoteUrl = await findFirstReachableVideoUrl(candidateUrls, timeoutMs);
  const remoteUrl = reachableRemoteUrl || candidateUrls[0] || "";
  const downloadCheckOk = Boolean(reachableRemoteUrl);
  const downloadCheckError = downloadCheckOk
    ? ""
    : "Upload accepted but no candidate public URL returned a video payload.";
  return {
    remoteUrl,
    previewRemoteUrl: "",
    cdnUrl: remoteUrl,
    previewCdnUrl: "",
    remoteResponse,
    uploadAccepted: true,
    uploadOk: true,
    downloadCheckOk,
    downloadCheckError,
    uploadResponseStatus: response.status,
    uploadResponseHeaders: responseHeaders,
    uploadResponseText: String(responseText || "").slice(0, 4000),
    candidateUrls,
  };
}

function buildMultipartPublicFallbackCandidates({
  rawBaseUrl,
  s3Bucket,
  s3Region,
  directory,
  fileName,
}) {
  const bases = buildMultipartBaseUrls({ rawBaseUrl, s3Bucket, s3Region });
  const cleanDir = String(directory || "").trim().replace(/^\/+|\/+$/gu, "");
  const encodedFile = encodeURIComponent(String(fileName || "").trim());
  if (!encodedFile) return [];

  const candidates = [];
  const dirEncoded = cleanDir
    ? cleanDir
      .split("/")
      .map((segment) => encodeURIComponent(decodeURIComponentSafe(segment)))
      .join("/")
    : "";
  for (const base of bases) {
    candidates.push(`${base}/${encodedFile}`);
    if (dirEncoded) {
      candidates.push(`${base}/${dirEncoded}/${encodedFile}`);
    }
  }
  return uniqueNonEmpty(candidates);
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
  const cacheControl = String(
    config.finalReelS3CacheControl ??
      process.env.FINAL_REEL_S3_CACHE_CONTROL ??
      "no-cache, max-age=0, must-revalidate",
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

  const s3 = new S3Client({
    region,
    credentials: {
      accessKeyId,
      secretAccessKey,
      ...(sessionToken ? { sessionToken } : {}),
    },
    forcePathStyle: bucket.includes("."),
  });

  const reelKey = buildS3Key(uploadPrefix, fileName);
  const previewKey = previewPath ? buildS3Key(uploadPrefix, previewFileName) : "";
  const reelBytes = await fs.readFile(sourcePath);
  log(`Uploading final reel to S3 (s3://${bucket}/${reelKey})...`);
  let reelUpload;
  try {
    reelUpload = await s3.send(new PutObjectCommand({
      Bucket: bucket,
      Key: reelKey,
      Body: reelBytes,
      ContentType: "video/mp4",
      CacheControl: cacheControl,
      ...(acl ? { ACL: acl } : {}),
    }));
  } catch (error) {
    throw new Error(
      `S3 SDK upload failed for s3://${bucket}/${reelKey}: ${String(error?.message || error || "unknown error")}`,
    );
  }
  if (previewPath && previewKey) {
    const previewBytes = await fs.readFile(previewPath);
    log(`Uploading preview reel to S3 (s3://${bucket}/${previewKey})...`);
    try {
      await s3.send(new PutObjectCommand({
        Bucket: bucket,
        Key: previewKey,
        Body: previewBytes,
        ContentType: "video/mp4",
        CacheControl: cacheControl,
        ...(acl ? { ACL: acl } : {}),
      }));
    } catch (error) {
      throw new Error(
        `S3 SDK upload failed for preview s3://${bucket}/${previewKey}: ${String(error?.message || error || "unknown error")}`,
      );
    }
  }

  const publicBase = explicitPublicBaseUrl;
  const s3ObjectBase = `https://${bucket}.s3.${region}.amazonaws.com`;
  const s3PathStyleBase = `https://s3.${region}.amazonaws.com/${encodeURIComponent(bucket)}`;
  const shouldUsePathStyle = bucket.includes(".");
  const remoteUrl = publicBase
    ? buildObjectUrlFromPublicBase(publicBase, reelKey)
    : `${shouldUsePathStyle ? s3PathStyleBase : s3ObjectBase}/${encodeS3Key(reelKey)}`;
  const previewRemoteUrl = previewKey
    ? (publicBase
        ? buildObjectUrlFromPublicBase(publicBase, previewKey)
        : `${shouldUsePathStyle ? s3PathStyleBase : s3ObjectBase}/${encodeS3Key(previewKey)}`)
    : "";
  const { ok: downloadCheckOk, error: downloadCheckError } = await tryValidateRemoteVideoUrl(remoteUrl, timeoutMs);
  const previewDownloadCheck = previewRemoteUrl
    ? await tryValidateRemoteVideoUrl(previewRemoteUrl, timeoutMs)
    : { ok: true, error: "" };

  return {
    remoteUrl,
    previewRemoteUrl,
    cdnUrl: remoteUrl,
    previewCdnUrl: previewRemoteUrl,
    objectKey: reelKey,
    previewObjectKey: previewKey,
    bucket,
    region,
    publicBaseUrl: publicBase,
    uploadAccepted: true,
    uploadOk: true,
    uploadETag: String(reelUpload?.ETag || "").trim(),
    uploadVersionId: String(reelUpload?.VersionId || "").trim(),
    downloadCheckOk: downloadCheckOk && previewDownloadCheck.ok,
    downloadCheckError: joinNonEmpty(
      downloadCheckError,
      previewDownloadCheck.ok ? "" : `Preview URL validation failed: ${previewDownloadCheck.error}`,
    ),
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

function resolveUploadPublicUrl({
  remoteResponse,
  responseText,
  directory,
  fileName,
  rawBaseUrl,
  s3Bucket,
  s3Region,
}) {
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
    const baseCandidates = buildMultipartBaseUrls({ rawBaseUrl, s3Bucket, s3Region });
    for (const base of baseCandidates) {
      return joinBaseUrl(base, relPath);
    }
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

function joinBaseUrl(base, relPath) {
  const cleanBase = String(base || "").replace(/\/+$/gu, "");
  if (!cleanBase) return "";
  const cleanRel = String(relPath || "").replace(/^\/+/gu, "");
  return `${cleanBase}/${cleanRel
    .split("/")
    .map((segment) => encodeURIComponent(decodeURIComponentSafe(segment)))
    .join("/")}`;
}

function buildMultipartBaseUrls({ rawBaseUrl, s3Bucket, s3Region }) {
  const bases = [];
  const cleanedRawBase = String(rawBaseUrl || "").trim().replace(/\/+$/gu, "");
  if (cleanedRawBase) {
    bases.push(cleanedRawBase);
  }

  const bucket = String(s3Bucket || "").trim();
  const region = String(s3Region || "").trim();
  if (bucket && region) {
    const virtualHostedBase = `https://${bucket}.s3.${region}.amazonaws.com`;
    const pathStyleBase = `https://s3.${region}.amazonaws.com/${encodeURIComponent(bucket)}`;
    bases.push(virtualHostedBase);
    if (bucket.includes(".")) {
      bases.push(pathStyleBase);
    } else {
      // Keep path-style as a fallback for S3-compatible backends/fronting proxies.
      bases.push(pathStyleBase);
    }
  }

  return uniqueNonEmpty(bases);
}

function uniqueNonEmpty(values) {
  const out = [];
  const seen = new Set();
  for (const value of values || []) {
    const text = String(value || "").trim();
    if (!text || seen.has(text)) continue;
    seen.add(text);
    out.push(text);
  }
  return out;
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
      throw new Error(`Remote URL returned ${response.status}. Body: ${body.slice(0, 300)}`);
    }
    if (contentType.includes("application/json") || contentType.includes("text/html")) {
      const body = await safeReadBody(response);
      throw new Error(
        `Remote URL is not serving a video payload (content-type: ${contentType || "unknown"}). ` +
          `Body: ${body.slice(0, 300)}`,
      );
    }
  } finally {
    clearTimeout(timer);
  }
}

async function tryValidateRemoteVideoUrl(url, timeoutMs) {
  const normalized = String(url || "").trim();
  if (!normalized) {
    return { ok: false, error: "No remote URL was provided for validation." };
  }
  try {
    await validateRemoteVideoUrl(normalized, timeoutMs);
    return { ok: true, error: "" };
  } catch (error) {
    return { ok: false, error: String(error?.message || error || "validation failed") };
  }
}

async function safeReadBody(response) {
  try {
    return String(await response.text());
  } catch {
    return "";
  }
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

function buildObjectUrlFromPublicBase(publicBase, objectKey) {
  const base = String(publicBase || "").trim().replace(/\/+$/gu, "");
  const cleanKey = String(objectKey || "").trim().replace(/^\/+/u, "");
  if (!base || !cleanKey) {
    return "";
  }

  try {
    const parsed = new URL(base);
    const basePathSegments = parsed.pathname
      .split("/")
      .filter(Boolean)
      .map((segment) => decodeURIComponentSafe(segment));
    const keySegments = cleanKey
      .split("/")
      .filter(Boolean)
      .map((segment) => decodeURIComponentSafe(segment));
    const keyPrefix = keySegments.slice(0, -1);
    const keyFile = keySegments[keySegments.length - 1] || "";
    const baseMatchesKeyPrefix =
      basePathSegments.length > 0 &&
      basePathSegments.length === keyPrefix.length &&
      basePathSegments.every((segment, index) => segment === keyPrefix[index]);
    if (baseMatchesKeyPrefix && keyFile) {
      return `${base}/${encodeURIComponent(keyFile)}`;
    }
  } catch {
    // Keep fallback behavior below.
  }

  return `${base}/${encodeS3Key(cleanKey)}`;
}

function joinNonEmpty(...parts) {
  return parts
    .map((value) => String(value || "").trim())
    .filter(Boolean)
    .join(" ");
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

async function buildVideoNamingContext(runDir, config = {}, fallbackRunId = "") {
  const manifest = await readJsonIfExists(path.join(runDir, "downloads-manifest.json"));
  const stockId = normalizeSlugSegment(
    firstNonEmpty(config.stockId, manifest?.stockId, fallbackRunId),
    "run",
  );
  const directMake = firstNonEmpty(config.make, manifest?.make, manifest?.listingMake);
  const directModel = firstNonEmpty(config.model, manifest?.model, manifest?.listingModel);
  const listingTitle = String(firstNonEmpty(config.listingTitle, manifest?.listingTitle)).trim();
  const inferred = inferMakeModelFromListingTitle(listingTitle);
  const make = normalizeSlugSegment(firstNonEmpty(directMake, inferred.make), "vehicle");
  const model = normalizeSlugSegment(firstNonEmpty(directModel, inferred.model), "model");
  return {
    stockId,
    make,
    model,
    baseName: `${stockId}-${make}-${model}`,
  };
}

function inferMakeModelFromListingTitle(listingTitle) {
  const tokens = String(listingTitle || "")
    .trim()
    .split(/\s+/u)
    .filter(Boolean);
  if (!tokens.length) {
    return { make: "", model: "" };
  }
  const first = tokens[0] || "";
  const hasYearPrefix = /^\d{4}$/u.test(first);
  const makeIndex = hasYearPrefix ? 1 : 0;
  const modelIndex = hasYearPrefix ? 2 : 1;
  return {
    make: tokens[makeIndex] || "",
    model: tokens[modelIndex] || "",
  };
}

function normalizeSlugSegment(value, fallback) {
  const cleaned = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, "-")
    .replace(/-+/gu, "-")
    .replace(/^-|-$/gu, "");
  return cleaned || fallback;
}

function firstNonEmpty(...values) {
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (text) return text;
  }
  return "";
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

async function cleanupSourceFootageAfterFinalized(runDir, log = () => {}) {
  const keepNames = new Set(["final-reel.mp4"]);
  const removableExt = new Set([".mov", ".mp4"]);
  const removableDirs = ["downloads", "download", "raw", "cache", "clips", "tmp", "temp", "samples"];
  let deletedFiles = 0;
  let deletedDirs = 0;

  for (const dirName of removableDirs) {
    const dirPath = path.join(runDir, dirName);
    try {
      await fs.rm(dirPath, { recursive: true, force: true });
      deletedDirs += 1;
    } catch {
      // best-effort cleanup
    }
  }

  const allPaths = [];
  await collectAllPaths(runDir, allPaths);
  for (const filePath of allPaths) {
    const baseName = path.basename(filePath).toLowerCase();
    const ext = path.extname(filePath).toLowerCase();
    if (!removableExt.has(ext)) continue;
    if (keepNames.has(baseName)) continue;
    try {
      await fs.unlink(filePath);
      deletedFiles += 1;
    } catch {
      // best-effort cleanup
    }
  }
  if (deletedFiles > 0 || deletedDirs > 0) {
    log(`Source footage cleanup complete: deleted ${deletedFiles} media file(s), ${deletedDirs} folder(s).`);
  }
}

async function collectAllPaths(dirPath, out) {
  let entries = [];
  try {
    entries = await fs.readdir(dirPath, { withFileTypes: true });
  } catch {
    return;
  }
  for (const entry of entries) {
    const fullPath = path.join(dirPath, entry.name);
    if (entry.isDirectory()) {
      await collectAllPaths(fullPath, out);
      continue;
    }
    if (entry.isFile()) {
      out.push(fullPath);
    }
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

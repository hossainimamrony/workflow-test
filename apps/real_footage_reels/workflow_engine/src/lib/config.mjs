import { readFileSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { getLockedTargetSequence } from "./reel-rules.mjs";

const IS_WINDOWS = process.platform === "win32";
const DEFAULT_PYTHON_PATH = IS_WINDOWS ? "python" : "python3";
const DEFAULT_FFMPEG_PATH = IS_WINDOWS ? "ffmpeg.exe" : "ffmpeg";
const DEFAULT_COMPOSE_WIDTH = 1080;
const DEFAULT_COMPOSE_HEIGHT = 1920;
const FAST_COMPOSE_WIDTH = 720;
const FAST_COMPOSE_HEIGHT = 1280;
const DEFAULT_COMPOSE_FPS = 30;
const FAST_COMPOSE_FPS = 24;
const DEFAULT_COMPOSE_MAIN_DURATION_SECONDS = 14;
const DEFAULT_END_SCENE_DURATION_SECONDS = 3.5;
const DEFAULT_MAX_TOTAL_REEL_DURATION_SECONDS = 17;
const DEFAULT_CLIP_START_SKIP_SECONDS = 2;
const MAX_CLIP_START_SKIP_SECONDS = 2;
const DEFAULT_MP4_VIDEO_CODEC = "libx264";
const DEFAULT_MP4_PRESET = "medium";
const DEFAULT_MP4_CRF = 18;
const GEMINI_KEY_PLACEHOLDERS = new Set([
  "",
  "your_gemini_api_key_here",
  "your_api_key_here",
  "paste_your_gemini_api_key_here",
  "replace_with_your_gemini_api_key",
  "replace_me",
  "changeme",
]);

export function loadConfigFromCli(argv) {
  const args = parseArgs(argv);
  const env = loadEnvConfig(process.cwd());

  return createRuntimeConfig(
    {
      command: args._[0] ?? "help",
      urls: arrayValue(args.url),
      outDir: args.out ?? null,
      shotsPerClip: numberValue(args.shots, 3),
      maxClips: args["max-clips"] ? numberValue(args["max-clips"]) : null,
      compose: Boolean(args.compose),
      headless: !args.headful,
      browserPath: args.browser || null,
      pythonPath: args.python || null,
      ffmpegPath: args.ffmpeg || null,
      geminiApiKey: null,
      geminiModel: args.model || null,
      composeWidth: numberValue(args.width, DEFAULT_COMPOSE_WIDTH),
      composeHeight: numberValue(args.height, DEFAULT_COMPOSE_HEIGHT),
      composeFps: numberValue(args.fps, 30),
      clipStartSkipSeconds: numberValue(args["clip-start"], DEFAULT_CLIP_START_SKIP_SECONDS),
    },
    env,
  );
}

export function createRuntimeConfig(input = {}, env = loadEnvConfig(process.cwd())) {
  const outDir = input.outDir ? path.resolve(process.cwd(), input.outDir) : null;
  const resolvedGeminiApiKey = normalizeGeminiApiKey(
    input.geminiApiKey ?? env.GEMINI_API_KEY ?? process.env.GEMINI_API_KEY ?? "",
  );
  const strictEndScene = parseBooleanLike(
    firstEnv(input.endSceneStrict, env.END_SCENE_STRICT, process.env.END_SCENE_STRICT),
    true,
  );
  const fastRender = resolveFastRenderMode(input, env);
  const defaultComposeWidth = fastRender ? FAST_COMPOSE_WIDTH : DEFAULT_COMPOSE_WIDTH;
  const defaultComposeHeight = fastRender ? FAST_COMPOSE_HEIGHT : DEFAULT_COMPOSE_HEIGHT;
  const defaultComposeFps = fastRender ? FAST_COMPOSE_FPS : DEFAULT_COMPOSE_FPS;
  const webmCodec = String(
    firstEnv(
      input.webmCodec,
      env.WEBM_CODEC,
      process.env.WEBM_CODEC,
      fastRender ? "libvpx" : "libvpx-vp9",
    ),
  ).trim();
  const webmDeadline = String(
    firstEnv(
      input.webmDeadline,
      env.WEBM_DEADLINE,
      process.env.WEBM_DEADLINE,
      fastRender ? "good" : "",
    ),
  ).trim();
  const webmCpuUsed = numberValue(
    firstEnv(input.webmCpuUsed, env.WEBM_CPU_USED, process.env.WEBM_CPU_USED),
    fastRender ? 5 : null,
  );
  const webmCrf = numberValue(
    firstEnv(input.webmCrf, env.WEBM_CRF, process.env.WEBM_CRF),
    fastRender ? 34 : 30,
  );
  const webmThreads = numberValue(
    firstEnv(input.webmThreads, env.WEBM_THREADS, process.env.WEBM_THREADS),
    fastRender ? 2 : null,
  );
  const mp4VideoCodec = String(
    firstEnv(
      input.mp4VideoCodec,
      env.MP4_VIDEO_CODEC,
      process.env.MP4_VIDEO_CODEC,
      DEFAULT_MP4_VIDEO_CODEC,
    ),
  ).trim() || DEFAULT_MP4_VIDEO_CODEC;
  const mp4Preset = String(
    firstEnv(
      input.mp4Preset,
      env.MP4_PRESET,
      process.env.MP4_PRESET,
      DEFAULT_MP4_PRESET,
    ),
  ).trim() || DEFAULT_MP4_PRESET;
  const mp4Crf = numberValue(
    firstEnv(input.mp4Crf, env.MP4_CRF, process.env.MP4_CRF),
    fastRender ? 20 : DEFAULT_MP4_CRF,
  );
  const mp4Threads = numberValue(
    firstEnv(
      input.mp4Threads,
      env.MP4_THREADS,
      process.env.MP4_THREADS,
      input.webmThreads,
      env.WEBM_THREADS,
      process.env.WEBM_THREADS,
    ),
    fastRender ? 2 : null,
  );
  const rawWebmCodec = firstEnv(input.webmCodec, env.WEBM_CODEC, process.env.WEBM_CODEC);
  const rawEndSceneSupersample = firstEnv(
    input.endSceneSupersample,
    env.END_SCENE_SUPERSAMPLE,
    process.env.END_SCENE_SUPERSAMPLE,
  );
  const reelDurations = resolveReelDurations(input);
  let composeWidth = numberValue(
    firstEnv(input.composeWidth, env.COMPOSE_WIDTH, process.env.COMPOSE_WIDTH),
    defaultComposeWidth,
  );
  let composeHeight = numberValue(
    firstEnv(input.composeHeight, env.COMPOSE_HEIGHT, process.env.COMPOSE_HEIGHT),
    defaultComposeHeight,
  );
  let composeFps = numberValue(
    firstEnv(input.composeFps, env.COMPOSE_FPS, process.env.COMPOSE_FPS),
    defaultComposeFps,
  );
  let endSceneSupersample = numberValue(
    rawEndSceneSupersample,
    fastRender ? 1 : 2,
  );
  let webmCodecFinal = webmCodec;
  let webmCrfFinal = Number.isFinite(Number(webmCrf)) ? Number(webmCrf) : (fastRender ? 34 : 30);
  let webmCpuUsedFinal = Number.isFinite(Number(webmCpuUsed)) ? Number(webmCpuUsed) : (fastRender ? 5 : null);
  let webmDeadlineFinal = webmDeadline;
  let webmThreadsFinal = Number.isFinite(Number(webmThreads)) ? Number(webmThreads) : null;
  let mp4VideoCodecFinal = mp4VideoCodec || DEFAULT_MP4_VIDEO_CODEC;
  let mp4PresetFinal = mp4Preset || DEFAULT_MP4_PRESET;
  let mp4CrfFinal = Number.isFinite(Number(mp4Crf)) ? Number(mp4Crf) : DEFAULT_MP4_CRF;
  let mp4ThreadsFinal = Number.isFinite(Number(mp4Threads)) ? Number(mp4Threads) : null;
  const previewEnabled = parseBooleanLike(
    firstEnv(input.previewEnabled, env.REAL_FOOTAGE_PREVIEW_ENABLED, process.env.REAL_FOOTAGE_PREVIEW_ENABLED),
    true,
  );
  const previewMaxWidth = numberValue(
    firstEnv(input.previewMaxWidth, env.REAL_FOOTAGE_PREVIEW_MAX_WIDTH, process.env.REAL_FOOTAGE_PREVIEW_MAX_WIDTH),
    720,
  );
  const previewMaxHeight = numberValue(
    firstEnv(input.previewMaxHeight, env.REAL_FOOTAGE_PREVIEW_MAX_HEIGHT, process.env.REAL_FOOTAGE_PREVIEW_MAX_HEIGHT),
    1280,
  );
  const previewCrf = numberValue(
    firstEnv(input.previewCrf, env.REAL_FOOTAGE_PREVIEW_CRF, process.env.REAL_FOOTAGE_PREVIEW_CRF),
    30,
  );
  const previewFps = numberValue(
    firstEnv(input.previewFps, env.REAL_FOOTAGE_PREVIEW_FPS, process.env.REAL_FOOTAGE_PREVIEW_FPS),
    null,
  );
  const previewPreset = String(
    firstEnv(input.previewPreset, env.REAL_FOOTAGE_PREVIEW_PRESET, process.env.REAL_FOOTAGE_PREVIEW_PRESET, "veryfast"),
  ).trim() || "veryfast";
  const previewAudioBitrate = String(
    firstEnv(
      input.previewAudioBitrate,
      env.REAL_FOOTAGE_PREVIEW_AUDIO_BITRATE,
      process.env.REAL_FOOTAGE_PREVIEW_AUDIO_BITRATE,
      "96k",
    ),
  ).trim() || "96k";
  const publishWebm = parseBooleanLike(
    firstEnv(input.publishWebm, env.FINAL_REEL_PUBLISH_WEBM, process.env.FINAL_REEL_PUBLISH_WEBM),
    false,
  );
  const finalReelRemoteUploadEnabled = parseBooleanLike(
    firstEnv(
      input.finalReelRemoteUploadEnabled,
      env.FINAL_REEL_REMOTE_UPLOAD_ENABLED,
      process.env.FINAL_REEL_REMOTE_UPLOAD_ENABLED,
    ),
    false,
  );
  const finalReelRemoteUploadRequired = parseBooleanLike(
    firstEnv(
      input.finalReelRemoteUploadRequired,
      env.FINAL_REEL_REMOTE_UPLOAD_REQUIRED,
      process.env.FINAL_REEL_REMOTE_UPLOAD_REQUIRED,
    ),
    false,
  );
  const finalReelUploadEndpoint = String(
    firstEnv(
      input.finalReelUploadEndpoint,
      env.FINAL_REEL_UPLOAD_ENDPOINT,
      env.S3_UPLOAD_URL,
      process.env.FINAL_REEL_UPLOAD_ENDPOINT,
      process.env.S3_UPLOAD_URL,
      "https://www.cbs.s1.carbarn.com.au/carbarnau/s3/uploadfiles",
    ),
  ).trim();
  const finalReelUploadDirectory = String(
    firstEnv(
      input.finalReelUploadDirectory,
      env.FINAL_REEL_UPLOAD_DIRECTORY,
      env.S3_UPLOAD_DIRECTORY,
      process.env.FINAL_REEL_UPLOAD_DIRECTORY,
      process.env.S3_UPLOAD_DIRECTORY,
      "social-media-content/reels",
    ),
  ).trim();
  const finalReelCdnBase = String(
    firstEnv(
      input.finalReelCdnBase,
      env.FINAL_REEL_CDN_BASE,
      env.S3_CDN_BASE_URL,
      process.env.FINAL_REEL_CDN_BASE,
      process.env.S3_CDN_BASE_URL,
      "https://www.storage.importautos.com.au/social-media-content/reels",
    ),
  ).trim();
  const finalReelUploadTimeoutMs = numberValue(
    firstEnv(
      input.finalReelUploadTimeoutMs,
      env.FINAL_REEL_UPLOAD_TIMEOUT_MS,
      process.env.FINAL_REEL_UPLOAD_TIMEOUT_MS,
    ),
    180000,
  );
  const finalReelRemoteProvider = String(
    firstEnv(
      input.finalReelRemoteProvider,
      env.FINAL_REEL_REMOTE_PROVIDER,
      process.env.FINAL_REEL_REMOTE_PROVIDER,
      "",
    ),
  ).trim().toLowerCase();
  const finalReelS3Bucket = String(
    firstEnv(
      input.finalReelS3Bucket,
      env.FINAL_REEL_S3_BUCKET,
      env.S3_BUCKET,
      env["cloud.aws.s3.bucket"],
      process.env.FINAL_REEL_S3_BUCKET,
      process.env.S3_BUCKET,
      process.env["cloud.aws.s3.bucket"],
    ),
  ).trim();
  const finalReelS3Region = String(
    firstEnv(
      input.finalReelS3Region,
      env.FINAL_REEL_S3_REGION,
      env.AWS_REGION,
      env.AWS_DEFAULT_REGION,
      env["cloud.aws.region.static"],
      process.env.FINAL_REEL_S3_REGION,
      process.env.AWS_REGION,
      process.env.AWS_DEFAULT_REGION,
      process.env["cloud.aws.region.static"],
      "ap-southeast-2",
    ),
  ).trim();
  const finalReelS3AccessKeyId = String(
    firstEnv(
      input.finalReelS3AccessKeyId,
      env.FINAL_REEL_S3_ACCESS_KEY_ID,
      env.AWS_ACCESS_KEY_ID,
      env["cloud.aws.credentials.accessKey"],
      process.env.FINAL_REEL_S3_ACCESS_KEY_ID,
      process.env.AWS_ACCESS_KEY_ID,
      process.env["cloud.aws.credentials.accessKey"],
    ),
  ).trim();
  const finalReelS3SecretAccessKey = String(
    firstEnv(
      input.finalReelS3SecretAccessKey,
      env.FINAL_REEL_S3_SECRET_ACCESS_KEY,
      env.AWS_SECRET_ACCESS_KEY,
      env["cloud.aws.credentials.secretKey"],
      process.env.FINAL_REEL_S3_SECRET_ACCESS_KEY,
      process.env.AWS_SECRET_ACCESS_KEY,
      process.env["cloud.aws.credentials.secretKey"],
    ),
  ).trim();
  const finalReelS3SessionToken = String(
    firstEnv(
      input.finalReelS3SessionToken,
      env.FINAL_REEL_S3_SESSION_TOKEN,
      env.AWS_SESSION_TOKEN,
      process.env.FINAL_REEL_S3_SESSION_TOKEN,
      process.env.AWS_SESSION_TOKEN,
    ),
  ).trim();
  const finalReelS3Prefix = String(
    firstEnv(
      input.finalReelS3Prefix,
      env.FINAL_REEL_S3_PREFIX,
      process.env.FINAL_REEL_S3_PREFIX,
      finalReelUploadDirectory,
    ),
  ).trim();
  const finalReelS3PublicBaseUrl = String(
    firstEnv(
      input.finalReelS3PublicBaseUrl,
      env.FINAL_REEL_S3_PUBLIC_BASE_URL,
      process.env.FINAL_REEL_S3_PUBLIC_BASE_URL,
      "",
    ),
  ).trim();
  const finalReelS3Acl = String(
    firstEnv(
      input.finalReelS3Acl,
      env.FINAL_REEL_S3_ACL,
      process.env.FINAL_REEL_S3_ACL,
      "",
    ),
  ).trim();
  const normalizedRemoteProvider = (
    finalReelRemoteProvider === "s3" || finalReelRemoteProvider === "multipart"
  )
    ? finalReelRemoteProvider
    : "";
  // Safety default: when an upload endpoint is available, use multipart API mode.
  // This avoids accidental direct-S3 uploads when stale env vars still contain bucket settings.
  const finalReelRemoteProviderFinal = finalReelUploadEndpoint
    ? "multipart"
    : (normalizedRemoteProvider || "multipart");

  if (strictEndScene) {
    composeWidth = Math.max(1080, Number(composeWidth) || 1080);
    composeHeight = Math.max(1920, Number(composeHeight) || 1920);
    composeFps = Math.max(30, Number(composeFps) || 30);
    endSceneSupersample = Math.max(1, Math.min(2, Number(endSceneSupersample) || 1));
    if (!rawWebmCodec && isPythonAnywhereRuntime(env)) {
      webmCodecFinal = "libvpx";
    } else {
      webmCodecFinal = webmCodecFinal || (fastRender ? "libvpx" : "libvpx-vp9");
    }
    webmDeadlineFinal = webmDeadlineFinal || "good";
    webmCrfFinal = Math.min(webmCrfFinal, 22);
    if (Number.isFinite(webmCpuUsedFinal)) {
      webmCpuUsedFinal = Math.min(webmCpuUsedFinal, 4);
    } else {
      webmCpuUsedFinal = 3;
    }
    if (!Number.isFinite(webmThreadsFinal) || webmThreadsFinal <= 0) {
      webmThreadsFinal = 2;
    }
    if (!Number.isFinite(mp4ThreadsFinal) || mp4ThreadsFinal <= 0) {
      mp4ThreadsFinal = 2;
    }
    mp4VideoCodecFinal = mp4VideoCodecFinal || DEFAULT_MP4_VIDEO_CODEC;
    mp4PresetFinal = mp4PresetFinal || DEFAULT_MP4_PRESET;
    mp4CrfFinal = Math.min(mp4CrfFinal, DEFAULT_MP4_CRF);
    if (!rawEndSceneSupersample) {
      endSceneSupersample = 1;
    }
  }

  return {
    command: input.command ?? "help",
    urls: arrayValue(input.urls ?? []),
    listingTitle: String(input.listingTitle ?? "").trim(),
    stockId: String(input.stockId ?? "").trim(),
    carDescription: String(input.carDescription ?? "").trim(),
    listingPrice: String(
      input.listingPrice ?? firstEnv(env.LISTING_PRICE, process.env.LISTING_PRICE) ?? "",
    ).trim(),
    priceIncludes: normalizePriceIncludesField(
      input.priceIncludes ?? env.PRICE_INCLUDES ?? process.env.PRICE_INCLUDES ?? "",
    ),
    elevenLabsApiKey: normalizePlainSecret(
      firstEnv(
        input.elevenLabsApiKey,
        env.ELEVEN_LABS_API_KEY,
        env.ELEVENLABS_API_KEY,
        process.env.ELEVEN_LABS_API_KEY,
        process.env.ELEVENLABS_API_KEY,
      ),
    ),
    elevenLabsVoiceId: String(
      firstEnv(
        input.elevenLabsVoiceId,
        env.ELEVENLAB_VOICE_ID,
        env.ELEVENLABS_VOICE_ID,
        process.env.ELEVENLAB_VOICE_ID,
        process.env.ELEVENLABS_VOICE_ID,
      ),
    ).trim(),
    outDir,
    shotsPerClip: input.shotsPerClip ?? 3,
    maxClips: input.maxClips ?? null,
    compose: Boolean(input.compose),
    headless: input.headless ?? true,
    browserPath: input.browserPath || env.BROWSER_PATH || null,
    pythonPath: input.pythonPath || env.PYTHON_PATH || DEFAULT_PYTHON_PATH,
    ffmpegPath: input.ffmpegPath || env.FFMPEG_PATH || DEFAULT_FFMPEG_PATH,
    geminiApiKey: resolvedGeminiApiKey,
    geminiModel: input.geminiModel || env.GEMINI_MODEL || "gemini-2.5-pro",
    targetSequence: getLockedTargetSequence(),
    composeWidth,
    composeHeight,
    composeFps,
    fastRender,
    strictEndScene,
    webmCodec: webmCodecFinal,
    webmDeadline: webmDeadlineFinal,
    webmCpuUsed: webmCpuUsedFinal,
    webmCrf: webmCrfFinal,
    webmThreads: webmThreadsFinal,
    mp4VideoCodec: mp4VideoCodecFinal,
    mp4Preset: mp4PresetFinal,
    mp4Crf: mp4CrfFinal,
    mp4Threads: mp4ThreadsFinal,
    previewEnabled,
    previewMaxWidth,
    previewMaxHeight,
    previewCrf,
    previewFps,
    previewPreset,
    previewAudioBitrate,
    publishWebm,
    finalReelRemoteUploadEnabled,
    finalReelRemoteUploadRequired,
    finalReelRemoteProvider: finalReelRemoteProviderFinal,
    finalReelUploadEndpoint,
    finalReelUploadDirectory,
    finalReelCdnBase,
    finalReelUploadTimeoutMs,
    finalReelS3Bucket,
    finalReelS3Region,
    finalReelS3AccessKeyId,
    finalReelS3SecretAccessKey,
    finalReelS3SessionToken,
    finalReelS3Prefix,
    finalReelS3PublicBaseUrl,
    finalReelS3Acl,
    endSceneSupersample,
    /** Skip this many seconds at the start of each source clip before sampling/composing. */
    clipStartSkipSeconds: clampClipStartSkipSeconds(
      input.clipStartSkipSeconds ?? DEFAULT_CLIP_START_SKIP_SECONDS,
    ),
    /** Main montage (clips only), seconds — voice-over matches this length. */
    composeMainDurationSeconds: reelDurations.composeMainDurationSeconds,
    /** Branded Carbarn end card appended after the main montage (typically 3–5s). */
    endSceneDurationSeconds: reelDurations.endSceneDurationSeconds,
    /** Hard cap for main + end (default 17s). */
    maxTotalReelDurationSeconds: reelDurations.maxTotalReelDurationSeconds,
    /** @deprecated Same as composeMainDurationSeconds (main reel only, not including end scene). */
    composeDurationSeconds: reelDurations.composeMainDurationSeconds,
    /** main + end scene */
    totalComposedDurationSeconds: reelDurations.totalComposedDurationSeconds,
    requestedComposeMainDurationSeconds: reelDurations.requestedMainDurationSeconds,
    mainDurationShortenedForEndScene: reelDurations.mainDurationShortened,
    /**
     * Voice-over script approval is required before ElevenLabs TTS can run.
     */
    voiceoverScriptApproval: input.voiceoverScriptApproval ?? true,
    browserProfileDir: input.browserProfileDir
      ? path.resolve(process.cwd(), String(input.browserProfileDir))
      : path.join(process.cwd(), ".browser-profile"),
  };
}

export function loadEnvConfig(cwd = process.cwd()) {
  const env = loadDotEnvSync(cwd);
  const geminiApiKey = normalizeGeminiApiKey(env.GEMINI_API_KEY || process.env.GEMINI_API_KEY || "");
  return {
    ...env,
    GEMINI_API_KEY: geminiApiKey,
    GEMINI_MODEL: env.GEMINI_MODEL || "gemini-2.5-pro",
    BROWSER_PATH: env.BROWSER_PATH || "",
    PYTHON_PATH: env.PYTHON_PATH || DEFAULT_PYTHON_PATH,
    FFMPEG_PATH: env.FFMPEG_PATH || DEFAULT_FFMPEG_PATH,
  };
}

export function resolveReelDurations(input = {}) {
  const requestedMainDurationSeconds = clampComposeMainSeconds(
    input.requestedComposeMainDurationSeconds ??
    input.composeMainDurationSeconds ??
      input.composeDurationSeconds ??
      DEFAULT_COMPOSE_MAIN_DURATION_SECONDS,
  );
  const endSceneDurationSeconds = clampEndSceneSeconds(
    input.endSceneDurationSeconds ?? DEFAULT_END_SCENE_DURATION_SECONDS,
  );
  const maxTotalReelDurationSeconds = normalizeMaxTotalReelSeconds(
    input.maxTotalReelDurationSeconds ?? DEFAULT_MAX_TOTAL_REEL_DURATION_SECONDS,
  );
  const maxAvailableMainSeconds = roundDurationTenths(maxTotalReelDurationSeconds - endSceneDurationSeconds);

  if (maxAvailableMainSeconds <= 0) {
    throw new Error(
      `Video timing: end scene (${endSceneDurationSeconds}s) leaves no time for the main montage within the ${maxTotalReelDurationSeconds}s cap.`,
    );
  }

  const composeMainDurationSeconds = roundDurationTenths(
    Math.min(requestedMainDurationSeconds, maxAvailableMainSeconds),
  );

  return {
    requestedMainDurationSeconds,
    composeMainDurationSeconds,
    endSceneDurationSeconds,
    maxTotalReelDurationSeconds,
    totalComposedDurationSeconds: roundDurationTenths(composeMainDurationSeconds + endSceneDurationSeconds),
    mainDurationShortened: composeMainDurationSeconds + 0.001 < requestedMainDurationSeconds,
  };
}

export function hasGeminiApiKey(value) {
  return Boolean(normalizeGeminiApiKey(value));
}

export function hasVoiceoverEnv(env = loadEnvConfig(process.cwd())) {
  const key = normalizePlainSecret(
    firstEnv(
      env.ELEVEN_LABS_API_KEY,
      env.ELEVENLABS_API_KEY,
      process.env.ELEVEN_LABS_API_KEY,
      process.env.ELEVENLABS_API_KEY,
    ),
  );
  const voice = String(
    firstEnv(
      env.ELEVENLAB_VOICE_ID,
      env.ELEVENLABS_VOICE_ID,
      process.env.ELEVENLAB_VOICE_ID,
      process.env.ELEVENLABS_VOICE_ID,
    ),
  ).trim();
  return Boolean(key && voice.length > 4);
}

function normalizePlainSecret(value) {
  const normalized = String(value ?? "").trim();
  return normalized;
}

function resolveFastRenderMode(input, env) {
  if (typeof input.fastRender === "boolean") {
    return input.fastRender;
  }
  const onPythonAnywhere = isPythonAnywhereRuntime(env);
  const raw = firstEnv(input.fastRender, env.REAL_FOOTAGE_FAST_RENDER, process.env.REAL_FOOTAGE_FAST_RENDER);
  if (!raw) {
    return onPythonAnywhere;
  }
  return parseBooleanLike(raw, onPythonAnywhere);
}

function isPythonAnywhereRuntime(env = {}) {
  return Boolean(
    firstEnv(
      env.PYTHONANYWHERE_SITE,
      env.PYTHONANYWHERE_DOMAIN,
      env.PYTHONANYWHERE_HOME,
      process.env.PYTHONANYWHERE_SITE,
      process.env.PYTHONANYWHERE_DOMAIN,
      process.env.PYTHONANYWHERE_HOME,
    ),
  );
}

function parseBooleanLike(value, fallback = false) {
  const normalized = String(value ?? "").trim().toLowerCase();
  if (!normalized) return fallback;
  if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
  return fallback;
}

function firstEnv(...values) {
  for (const value of values) {
    const s = normalizePlainSecret(value);
    if (s) {
      return s;
    }
  }
  return "";
}

function normalizePriceIncludesField(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  if (Array.isArray(value)) {
    const list = value.map((v) => String(v).trim()).filter(Boolean);
    return list.length ? list : null;
  }
  return String(value)
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter(Boolean);
}

function clampComposeMainSeconds(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) {
    return DEFAULT_COMPOSE_MAIN_DURATION_SECONDS;
  }
  return Math.min(14, Math.max(8, Math.round(n * 10) / 10));
}

function clampEndSceneSeconds(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) {
    return DEFAULT_END_SCENE_DURATION_SECONDS;
  }
  return Math.min(5, Math.max(3, Math.round(n * 10) / 10));
}

function normalizeMaxTotalReelSeconds(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) {
    return DEFAULT_MAX_TOTAL_REEL_DURATION_SECONDS;
  }
  return Math.max(4, roundDurationTenths(n));
}

function roundDurationTenths(value) {
  return Math.round(Number(value) * 10) / 10;
}

function clampClipStartSkipSeconds(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) {
    return DEFAULT_CLIP_START_SKIP_SECONDS;
  }
  return Math.min(MAX_CLIP_START_SKIP_SECONDS, Math.max(0, Math.round(n * 10) / 10));
}

function loadDotEnvSync(cwd) {
  const envPath = path.join(cwd, ".env");
  try {
    let content = readFileSync(envPath, "utf8");
    if (content.charCodeAt(0) === 0xfeff) {
      content = content.slice(1);
    }
    const pairs = {};
    for (const line of content.split(/\r?\n/u)) {
      let trimmed = line.trim();
      if (trimmed.toLowerCase().startsWith("export ")) {
        trimmed = trimmed.slice(7).trim();
      }
      if (!trimmed || trimmed.startsWith("#") || !trimmed.includes("=")) {
        continue;
      }
      const separator = trimmed.indexOf("=");
      const key = trimmed.slice(0, separator).trim();
      const value = trimmed.slice(separator + 1).trim().replace(/^['"]|['"]$/gu, "");
      pairs[key] = value;
    }
    return pairs;
  } catch {
    return {};
  }
}

function parseArgs(argv) {
  const result = { _: [] };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith("--")) {
      result._.push(token);
      continue;
    }

    const key = token.slice(2);
    const next = argv[index + 1];
    if (!next || next.startsWith("--")) {
      result[key] = true;
      continue;
    }

    if (result[key] === undefined) {
      result[key] = next;
    } else if (Array.isArray(result[key])) {
      result[key].push(next);
    } else {
      result[key] = [result[key], next];
    }
    index += 1;
  }

  return result;
}

function arrayValue(value) {
  if (value === undefined) {
    return [];
  }

  return Array.isArray(value) ? value : [value];
}

function numberValue(value, fallback = null) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }

  const numeric = Number(value);
  if (Number.isNaN(numeric)) {
    throw new Error(`Expected a number but received "${value}".`);
  }

  return numeric;
}

function normalizeGeminiApiKey(value) {
  const normalized = String(value ?? "").trim();
  if (!normalized) {
    return "";
  }

  return GEMINI_KEY_PLACEHOLDERS.has(normalized.toLowerCase()) ? "" : normalized;
}

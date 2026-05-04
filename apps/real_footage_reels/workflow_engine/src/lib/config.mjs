import { readFileSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { getLockedTargetSequence } from "./reel-rules.mjs";

const DEFAULT_PYTHON_PATH = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python312\\python.exe";
const DEFAULT_FFMPEG_PATH =
  "C:\\Users\\user\\AppData\\Local\\Microsoft\\WinGet\\Packages\\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe\\ffmpeg-8.1-full_build\\bin\\ffmpeg.exe";
const DEFAULT_COMPOSE_WIDTH = 1080;
const DEFAULT_COMPOSE_HEIGHT = 1920;
const DEFAULT_COMPOSE_MAIN_DURATION_SECONDS = 14;
const DEFAULT_END_SCENE_DURATION_SECONDS = 3.5;
const DEFAULT_MAX_TOTAL_REEL_DURATION_SECONDS = 17;
const DEFAULT_CLIP_START_SKIP_SECONDS = 2;
const MAX_CLIP_START_SKIP_SECONDS = 2;
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

  const reelDurations = resolveReelDurations(input);

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
    composeWidth: input.composeWidth ?? DEFAULT_COMPOSE_WIDTH,
    composeHeight: input.composeHeight ?? DEFAULT_COMPOSE_HEIGHT,
    composeFps: input.composeFps ?? 30,
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

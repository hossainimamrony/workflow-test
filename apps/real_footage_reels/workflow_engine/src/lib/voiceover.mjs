import fs from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";

import { resolveReelDurations } from "./config.mjs";
import { publishFinalReelMp4 } from "./final-reel-output.mjs";
import { generateVoiceoverScriptVariants } from "./gemini.mjs";
import { writeJson } from "./fs-utils.mjs";

const VOICEOVER_STATUS_FILE = "voiceover-status.json";
const ELEVENLABS_RETRY_DELAYS_MS = [500, 1500, 3500];

/**
 * Generate several script options for Studio approval before TTS/render handoff.
 */
export async function draftVoiceoverScripts(runDir, config, log = () => {}, options = {}) {
  const strict = Boolean(options.strict);
  const ctx = await resolveVoiceoverContext(runDir, config, log, { requireVideo: false });
  if (!ctx) {
    if (strict) {
      throw new Error(
        "Script draft failed: car description is missing. Add a description in Studio and run script generation again.",
      );
    }
    return null;
  }
  const { normalizedDir, manifest, carDescription, mainSeconds, endSeconds, totalSeconds } = ctx;
  const listingPrice = String(manifest?.listingPrice ?? config.listingPrice ?? "").trim();

  if (!config.geminiApiKey) {
    log("Voice-over drafts skipped: GEMINI_API_KEY required.");
    if (strict) {
      throw new Error(
        "Script draft failed: GEMINI_API_KEY is missing or placeholder. Add a real Gemini key in environment/.env and restart the web app.",
      );
    }
    return null;
  }

  log(
    `Generating ${3} voice-over script options with Gemini (~${totalSeconds.toFixed(1)}s target: ${mainSeconds}s montage + ${endSeconds}s end card)...`,
  );
  const { variants } = await generateVoiceoverScriptVariants(
    config,
    {
      listingTitle: String(manifest.listingTitle ?? "").trim(),
      stockId: String(manifest.stockId ?? "").trim(),
      carDescription,
      listingPrice,
      targetSeconds: totalSeconds,
      mainMontageSeconds: mainSeconds,
      endSceneSeconds: endSeconds,
    },
    3,
  );

  const draftPath = path.join(normalizedDir, "voiceover-script-draft.json");
  await writeJson(draftPath, {
    createdAt: new Date().toISOString(),
    status: "pending",
    mainMontageDurationSeconds: mainSeconds,
    endSceneDurationSeconds: endSeconds,
    targetDurationSeconds: totalSeconds,
    variants,
  });

  log(`Saved script drafts to voiceover-script-draft.json (${variants.length} options).`);
  return { variants, draftPath };
}

/**
 * After silent final reel is rendered, use an approved script for TTS (ElevenLabs),
 * normalize audio to full video length (main montage + end card), then mux audio.
 * @param {{ approvedScript?: string, failOnTtsError?: boolean }} [options]
 */
export async function applyVoiceoverToReel(runDir, config, log = () => {}, options = {}) {
  const approvedScript = typeof options.approvedScript === "string" ? options.approvedScript.trim() : "";
  const failOnTtsError = options.failOnTtsError !== false;
  const ctx = await resolveVoiceoverContext(runDir, config, log);
  if (!ctx) {
    return null;
  }
  const { normalizedDir, mainSeconds, endSeconds, totalSeconds } = ctx;

  if (!config.elevenLabsApiKey || !config.elevenLabsVoiceId) {
    const missing = [
      !config.elevenLabsApiKey ? "ELEVEN_LABS_API_KEY (or ELEVENLABS_API_KEY)" : null,
      !config.elevenLabsVoiceId ? "ELEVENLAB_VOICE_ID (or ELEVENLABS_VOICE_ID)" : null,
    ]
      .filter(Boolean)
      .join(" and ");
    log(
      `Voice-over skipped: missing ${missing}. Add to the project .env file (save the file) and/or set in the system environment, then restart the server.`,
    );
    return null;
  }

  if (!approvedScript) {
    log("Voice-over TTS skipped: approve a script from Runs before stitching audio.");
    return null;
  }

  const script = approvedScript.replace(/\s+/gu, " ").trim();
  const spokenScript = normalizeScriptForSpeech(script);
  log("Using approved/edited voice-over script (skipping Gemini).");
  if (spokenScript !== script) {
    log("Normalized script for speech-friendly ElevenLabs input.");
  }

  log("Synthesizing speech with ElevenLabs...");
  const rawAudioPath = path.join(normalizedDir, "voiceover-raw.mp3");
  let rawDuration;
  try {
    ({ rawDuration } = await synthesizeElevenLabs(
      config,
      spokenScript,
      rawAudioPath,
      log,
    ));
  } catch (error) {
    await writeVoiceoverFailureStatus(normalizedDir, error);
    if (!failOnTtsError) {
      log(`Voice-over failed after ElevenLabs retries. Keeping the silent reel: ${error?.message ?? error}`);
      return null;
    }
    throw error;
  }

  log(
    `ElevenLabs raw audio: ${rawDuration.toFixed(2)}s.`,
  );
  const voicePath = path.join(normalizedDir, "voiceover.mp3");
  await prepareVoiceoverAudioForTarget({
    ffmpegPath: config.ffmpegPath,
    rawAudioPath,
    outAudioPath: voicePath,
    rawDuration,
    targetSeconds: totalSeconds,
    log,
  });

  log("Muxing final reel with voice-over audio...");

  const mutePath = path.join(normalizedDir, "final-reel-mute.webm");
  const cleanVideoPath = await resolveCleanVideoSource({
    ffmpegPath: config.ffmpegPath,
    runDir: normalizedDir,
    mainSeconds,
    totalSeconds,
    log,
  });
  await fs.copyFile(cleanVideoPath, mutePath);

  const outPath = path.join(normalizedDir, "final-reel.webm");
  await muxVideoWithAudio({
    ffmpegPath: config.ffmpegPath,
    videoPath: mutePath,
    audioPath: voicePath,
    outPath,
  });

  await writeJson(path.join(normalizedDir, "voiceover-manifest.json"), {
    createdAt: new Date().toISOString(),
    script,
    spokenScript,
    mainMontageDurationSeconds: mainSeconds,
    endSceneDurationSeconds: endSeconds,
    targetDurationSeconds: totalSeconds,
    totalVideoDurationSeconds: totalSeconds,
    audioDurationSeconds: (await probeMediaDuration(config.ffmpegPath, voicePath)) || totalSeconds,
    voiceId: config.elevenLabsVoiceId,
  });

  await clearVoiceoverFailureStatus(normalizedDir);
  await mergeDraftStatusApplied(normalizedDir, script);
  await publishFinalReelMp4(normalizedDir, config.ffmpegPath, log);

  log(`Voice-over complete: ${outPath}`);
  return { script, outputPath: outPath };
}

export async function resetVoiceoverStateForSilentRebuild(runDir) {
  const normalizedDir = path.resolve(runDir);
  await Promise.all([
    removeIfExists(path.join(normalizedDir, "voiceover-manifest.json")),
    clearVoiceoverFailureStatus(normalizedDir),
  ]);
}

export async function reapplySavedVoiceoverToReel(runDir, config, log = () => {}) {
  const normalizedDir = path.resolve(runDir);
  const voiceoverManifest = await readJsonIfExists(path.join(normalizedDir, "voiceover-manifest.json"));
  if (!voiceoverManifest) {
    return false;
  }

  const videoPath = path.join(normalizedDir, "final-reel.webm");
  const voicePath = path.join(normalizedDir, "voiceover.mp3");
  try {
    await Promise.all([fs.access(videoPath), fs.access(voicePath)]);
  } catch {
    log("Saved voice-over manifest exists, but voice-over audio is missing. Keeping silent refreshed reel.");
    return false;
  }

  const totalSeconds =
    Number(voiceoverManifest.totalVideoDurationSeconds) ||
    resolveReelDurations(config).totalComposedDurationSeconds;
  const mainSeconds =
    Number(voiceoverManifest.mainMontageDurationSeconds) ||
    resolveReelDurations(config).composeMainDurationSeconds;

  const mutePath = path.join(normalizedDir, "final-reel-mute.webm");
  const cleanVideoPath = await resolveCleanVideoSource({
    ffmpegPath: config.ffmpegPath,
    runDir: normalizedDir,
    mainSeconds,
    totalSeconds,
    log,
  });
  await fs.copyFile(cleanVideoPath, mutePath);

  await muxVideoWithAudio({
    ffmpegPath: config.ffmpegPath,
    videoPath: mutePath,
    audioPath: voicePath,
    outPath: videoPath,
  });

  await clearVoiceoverFailureStatus(normalizedDir);
  await publishFinalReelMp4(normalizedDir, config.ffmpegPath, log);
  log("Reapplied saved voice-over onto the refreshed reel.");
  return true;
}

async function resolveCleanVideoSource({ ffmpegPath, runDir, mainSeconds, totalSeconds, log }) {
  const cleanPath = path.join(runDir, "final-reel-clean.webm");
  const mainPath = path.join(runDir, "main-reel.webm");
  const endPath = path.join(runDir, "end-scene.webm");
  if (!(await fileExists(mainPath)) || !(await fileExists(endPath))) {
    throw new Error(
      "Cannot rebuild voice-over base: required source segments are missing (main-reel.webm and/or end-scene.webm). Re-run render before rebuilding voice-over.",
    );
  }
  await concatMainAndEndForVoiceover({
    ffmpegPath,
    runDir,
    mainPath,
    endPath,
    mainDurationSeconds: mainSeconds,
    outPath: cleanPath,
    durationSeconds: totalSeconds,
  });
  log("Rebuilt clean base reel from main + end scene for voice-over rebuild.");
  return cleanPath;
}

async function mergeDraftStatusApplied(runDir, appliedScript) {
  const draftPath = path.join(runDir, "voiceover-script-draft.json");
  const existing = await readJsonIfExists(draftPath);
  if (!existing) {
    return;
  }
  await writeJson(draftPath, {
    ...existing,
    status: "applied",
    appliedAt: new Date().toISOString(),
    appliedScript,
  });
}

async function resolveVoiceoverContext(runDir, config, log, options = {}) {
  const normalizedDir = path.resolve(runDir);
  const rootManifestPath = path.join(normalizedDir, "downloads-manifest.json");
  const nestedManifestPath = path.join(normalizedDir, "downloads", "downloads-manifest.json");
  const endSceneManifest = await readJsonIfExists(path.join(normalizedDir, "end-scene-manifest.json"));
  let manifest = await readJsonIfExists(rootManifestPath);
  let manifestWritePath = rootManifestPath;
  if (!manifest) {
    manifest = await readJsonIfExists(nestedManifestPath);
    manifestWritePath = nestedManifestPath;
  }

  const fromManifest = String(manifest?.carDescription ?? "").trim();
  const fromConfig = String(config.carDescription ?? "").trim();
  let carDescription = fromManifest || fromConfig;

  if (manifest && fromConfig && !fromManifest) {
    await writeJson(manifestWritePath, { ...manifest, carDescription: fromConfig });
    manifest = { ...manifest, carDescription: fromConfig };
    carDescription = fromConfig;
    log("Saved car description into downloads manifest (was missing) for voice-over.");
  }

  if (!carDescription) {
    log(
      "Voice-over skipped: no car description. Add it in Studio (Car description) and run again, or add \"carDescription\" to downloads-manifest.json for this run.",
    );
    return null;
  }

  const configuredDurations = resolveReelDurations(config);
  const mainSeconds =
    Number(endSceneManifest?.mainDurationSeconds) || configuredDurations.composeMainDurationSeconds;
  const endSeconds =
    Number(endSceneManifest?.endDurationSeconds) || configuredDurations.endSceneDurationSeconds;
  const totalSeconds =
    Number(endSceneManifest?.totalDurationSeconds) || mainSeconds + endSeconds;
  const requireVideo = options.requireVideo !== false;

  const videoPath = path.join(normalizedDir, "final-reel.webm");
  if (requireVideo) {
    await fs.access(videoPath).catch(() => {
      throw new Error("final-reel.webm not found; compose the reel first.");
    });

    const videoDurProbe = await probeMediaDuration(config.ffmpegPath, videoPath);
    const expectedVideoDur = mainSeconds + endSeconds;
    if (videoDurProbe > 0 && videoDurProbe < expectedVideoDur - 0.35) {
      throw new Error(
        `Combined video is ${videoDurProbe.toFixed(1)}s but should be ~${expectedVideoDur}s (${mainSeconds}s footage + ${endSeconds}s end template). Re-run Render so main + end card concatenate. Current file may be montage-only.`,
      );
    }
  }

  return {
    normalizedDir,
    manifest,
    carDescription,
    mainSeconds,
    endSeconds,
    totalSeconds,
    videoPath: requireVideo ? videoPath : null,
  };
}

export function normalizeScriptForSpeech(script) {
  let text = String(script ?? "").trim().replace(/\s+/gu, " ");
  if (!text) {
    return "";
  }

  text = text.replace(/\s*&\s*/gu, " and ");
  text = text.replace(/\bcarbarn\.com\.au\b/giu, "Carbarn dot com dot A U");

  text = text.replace(
    /(?:AU\$|\$)\s*(\d[\d,]*)(?:\.(\d{1,2}))?/gu,
    (_, wholePart, decimalPart) => currencyToSpeech(wholePart, decimalPart),
  );

  text = text.replace(
    /\b(\d+(?:\.\d+)?)\s*k\s*(?:km|kms|kilometres?|kilometers?)\b/giu,
    (_, thousands) => `${integerToWords(Math.round(Number(thousands) * 1000))} kilometres`,
  );

  text = text.replace(
    /\b(\d[\d,]*)(?:\.(\d+))?\s*(?:km|kms|kilometres?|kilometers?)\b/giu,
    (_, wholePart, decimalPart) =>
      `${numberStringToSpeech(wholePart, decimalPart)} kilometres`,
  );

  text = text.replace(
    /\b(\d+(?:\.\d+)?)\s*(?:l|litre|liter)\b/giu,
    (_, rawValue) => `${decimalNumberToWords(rawValue)} litre`,
  );

  // Reduce long or stacked punctuation so TTS keeps a tighter cadence.
  text = text
    .replace(/\.{2,}/gu, ".")
    .replace(/[,:;()\[\]{}]/gu, " ")
    .replace(/[!?]+/gu, ".")
    .replace(/\s*-\s*/gu, " ")
    .replace(/\s*\/\s*/gu, " ");

  text = text
    .replace(/\b4wd\b/giu, "four wheel drive")
    .replace(/\b2wd\b/giu, "two wheel drive")
    .replace(/\bawd\b/giu, "all wheel drive")
    .replace(/\bfwd\b/giu, "front wheel drive")
    .replace(/\brwd\b/giu, "rear wheel drive")
    .replace(/\bcvt\b/giu, "C V T")
    .replace(/\babs\b/giu, "A B S");

  text = text.replace(/\b(19\d{2}|20\d{2})\b/gu, (match) => yearToWords(Number(match)));
  text = text.replace(/\s+([,.;:!?])/gu, "$1");
  text = text.replace(/\s*\.\s*/gu, ". ");
  text = text.replace(/\s+/gu, " ").trim();

  return text;
}

function currencyToSpeech(wholePart, decimalPart) {
  const whole = Number(String(wholePart).replace(/,/gu, ""));
  if (!Number.isFinite(whole)) {
    return `${wholePart}${decimalPart ? `.${decimalPart}` : ""} dollars`;
  }

  const dollarWords = `${integerToWords(whole)} dollar${whole === 1 ? "" : "s"}`;
  const cents = decimalPart ? Number(String(decimalPart).padEnd(2, "0").slice(0, 2)) : 0;
  if (!Number.isFinite(cents) || cents <= 0) {
    return dollarWords;
  }

  return `${dollarWords} and ${integerToWords(cents)} cent${cents === 1 ? "" : "s"}`;
}

function numberStringToSpeech(wholePart, decimalPart) {
  const whole = Number(String(wholePart).replace(/,/gu, ""));
  if (!Number.isFinite(whole)) {
    return `${wholePart}${decimalPart ? `.${decimalPart}` : ""}`;
  }

  if (decimalPart) {
    return decimalNumberToWords(`${whole}.${decimalPart}`);
  }

  return integerToWords(whole);
}

function decimalNumberToWords(value) {
  const [wholePart, decimalPart = ""] = String(value).split(".");
  const whole = Number(String(wholePart).replace(/,/gu, ""));
  if (!Number.isFinite(whole)) {
    return String(value);
  }
  if (!decimalPart) {
    return integerToWords(whole);
  }

  const decimalWords = decimalPart
    .split("")
    .map((digit) => integerToWords(Number(digit)))
    .join(" ");

  return `${integerToWords(whole)} point ${decimalWords}`;
}

function yearToWords(year) {
  if (!Number.isFinite(year)) {
    return String(year);
  }

  if (year >= 2000 && year <= 2009) {
    const suffix = year % 100;
    return suffix === 0 ? "two thousand" : `two thousand ${integerToWords(suffix)}`;
  }

  if (year >= 2010 && year <= 2099) {
    const suffix = year % 100;
    return suffix === 0 ? "twenty hundred" : `twenty ${integerToWords(suffix)}`;
  }

  if (year >= 1900 && year <= 1999) {
    const suffix = year % 100;
    return suffix === 0 ? "nineteen hundred" : `nineteen ${integerToWords(suffix)}`;
  }

  return integerToWords(year);
}

function integerToWords(value) {
  const number = Math.trunc(Number(value));
  if (!Number.isFinite(number)) {
    return String(value);
  }

  if (number === 0) {
    return "zero";
  }

  if (number < 0) {
    return `minus ${integerToWords(Math.abs(number))}`;
  }

  const ones = [
    "zero",
    "one",
    "two",
    "three",
    "four",
    "five",
    "six",
    "seven",
    "eight",
    "nine",
  ];
  const teens = [
    "ten",
    "eleven",
    "twelve",
    "thirteen",
    "fourteen",
    "fifteen",
    "sixteen",
    "seventeen",
    "eighteen",
    "nineteen",
  ];
  const tens = [
    "",
    "",
    "twenty",
    "thirty",
    "forty",
    "fifty",
    "sixty",
    "seventy",
    "eighty",
    "ninety",
  ];
  const scales = [
    { value: 1_000_000_000, label: "billion" },
    { value: 1_000_000, label: "million" },
    { value: 1_000, label: "thousand" },
  ];

  const underHundred = (n) => {
    if (n < 10) {
      return ones[n];
    }
    if (n < 20) {
      return teens[n - 10];
    }
    const tenValue = Math.floor(n / 10);
    const unitValue = n % 10;
    return unitValue ? `${tens[tenValue]}-${ones[unitValue]}` : tens[tenValue];
  };

  const underThousand = (n) => {
    if (n < 100) {
      return underHundred(n);
    }
    const hundredValue = Math.floor(n / 100);
    const remainder = n % 100;
    return remainder
      ? `${ones[hundredValue]} hundred and ${underHundred(remainder)}`
      : `${ones[hundredValue]} hundred`;
  };

  let remaining = number;
  const parts = [];

  for (const scale of scales) {
    if (remaining >= scale.value) {
      const scaleAmount = Math.floor(remaining / scale.value);
      parts.push(`${integerToWords(scaleAmount)} ${scale.label}`);
      remaining %= scale.value;
    }
  }

  if (remaining > 0) {
    if (parts.length && remaining < 100) {
      parts.push(`and ${underHundred(remaining)}`);
    } else {
      parts.push(underThousand(remaining));
    }
  }

  return parts.join(" ");
}

async function readJsonIfExists(filePath) {
  try {
    const content = await fs.readFile(filePath, "utf8");
    return JSON.parse(content);
  } catch {
    return null;
  }
}

async function synthesizeElevenLabs(config, text, outAudioPath, log = () => {}) {
  const maxAttempts = ELEVENLABS_RETRY_DELAYS_MS.length + 1;
  let lastError = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      return await synthesizeElevenLabsOnce(config, text, outAudioPath);
    } catch (error) {
      lastError = error;
      const delayMs = ELEVENLABS_RETRY_DELAYS_MS[attempt - 1];
      if (!isRetryableElevenLabsError(error) || delayMs === undefined) {
        error.elevenLabsAttempts = attempt;
        throw error;
      }
      log(
        `ElevenLabs attempt ${attempt}/${maxAttempts} failed (${error?.message ?? error}). Retrying in ${delayMs}ms...`,
      );
      await sleep(delayMs);
    }
  }

  throw lastError ?? new Error("ElevenLabs TTS failed.");
}

async function synthesizeElevenLabsOnce(config, text, outAudioPath) {
  const voiceId = config.elevenLabsVoiceId;
  const url = `https://api.elevenlabs.io/v1/text-to-speech/${encodeURIComponent(voiceId)}`;

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "xi-api-key": config.elevenLabsApiKey,
      "content-type": "application/json",
      accept: "audio/mpeg",
    },
    body: JSON.stringify({
      text,
      model_id: "eleven_multilingual_v2",
    }),
  });

  if (!response.ok) {
    const errText = await response.text();
    const error = new Error(`ElevenLabs TTS failed (${response.status}): ${errText}`);
    error.elevenLabsStatus = response.status;
    throw error;
  }

  const buffer = Buffer.from(await response.arrayBuffer());
  if (!buffer.length) {
    throw new Error("ElevenLabs response returned empty audio.");
  }
  const buf = buffer;
  await fs.writeFile(outAudioPath, buf);
  const rawDuration = await probeMediaDuration(config.ffmpegPath, outAudioPath);
  return { rawDuration };
}

function isRetryableElevenLabsError(error) {
  const status = Number(error?.elevenLabsStatus);
  if (status === 429 || status >= 500) {
    return true;
  }
  const message = String(error?.message ?? "").toLowerCase();
  return message.includes("fetch failed");
}

async function probeMediaDuration(ffmpegPath, filePath) {
  const ffprobePath = await resolveFfprobePath(ffmpegPath);
  if (!ffprobePath) {
    return probeDurationFallback(ffmpegPath, filePath);
  }
  const result = await runProcess(ffprobePath, [
    "-v",
    "error",
    "-show_entries",
    "format=duration",
    "-of",
    "default=noprint_wrappers=1:nokey=1",
    filePath,
  ]);
  const duration = Number(result.stdout.trim());
  return Number.isFinite(duration) && duration > 0 ? duration : probeDurationFallback(ffmpegPath, filePath);
}

async function probeDurationFallback(ffmpegPath, filePath) {
  const result = await runProcess(ffmpegPath, ["-i", filePath], { allowFailure: true });
  const durationMatch = /Duration:\s+(\d+):(\d+):(\d+(?:\.\d+)?)/u.exec(result.stderr);
  if (!durationMatch) {
    return 0;
  }
  const hours = Number(durationMatch[1]);
  const minutes = Number(durationMatch[2]);
  const seconds = Number(durationMatch[3]);
  return hours * 3600 + minutes * 60 + seconds;
}

async function prepareVoiceoverAudioForTarget({
  ffmpegPath,
  rawAudioPath,
  outAudioPath,
  rawDuration,
  targetSeconds,
  log,
}) {
  if (rawDuration <= 0 || !Number.isFinite(rawDuration)) {
    throw new Error("Could not measure audio duration.");
  }

  // Keep ElevenLabs audio exactly as generated unless it exceeds the reel cap.
  if (rawDuration <= targetSeconds + 0.02) {
    await fs.copyFile(rawAudioPath, outAudioPath);
    return;
  }

  const requiredTempo = rawDuration / targetSeconds;
  const appliedTempo = Number(requiredTempo.toFixed(4));
  log(
    `Voice-over exceeds target (${rawDuration.toFixed(2)}s > ${targetSeconds.toFixed(1)}s). Applying slight speed-up x${appliedTempo.toFixed(3)} to fit.`,
  );
  await runFfmpeg(ffmpegPath, [
    "-y",
    "-i",
    rawAudioPath,
    "-af",
    `atempo=${appliedTempo}`,
    outAudioPath,
  ]);
}

async function resolveFfprobePath(ffmpegPath) {
  const probePath = path.join(path.dirname(ffmpegPath), "ffprobe.exe");
  try {
    await fs.access(probePath);
    return probePath;
  } catch {
    return null;
  }
}

async function muxVideoWithAudio({ ffmpegPath, videoPath, audioPath, outPath }) {
  const runDir = path.dirname(path.resolve(videoPath));
  const rel = (abs) => path.relative(runDir, path.resolve(abs)).split(path.sep).join("/");
  const vIn = rel(videoPath);
  const aIn = rel(audioPath);
  const outRel = rel(outPath);

  const args = [
    "-y",
    "-i",
    vIn,
    "-i",
    aIn,
    "-map",
    "0:v:0",
    "-map",
    "1:a:0",
    "-shortest",
    "-c:v",
    "copy",
    "-c:a",
    "libopus",
    "-b:a",
    "128k",
    outRel,
  ];

  await runFfmpeg(ffmpegPath, args, { cwd: runDir });
}

function runFfmpeg(ffmpegPath, args, options = {}) {
  return runProcess(ffmpegPath, args, options);
}

async function concatMainAndEndForVoiceover({
  ffmpegPath,
  runDir,
  mainPath,
  endPath,
  mainDurationSeconds,
  outPath,
  durationSeconds,
}) {
  const rel = (abs) => path.relative(runDir, path.resolve(abs)).split(path.sep).join("/");
  const args = [
    "-y",
    "-t",
    String(mainDurationSeconds),
    "-i",
    rel(mainPath),
    "-i",
    rel(endPath),
    "-filter_complex",
    "[0:v][1:v]concat=n=2:v=1:a=0[outv]",
    "-map",
    "[outv]",
    "-t",
    String(durationSeconds),
    "-c:v",
    "libvpx-vp9",
    "-row-mt",
    "1",
    "-pix_fmt",
    "yuv420p",
    "-crf",
    "30",
    "-b:v",
    "0",
    rel(outPath),
  ];
  await runFfmpeg(ffmpegPath, args, { cwd: runDir });
}

async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function writeVoiceoverFailureStatus(runDir, error) {
  await writeJson(path.join(runDir, VOICEOVER_STATUS_FILE), {
    status: "failed",
    retryable: isRetryableElevenLabsError(error),
    lastAttemptAt: new Date().toISOString(),
    lastError: String(error?.message ?? error),
    attempts: Number(error?.elevenLabsAttempts) || null,
  });
}

async function clearVoiceoverFailureStatus(runDir) {
  await removeIfExists(path.join(runDir, VOICEOVER_STATUS_FILE));
}

async function removeIfExists(filePath) {
  await fs.rm(filePath, { force: true }).catch(() => {});
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function runProcess(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd ?? process.cwd(),
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });

    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("error", reject);

    child.on("close", (code) => {
      if (code === 0 || options.allowFailure) {
        resolve({ code, stdout, stderr });
        return;
      }
      reject(new Error(`ffmpeg failed (${code}): ${stderr || stdout}`.trim()));
    });
  });
}

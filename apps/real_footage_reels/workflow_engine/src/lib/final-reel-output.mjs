import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { spawn } from "node:child_process";

const PREVIEW_FILE_NAME = "final-reel-preview.mp4";

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

function parseBooleanLike(value, fallback = false) {
  const normalized = String(value ?? "").trim().toLowerCase();
  if (!normalized) return fallback;
  if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
  return fallback;
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

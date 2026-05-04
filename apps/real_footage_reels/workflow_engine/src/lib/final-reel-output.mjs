import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { spawn } from "node:child_process";

export async function publishFinalReelMp4(runDir, ffmpegPath, log = () => {}) {
  const normalizedDir = path.resolve(runDir);
  const sourcePath = path.join(normalizedDir, "final-reel.webm");
  const outPath = path.join(normalizedDir, "final-reel.mp4");

  let sourceStats;
  try {
    sourceStats = await fs.stat(sourcePath);
  } catch {
    return null;
  }

  try {
    const outStats = await fs.stat(outPath);
    if (outStats.mtimeMs >= sourceStats.mtimeMs) {
      return outPath;
    }
  } catch {
    // Publish a fresh MP4 below.
  }

  log("Publishing MP4 final reel...");
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
      "libx264",
      "-preset",
      "medium",
      "-crf",
      "18",
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
  return outPath;
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

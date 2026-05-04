import fs from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";

import { ensureDir, writeJson } from "./fs-utils.mjs";

export async function extractFirstFramesWithPython(config, downloads, runDir) {
  const framesDir = path.join(runDir, "frames");
  const inputManifestPath = path.join(runDir, "python-frame-input.json");
  const outputManifestPath = path.join(runDir, "frames-manifest.json");

  await ensureDir(framesDir);
  await writeJson(inputManifestPath, {
    createdAt: new Date().toISOString(),
    videos: downloads,
  });

  const scriptPath = path.join(process.cwd(), "scripts", "extract_first_frames.py");
  const args = [
    scriptPath,
    "--input",
    inputManifestPath,
    "--output-dir",
    framesDir,
    "--output-manifest",
    outputManifestPath,
    "--ffmpeg",
    config.ffmpegPath,
    "--shots",
    String(config.shotsPerClip),
    "--skip-start-seconds",
    String(config.clipStartSkipSeconds ?? 2),
  ];

  await runProcess(config.pythonPath, args, {
    cwd: process.cwd(),
  });

  const output = JSON.parse(await fs.readFile(outputManifestPath, "utf8"));
  return output.videos;
}

function runProcess(command, args, options) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      ...options,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      const text = chunk.toString();
      stdout += text;
      process.stdout.write(text);
    });

    child.stderr.on("data", (chunk) => {
      const text = chunk.toString();
      stderr += text;
      process.stderr.write(text);
    });

    child.on("error", (error) => {
      reject(error);
    });

    child.on("close", (code) => {
      if (code === 0) {
        resolve({ stdout, stderr });
        return;
      }

      reject(
        new Error(
          `Python frame extraction failed with exit code ${code}.\n${stderr || stdout}`.trim(),
        ),
      );
    });
  });
}

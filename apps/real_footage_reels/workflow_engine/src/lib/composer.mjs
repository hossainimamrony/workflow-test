import fs from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";

export async function composeSelectedClips(_context, selectedClips, outputPath, options) {
  if (!selectedClips.length) {
    throw new Error("No clips were provided for composition.");
  }

  const segments = [];
  const clipStartSkipSeconds = Math.max(0, Number(options.clipStartSkipSeconds ?? 0) || 0);
  for (const clip of selectedClips) {
    const inputDuration = await probeVideoDuration(options.ffmpegPath, clip.filePath);
    const durationSeconds = clip.durationSeconds ?? options.durationSeconds / selectedClips.length;

    segments.push({
      ...clip,
      durationSeconds,
      startTime: resolveSegmentStart(
        inputDuration,
        clip.startRatio ?? 0,
        durationSeconds,
        clipStartSkipSeconds,
      ),
    });
  }

  const { args, debugPayload } = buildFfmpegCommand(segments, outputPath, options);
  await fs.writeFile(
    path.join(path.dirname(outputPath), "composition-plan.json"),
    `${JSON.stringify(debugPayload, null, 2)}\n`,
    "utf8",
  );

  await runProcess(options.ffmpegPath, args, {
    cwd: path.dirname(outputPath),
    log: options.log,
    heartbeatLabel: "Main reel encoding",
  });
}

function buildFfmpegCommand(segments, outputPath, options) {
  const args = ["-y"];
  const codec = String(options.webmCodec || "libvpx-vp9").trim();
  const deadline = String(options.webmDeadline || "").trim();
  const cpuUsed = Number(options.webmCpuUsed);
  const crf = Number(options.webmCrf);
  const threads = Number(options.webmThreads);

  for (const segment of segments) {
    args.push(
      "-ss",
      segment.startTime.toFixed(3),
      "-t",
      segment.durationSeconds.toFixed(3),
      "-i",
      segment.filePath,
    );
  }

  const videoChains = segments.map((_, index) => (
    `[${index}:v]scale=${options.width}:${options.height}:force_original_aspect_ratio=increase,` +
    `crop=${options.width}:${options.height},setsar=1,fps=${options.fps},format=yuv420p,setpts=PTS-STARTPTS[v${index}]`
  ));
  const concatInputs = segments.map((_, index) => `[v${index}]`).join("");
  const filterComplex = `${videoChains.join(";")};${concatInputs}concat=n=${segments.length}:v=1:a=0[outv]`;

  args.push(
    "-filter_complex",
    filterComplex,
    "-map",
    "[outv]",
    "-an",
    "-c:v",
    codec,
  );
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
    "-pix_fmt",
    "yuv420p",
    "-crf",
    String(Number.isFinite(crf) ? crf : 30),
    "-b:v",
    "0",
    outputPath,
  );

  return {
    args,
    debugPayload: {
      createdAt: new Date().toISOString(),
      outputPath,
      width: options.width,
      height: options.height,
      fps: options.fps,
      codec,
      deadline: deadline || null,
      cpuUsed: Number.isFinite(cpuUsed) ? cpuUsed : null,
      threads: Number.isFinite(threads) ? threads : null,
      clipStartSkipSeconds: options.clipStartSkipSeconds ?? 0,
      durationSeconds: options.durationSeconds,
      segments: segments.map((segment) => ({
        clipId: segment.clipId,
        purpose: segment.purpose ?? segment.role,
        filePath: segment.filePath,
        startTime: segment.startTime,
        durationSeconds: segment.durationSeconds,
      })),
      filterComplex,
    },
  };
}

async function probeVideoDuration(ffmpegPath, filePath) {
  const ffprobePath = await resolveFfprobePath(ffmpegPath);

  if (ffprobePath) {
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
    if (Number.isFinite(duration) && duration > 0) {
      return duration;
    }
  }

  const fallback = await runProcess(ffmpegPath, ["-i", filePath], { allowFailure: true });
  const durationMatch = /Duration:\s+(\d+):(\d+):(\d+(?:\.\d+)?)/u.exec(fallback.stderr);
  if (!durationMatch) {
    throw new Error(`Could not determine video duration for ${filePath}.`);
  }

  const hours = Number(durationMatch[1]);
  const minutes = Number(durationMatch[2]);
  const seconds = Number(durationMatch[3]);
  return hours * 3600 + minutes * 60 + seconds;
}

async function resolveFfprobePath(ffmpegPath) {
  const candidates = [
    path.join(path.dirname(ffmpegPath), "ffprobe.exe"),
    path.join(path.dirname(ffmpegPath), "ffprobe"),
    "ffprobe",
  ];
  for (const probePath of candidates) {
    try {
      await runProcess(probePath, ["-version"], { allowFailure: false, maxCaptureBytes: 1024 * 8 });
      return probePath;
    } catch {
      continue;
    }
  }
  return null;
}

function resolveSegmentStart(inputDuration, startRatio, segmentDuration, minStartSeconds = 0) {
  const playableRange = Math.max(0, inputDuration - Math.max(0.1, segmentDuration) - 0.05);
  const skipStart = Math.min(playableRange, Math.max(0, Number(minStartSeconds) || 0));
  if (skipStart > 0.05) {
    return skipStart;
  }

  const ratioStart = playableRange * Math.max(0, Math.min(1, startRatio));
  return Math.max(0, Math.min(playableRange, ratioStart));
}

function runProcess(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd ?? process.cwd(),
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    let lastHeartbeatAt = 0;
    let stderrBuffer = "";

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });

    child.stderr.on("data", (chunk) => {
      const text = chunk.toString();
      stderr += text;
      stderrBuffer += text;

      const lines = stderrBuffer.split(/\r?\n/u);
      stderrBuffer = lines.pop() ?? "";
      for (const lineRaw of lines) {
        const line = String(lineRaw || "").trim();
        if (!line) continue;
        if (typeof options.log === "function" && line.startsWith("frame=")) {
          const now = Date.now();
          if (now - lastHeartbeatAt > 7000) {
            lastHeartbeatAt = now;
            const timeMatch = /time=\s*([0-9:.]+)/u.exec(line);
            const fpsMatch = /fps=\s*([0-9.]+)/u.exec(line);
            const speedMatch = /speed=\s*([0-9.]+x)/u.exec(line);
            const parts = [];
            if (timeMatch?.[1]) parts.push(`time ${timeMatch[1]}`);
            if (fpsMatch?.[1]) parts.push(`fps ${fpsMatch[1]}`);
            if (speedMatch?.[1]) parts.push(`speed ${speedMatch[1]}`);
            options.log(
              `${options.heartbeatLabel || "ffmpeg"} heartbeat${parts.length ? ` (${parts.join(", ")})` : ""}`,
            );
          }
        }
      }
    });

    child.on("error", (error) => {
      reject(error);
    });

    child.on("close", (code) => {
      if (code === 0 || options.allowFailure) {
        resolve({ code, stdout, stderr });
        return;
      }

      reject(
        new Error(
          `Composition process failed with exit code ${code}.\n${stderr || stdout}`.trim(),
        ),
      );
    });
  });
}

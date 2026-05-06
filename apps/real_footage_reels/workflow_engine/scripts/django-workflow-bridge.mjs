import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";

import { createRuntimeConfig, hasGeminiApiKey, loadEnvConfig } from "../src/lib/config.mjs";
import { publishFinalReelMp4 } from "../src/lib/final-reel-output.mjs";
import { buildRunReport } from "../src/lib/run-report.mjs";
import {
  composeSavedRun,
  continueWorkflow,
  executeWorkflow,
  rerenderRunEndScene,
} from "../src/lib/workflow.mjs";
import { generateRunThumbnail } from "../src/lib/thumbnail/generator.mjs";
import { applyVoiceoverToReel, draftVoiceoverScripts } from "../src/lib/voiceover.mjs";

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith("--")) continue;
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) {
      out[key] = "1";
      continue;
    }
    out[key] = next;
    i += 1;
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const payloadPath = String(args.payload || "").trim();
  if (!payloadPath) {
    throw new Error("Missing --payload <json-file>.");
  }

  const payloadRaw = await fs.readFile(payloadPath, "utf8");
  const payload = JSON.parse(payloadRaw);
  const rootDir = process.cwd();
  const env = loadEnvConfig(rootDir);

  const log = (message) => {
    process.stdout.write(`[LOG] ${String(message)}\n`);
  };
  const onProgress = (progress) => {
    process.stdout.write(`[PROGRESS] ${JSON.stringify(progress)}\n`);
  };

  const command = String(payload.command || "run").trim();
  const resumeRunId = String(payload.resumeRunId || "").trim();
  const outDir = String(payload.outDir || "").trim();
  const runDir = resumeRunId
    ? path.join(rootDir, "runs", resumeRunId)
    : outDir
      ? path.resolve(rootDir, outDir)
      : null;

  let result = null;
  if (command === "thumbnail") {
    if (!runDir) {
      throw new Error("Missing resumeRunId or outDir for thumbnail generation.");
    }
    if (!hasGeminiApiKey(env.GEMINI_API_KEY)) {
      throw new Error("GEMINI_API_KEY is required for thumbnails.");
    }

    const title = String(payload.title || "").trim();
    const subtitle = String(payload.subtitle || "").trim();
    const referenceImageDataUrl = String(payload.referenceImageDataUrl || "").trim();
    const price = String(payload.price || payload.listingPrice || "").trim() || "AU ";

    if (!title) {
      throw new Error("title is required.");
    }
    if (!subtitle) {
      throw new Error("subtitle is required.");
    }
    if (!referenceImageDataUrl) {
      throw new Error("referenceImageDataUrl is required.");
    }

    onProgress({ phase: "thumbnail", percent: 15, label: "Preparing" });
    log("Generating thumbnail with Gemini...");

    const generated = await generateRunThumbnail({
      runDir,
      geminiApiKey: env.GEMINI_API_KEY,
      imageModel: env.THUMBNAIL_GEMINI_MODEL || env.GEMINI_IMAGE_MODEL || "",
      referenceImageDataUrl,
      title,
      subtitle,
      price,
    });

    onProgress({ phase: "thumbnail", percent: 100, label: "Done" });
    result = {
      runDir,
      imagePath: generated.imagePath,
      imageMimeType: generated.imageMimeType,
    };
  } else if (command === "script-draft") {
    if (!runDir) {
      throw new Error("Missing outDir/runDir for script-draft.");
    }
    await fs.mkdir(runDir, { recursive: true });

    const manifestPath = path.join(runDir, "downloads-manifest.json");
    let existingManifest = null;
    try {
      existingManifest = JSON.parse(await fs.readFile(manifestPath, "utf8"));
    } catch {
      existingManifest = null;
    }

    const normalizedUrl = String(payload.url || "").trim();
    const normalizedUrls = Array.isArray(payload.urls)
      ? payload.urls.map((value) => String(value || "").trim()).filter(Boolean)
      : [];
    const albumUrls = normalizedUrls.length
      ? normalizedUrls
      : (normalizedUrl ? [normalizedUrl] : []);

    const manifest = {
      createdAt: existingManifest?.createdAt || new Date().toISOString(),
      albumUrls: albumUrls.length ? albumUrls : (Array.isArray(existingManifest?.albumUrls) ? existingManifest.albumUrls : []),
      listingTitle: String(payload.listingTitle || existingManifest?.listingTitle || "").trim(),
      stockId: String(payload.stockId || existingManifest?.stockId || "").trim(),
      carDescription: String(payload.carDescription || existingManifest?.carDescription || "").trim(),
      listingPrice: String(payload.listingPrice || existingManifest?.listingPrice || "").trim(),
      priceIncludes: payload.priceIncludes ?? existingManifest?.priceIncludes ?? null,
      videos: Array.isArray(existingManifest?.videos) ? existingManifest.videos : [],
    };
    await fs.writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");

    const config = createRuntimeConfig(
      {
        command: "run",
        urls: manifest.albumUrls ?? [],
        listingTitle: manifest.listingTitle ?? "",
        stockId: manifest.stockId ?? "",
        carDescription: manifest.carDescription ?? "",
        listingPrice: manifest.listingPrice ?? "",
        priceIncludes: manifest.priceIncludes ?? null,
        maxClips: null,
        compose: false,
        headless: true,
        voiceoverScriptApproval: true,
        browserProfileDir: path.join(runDir, ".browser-profile"),
      },
      env,
    );

    onProgress({ phase: "voiceover", percent: 20, label: "Script drafts" });
    const draft = await draftVoiceoverScripts(runDir, config, log, { strict: true });
    if (!draft?.variants?.length) {
      throw new Error(
        "Script draft failed: no script variants were generated. Check GEMINI_API_KEY and run logs for details.",
      );
    }
    result = { runDir };
  } else if (command === "voiceover-draft" || command === "voiceover-apply") {
    if (!runDir) {
      throw new Error("Missing resumeRunId for voice-over action.");
    }
    const config = createRuntimeConfig(
      {
        command: "run",
        urls: [],
        listingTitle: payload.listingTitle ?? "",
        stockId: payload.stockId ?? "",
        carDescription: payload.carDescription ?? "",
        listingPrice: payload.listingPrice ?? "",
        priceIncludes: payload.priceIncludes ?? null,
        maxClips: null,
        compose: true,
        headless: true,
        voiceoverScriptApproval: false,
        browserProfileDir: path.join(runDir, ".browser-profile"),
      },
      env,
    );

    if (command === "voiceover-draft") {
      onProgress({ phase: "voiceover", percent: 20, label: "Script drafts" });
      const draft = await draftVoiceoverScripts(runDir, config, log, { strict: true });
      if (!draft?.variants?.length) {
        throw new Error(
          "Script draft failed: no script variants were generated. Check GEMINI_API_KEY and run logs for details.",
        );
      }
    } else {
      const script = String(payload.approvedScript || "").trim();
      if (!script) {
        throw new Error("Missing approvedScript for voice-over apply.");
      }
      onProgress({ phase: "voiceover", percent: 20, label: "Voice-over" });
      await applyVoiceoverToReel(runDir, config, log, { approvedScript: script });
    }

    result = { runDir };
  } else {
    const config = createRuntimeConfig(
      {
        command: command === "compose" ? "run" : command,
        urls: payload.urls || (payload.url ? [payload.url] : []),
        listingTitle: payload.listingTitle ?? "",
        stockId: payload.stockId ?? "",
        carDescription: payload.carDescription ?? "",
        listingPrice: payload.listingPrice ?? "",
        priceIncludes: payload.priceIncludes ?? null,
        maxClips: payload.maxClips ?? null,
        outDir: runDir,
        compose: Boolean(payload.compose),
        headless: payload.headless !== false,
        voiceoverScriptApproval: payload.voiceoverScriptApproval ?? true,
        browserProfileDir: runDir ? path.join(runDir, ".browser-profile") : undefined,
      },
      env,
    );

    if (command === "compose") {
      if (!runDir) throw new Error("Missing resumeRunId for compose.");
      const approvedScript = String(payload.approvedScript || "").trim();
      if (!approvedScript) {
        throw new Error("Approve/select a script first, then generate full video.");
      }
      const report = await buildRunReport(runDir, rootDir);
      if (!report?.pipeline?.analyze?.done) {
        throw new Error(
          "Analysis/plan is not ready yet. Run 'Prepare Footage + Plan' first, then compose.",
        );
      }
      result = await composeSavedRun(runDir, config, {
        log,
        onProgress,
        approvedScript,
      });
    } else if (command === "end-scene-rerender") {
      if (!runDir) throw new Error("Missing resumeRunId for end-scene-rerender.");
      result = await rerenderRunEndScene(runDir, config, { log, onProgress });
    } else if (resumeRunId) {
      result = await continueWorkflow(runDir, config, { log, onProgress });
    } else {
      result = await executeWorkflow(config, { log, onProgress });
    }
  }

  const finalRunDir = path.resolve(result?.runDir || runDir || "");
  const finalReport = finalRunDir ? await buildRunReport(finalRunDir, rootDir) : null;
  process.stdout.write(
    `[RESULT] ${JSON.stringify({
      runDir: finalRunDir,
      report: finalReport,
      imagePath: result?.imagePath ?? "",
      imageMimeType: result?.imageMimeType ?? "",
      publishManifest: result?.publishManifest ?? null,
    })}\n`,
  );
}

main().catch((error) => {
  process.stderr.write(`[ERROR] ${error?.stack || String(error)}\n`);
  process.exitCode = 1;
});

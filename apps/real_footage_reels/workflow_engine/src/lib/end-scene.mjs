import fs from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

import { launchRenderBrowser } from "./browser.mjs";
import { resolveReelDurations } from "./config.mjs";
import { writeJson } from "./fs-utils.mjs";

const DEFAULT_PRICE_INCLUDES = ["6 Months NSW Registration", "Fresh Roadworthy Certificate"];
const DEFAULT_END_FPS = 30;
const SAMPLE_END_SCENE_VERSION = 24;
const END_SCENE_SUPERSAMPLE = 2;
const END_SCENE_VP9_CRF = 18;
const END_SCENE_FONT_DIR = new URL("../../ui/fontfamily/", import.meta.url);
const END_SCENE_TEMPLATE_FILE = new URL("./templates/end-scene.browser.html", import.meta.url);
const DEFAULT_END_SCENE_SAMPLE = Object.freeze({
  listingTitle: "2024 Toyota Prius 4WD Hybrid G Package (Low KM)",
  stockId: "",
  listingPrice: "AU$43,900",
  priceIncludes: [
    "5 Year Extended Warranty",
    "6 Months NSW Registration",
    "Fresh Roadworthy Certificate",
  ],
});

/**
 * Renders a branded Carbarn end card (silent .webm), then concatenates after main reel.
 * Main length + end length must stay within config.maxTotalReelDurationSeconds (default 17).
 */
export async function appendEndSceneToReel(runDir, config, manifest, log = () => {}, options = {}) {
  const mainPath = path.join(runDir, "main-reel.webm");
  const finalPath = path.join(runDir, "final-reel.webm");
  const reelDurations = resolveReelDurations(config);
  const mainSec = reelDurations.composeMainDurationSeconds;
  const endSec = reelDurations.endSceneDurationSeconds;

  await fs.access(mainPath).catch(() => {
    throw new Error("main-reel.webm not found; compose step must run first.");
  });

  const meta = buildEndSceneMeta(manifest);
  const renderInfo = await renderEndScene({
    runDir,
    config,
    meta,
    durationSeconds: endSec,
    log,
    browserContext: options.browserContext ?? null,
  });

  log("Concatenating main reel + end scene...");
  await concatVideosVertical({
    config,
    ffmpegPath: config.ffmpegPath,
    cwd: runDir,
    mainRelativePath: "main-reel.webm",
    endRelativePath: "end-scene.webm",
    mainDurationSeconds: mainSec,
    endDurationSeconds: endSec,
    outRelativePath: "final-reel.webm",
  });

  await writeJson(path.join(runDir, "end-scene-manifest.json"), {
    createdAt: new Date().toISOString(),
    mainDurationSeconds: mainSec,
    endDurationSeconds: endSec,
    totalDurationSeconds: mainSec + endSec,
    renderer: renderInfo.renderer,
    htmlPath: renderInfo.htmlPath ?? null,
    framesDir: renderInfo.framesDir ?? null,
    ...meta,
  });

  log(`Combined reel: ${finalPath} (${(mainSec + endSec).toFixed(1)}s).`);
}

export async function ensureSampleEndScene(rootDir, config, log = () => {}, options = {}) {
  const sampleDir = path.join(rootDir, ".ui-cache", "end-scene-sample");
  const manifestPath = path.join(sampleDir, "sample-manifest.json");
  const videoPath = path.join(sampleDir, "end-scene.webm");
  const sourceVersion = await getEndSceneSourceVersion();
  const durationSeconds = clampEndSceneSeconds(
    options.durationSeconds ?? config.endSceneDurationSeconds ?? 3.5,
  );
  const force = Boolean(options.force);

  if (!force) {
    const [cachedManifest, cachedVideoPath] = await Promise.all([
      readJsonIfExists(manifestPath),
      fileIfExists(videoPath),
    ]);

    if (
      cachedVideoPath &&
      cachedManifest?.version === SAMPLE_END_SCENE_VERSION &&
      cachedManifest?.durationSeconds === durationSeconds &&
      cachedManifest?.sourceVersion === sourceVersion
    ) {
      return {
        sampleDir,
        videoPath: cachedVideoPath,
        manifestPath,
        durationSeconds,
        renderer: String(cachedManifest.renderer ?? "unknown"),
        meta: cachedManifest.meta ?? buildEndSceneMeta(DEFAULT_END_SCENE_SAMPLE),
        debug: cachedManifest.debug ?? null,
      };
    }
  }

  await fs.mkdir(sampleDir, { recursive: true });

  const meta = buildEndSceneMeta({
    ...DEFAULT_END_SCENE_SAMPLE,
    ...options.meta,
  });
  const renderInfo = await renderEndScene({
    runDir: sampleDir,
    config,
    meta,
    durationSeconds,
    log,
    browserContext: options.browserContext ?? null,
  });

  await writeJson(manifestPath, {
    createdAt: new Date().toISOString(),
    version: SAMPLE_END_SCENE_VERSION,
    sourceVersion,
    durationSeconds,
    renderer: renderInfo.renderer,
    htmlPath: renderInfo.htmlPath ?? null,
    framesDir: renderInfo.framesDir ?? null,
    debug: renderInfo.debug ?? null,
    meta,
  });

  return {
    sampleDir,
    videoPath,
    manifestPath,
    durationSeconds,
    renderer: renderInfo.renderer,
    meta,
    debug: renderInfo.debug ?? null,
  };
}

async function renderEndScene({ runDir, config, meta, durationSeconds, log, browserContext }) {
  log(`Rendering ${durationSeconds.toFixed(1)}s animated end scene in Chromium...`);
  return await renderAnimatedEndScene({
    runDir,
    config,
    meta,
    durationSeconds,
    log,
    browserContext,
  });
}

async function renderAnimatedEndScene({ runDir, config, meta, durationSeconds, log, browserContext }) {
  const width = config.composeWidth;
  const height = config.composeHeight;
  const supersample = Math.max(1, Math.min(2, Number(config.endSceneSupersample ?? END_SCENE_SUPERSAMPLE) || END_SCENE_SUPERSAMPLE));
  const renderWidth = width * supersample;
  const renderHeight = height * supersample;
  const fps = config.composeFps ?? DEFAULT_END_FPS;
  const frameCount = Math.max(1, Math.round(durationSeconds * fps));
  const htmlPath = path.join(runDir, "end-scene.browser.html");
  const framesDir = path.join(runDir, "end-scene-frames");
  const templateResult = await buildAnimatedEndSceneHtmlFromSampleTemplate({
    durationSeconds,
  });
  const html = templateResult.html;
  const debug = templateResult.debug;

  await fs.writeFile(htmlPath, html, "utf8");
  await fs.rm(framesDir, { recursive: true, force: true }).catch(() => {});
  await fs.mkdir(framesDir, { recursive: true });

  let ownedBrowserSession = null;
  let context = browserContext ?? null;
  if (!context) {
    ownedBrowserSession = await launchRenderBrowser(config);
    context = ownedBrowserSession.context;
  }

  const page = await context.newPage();
  try {
    await page.setViewportSize({ width: renderWidth, height: renderHeight });
    await page.setContent(html, { waitUntil: "load" });
    const patchApplied = await applySampleTemplatePatchInPage(page, meta, durationSeconds);
    if (!patchApplied.ok) {
      throw new Error(`Sample end-scene template selector missing: ${patchApplied.missing.join(", ")}`);
    }
    debug.appliedTokens = [...debug.appliedTokens, ...patchApplied.appliedTokens];
    await page.waitForFunction(() => typeof window.__setSceneTime === "function");
    await page.waitForFunction(() => window.__headlineFitted === true);

    for (let frameIndex = 0; frameIndex < frameCount; frameIndex += 1) {
      const ms = (frameIndex / fps) * 1000;
      await page.evaluate(async (value) => {
        await window.__setSceneTime(value);
      }, ms);
      await page.screenshot({
        path: path.join(framesDir, `frame-${String(frameIndex).padStart(4, "0")}.png`),
        type: "png",
      });
      if (typeof log === "function") {
        const every = Math.max(1, Math.floor(frameCount / 5));
        if ((frameIndex + 1) % every === 0 || frameIndex === frameCount - 1) {
          log(`End scene frames ${frameIndex + 1}/${frameCount}`);
        }
      }
    }
  } finally {
    await page.close().catch(() => {});
    if (ownedBrowserSession) {
      await ownedBrowserSession.close().catch(() => {});
    }
  }

  await encodeFramesToWebm({
    config,
    ffmpegPath: config.ffmpegPath,
    cwd: runDir,
    fps,
    durationSeconds,
    framesRelativePattern: path.join("end-scene-frames", "frame-%04d.png"),
    outRelativePath: "end-scene.webm",
    width,
    height,
  });

  return {
    renderer: "browser_animation",
    htmlPath: "end-scene.browser.html",
    framesDir: "end-scene-frames",
    debug,
  };
}

async function buildAnimatedEndSceneHtmlFromSampleTemplate(options) {
  let html;
  try {
    html = await fs.readFile(END_SCENE_TEMPLATE_FILE, "utf8");
  } catch (error) {
    throw new Error(
      `Missing required end-scene template at ${fileURLToPath(END_SCENE_TEMPLATE_FILE)}.`,
    );
  }

  const debug = {
    templatePath: fileURLToPath(END_SCENE_TEMPLATE_FILE),
    appliedTokens: [],
    fallbackUsed: false,
    errorMessage: null,
  };
  return { html, debug };
}

async function applySampleTemplatePatchInPage(page, meta, durationSeconds) {
  const durationMs = Math.max(1000, Math.round(durationSeconds * 1000));
  const headline = createHeadlineLayout(meta.listingTitle);
  const includes = meta.priceIncludes.slice(0, 3);
  return page.evaluate(({ payload }) => {
    const escapeHtmlInBrowser = (value) =>
      String(value ?? "")
        .replace(/&/gu, "&amp;")
        .replace(/</gu, "&lt;")
        .replace(/>/gu, "&gt;")
        .replace(/"/gu, "&quot;");
    const missing = [];
    const appliedTokens = [];
    const root = document.documentElement;
    if (!root) {
      return { ok: false, missing: ["documentElement"], appliedTokens };
    }
    root.style.setProperty("--scene-duration", `${payload.durationMs}ms`);
    appliedTokens.push("scene duration");

    const headlineNode = document.querySelector(".headline");
    if (!headlineNode) {
      missing.push(".headline");
    } else {
      headlineNode.innerHTML = payload.headlineLines
        .map((line) => `<span class=\"headline__line\">${escapeHtmlInBrowser(line)}</span>`)
        .join("");
      headlineNode.style.setProperty("--headline-fit-scale", String(payload.headlineScale));
      appliedTokens.push("headline");
    }

    const priceNode = document.querySelector(".price-value");
    if (!priceNode) {
      missing.push(".price-value");
    } else {
      priceNode.textContent = payload.priceLine;
      appliedTokens.push("price");
    }

    const includesList = document.querySelector(".includes");
    if (!includesList) {
      missing.push(".includes");
    } else {
      includesList.innerHTML = payload.includes
        .map((line, index) => `<li class=\"include include--${index + 1}\">${escapeHtmlInBrowser(line)}</li>`)
        .join("");
      appliedTokens.push("price includes");
    }

    if (!missing.length && typeof window.__refitHeadline === "function") {
      return Promise.resolve(window.__refitHeadline()).then(() => ({
        ok: true,
        missing,
        appliedTokens: [...appliedTokens, "headline refit"],
      }));
    }

    return { ok: missing.length === 0, missing, appliedTokens };
  }, {
    payload: {
      durationMs,
      headlineLines: headline.lines,
      headlineScale: headline.scale.toFixed(3),
      priceLine: meta.priceLine,
      includes,
    },
  });
}

// Legacy static fallback only. Browser-rendered production output always comes from the
// canonical template file in src/lib/templates/end-scene.browser.html.
async function renderStaticEndScene({ runDir, config, meta, durationSeconds, log }) {
  const assPath = path.join(runDir, "end-scene.ass");
  await fs.writeFile(assPath, buildEndSceneAss(meta, config.composeWidth, config.composeHeight, durationSeconds), "utf8");

  log(`Rendering ${durationSeconds.toFixed(1)}s fallback static end scene...`);
  await renderEndSceneWebm({
    ffmpegPath: config.ffmpegPath,
    cwd: runDir,
    assRelativePath: "end-scene.ass",
    outRelativePath: "end-scene.webm",
    durationSeconds,
    width: config.composeWidth,
    height: config.composeHeight,
  });
}

function buildEndSceneMeta(manifest) {
  const listingTitle = String(manifest?.listingTitle ?? "").trim() || "Vehicle";
  const stockId = String(manifest?.stockId ?? "").trim();
  const listingPrice =
    String(manifest?.listingPrice ?? "").trim() || extractPriceFromDescription(String(manifest?.carDescription ?? ""));
  const priceLine = listingPrice || "See carbarn.com.au for pricing";
  const priceIncludes = normalizePriceIncludes(manifest?.priceIncludes);
  return {
    listingTitle,
    stockId,
    priceLine,
    priceIncludes,
  };
}

function normalizePriceIncludes(value) {
  if (Array.isArray(value)) {
    return value.map((s) => String(s).trim()).filter(Boolean);
  }
  if (typeof value === "string" && value.trim()) {
    return value
      .split(/\r?\n/u)
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return [...DEFAULT_PRICE_INCLUDES];
}

export function extractPriceFromDescription(text) {
  const t = String(text ?? "");
  const au = t.match(/\bAU\$[\d,]+(?:\.\d{2})?\b/u);
  if (au) {
    return au[0];
  }
  const dollar = t.match(/\$[\d,]+(?:\.\d{2})?\b/u);
  if (dollar) {
    return dollar[0].startsWith("$") ? `AU${dollar[0]}` : dollar[0];
  }
  const priced = t.match(/\bPriced at\s+([^.\n]+)/iu);
  if (priced) {
    return priced[1].trim();
  }
  return "";
}

function clampEndSceneSeconds(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) {
    return 3.5;
  }
  return Math.min(5, Math.max(3, Math.round(n * 10) / 10));
}

// Legacy inline browser markup retained only as a dormant fallback reference while the
// canonical production template lives in src/lib/templates/end-scene.browser.html.
function buildAnimatedEndSceneHtml(meta, options) {
  const durationMs = Math.max(1000, Math.round(options.durationSeconds * 1000));
  const scale = options.width / 1080;
  const includesHtml = meta.priceIncludes
    .slice(0, 3)
    .map((item, index) => `<li class="include include--${index + 1}">${escapeHtml(item)}</li>`)
    .join("");
  const headline = createHeadlineLayout(meta.listingTitle);
  const headlineHtml = buildHeadlineHtml(headline.lines);

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Animated End Scene</title>
  <style>
    @font-face {
      font-family: "MontserratLocal";
      src: url("${options.titleFontDataUrl}") format("truetype");
      font-style: normal;
      font-weight: 700;
      font-display: block;
    }

    :root {
      --scene-duration: ${durationMs}ms;
      --scale: ${scale};
      --bg-top: #1677f2;
      --bg-mid: #1677f2;
      --bg-bottom: #1677f2;
      --copy: #ffffff;
      --copy-soft: #ffffff;
      --copy-faint: #ffffff;
      --line: #ffffff;
      --line-soft: rgba(255, 255, 255, 0.24);
      --pill-blue: #347fe8;
      --font-title: "MontserratLocal";
      --shadow: rgba(9, 44, 101, 0.22);
      --font-main: "MontserratLocal";
      --font-poppins: "MontserratLocal";
      --size-logo: calc(40px * var(--scale));
      --size-title: calc(74px * var(--scale));
      --size-available: calc(31px * var(--scale));
      --size-price-label: calc(31px * var(--scale));
      --size-price: calc(74px * var(--scale));
      --size-includes-label: calc(31px * var(--scale));
      --size-includes:  calc(36px * var(--scale));
      --size-ready-label: calc(31px * var(--scale));
      --size-ready-value: calc(31px * var(--scale));
      --size-footer-brand: calc(44px * var(--scale));
      --size-url: calc(35px * var(--scale));
    }

    * {
      box-sizing: border-box;
    }

    html, body {
      margin: 0;
      width: 100%;
      height: 100%;
      overflow: hidden;
      background: #156fe7;
    }

    body {
      font-family: var(--font-main);
      color: var(--copy);
      -webkit-font-smoothing: antialiased;
      text-rendering: geometricPrecision;
    }

    .scene {
      position: relative;
      width: 100vw;
      height: 100vh;
      overflow: hidden;
      background: var(--bg-top);
      isolation: isolate;
    }

    .scene::before,
    .scene::after {
      content: "";
      position: absolute;
      inset: 0;
      pointer-events: none;
    }

    .scene::before {
      background: none;
      opacity: 0;
      animation: glowIn var(--scene-duration) linear both paused;
    }

    .scene::after {
      background: none;
      opacity: 0;
      animation: backdropShift var(--scene-duration) linear both paused;
    }

    .brand-row {
      position: absolute;
      top: 5%;
      left: 50%;
      z-index: 2;
      justify-content: center;
      transform: translateX(-50%);
      animation: logoIn var(--scene-duration) linear both paused;
    }

    .brand {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: calc(408px * var(--scale));
      min-height: calc(72px * var(--scale));
      padding: calc(8px * var(--scale)) calc(28px * var(--scale));
      border-radius: 999px;
      border: calc(3.4px * var(--scale)) solid var(--line);
      background: transparent;
      box-shadow: none;
      text-transform: uppercase;
      letter-spacing: -0.01em;
      font-family: var(--font-main);
      font-size: var(--size-logo);
      font-weight: 900;
      line-height: 1;
    }

    .card {
      position: absolute;
      left: 8.2%;
      top: 11.3%;
      width: 83.6%;
      height: 78.3%;
      z-index: 1;
      border: calc(3.1px * var(--scale)) solid var(--line);
      border-radius: calc(10px * var(--scale));
      box-shadow: none;
      animation: cardIn var(--scene-duration) linear both paused;
    }

    .card__content {
      position: relative;
      height: 100%;
      text-align: center;
    }

    .title-block,
    .price-block,
    .includes-block,
    .ready-block,
    .footer {
      box-sizing: border-box;
      padding-inline: calc(12px * var(--scale));
    }

    .title-block {
      position: absolute;
      top: 10.2%;
      left: 50%;
      width: 76%;
      max-width: calc(760px * var(--scale));
      display: grid;
      gap: calc(6px * var(--scale));
      justify-items: center;
      padding-inline: calc(20px * var(--scale));
      transform: translateX(-50%);
    }

    .headline {
      margin: 0;
      width: 100%;
      font-family: var(--font-title);
      font-size: calc(var(--size-title) * var(--headline-fit-scale, 1));
      line-height: 1.02;
      letter-spacing: -0.04em;
      font-weight: 900;
      color: var(--copy);
      display: grid;
      gap: calc(4px * var(--scale));
      justify-items: center;
      animation: titleIn var(--scene-duration) linear both paused;
    }

    .headline__line {
      display: block;
      max-width: 100%;
      white-space: nowrap;
      text-align: center;
    }

    .headline,
    .subheadline,
    .price-label,
    .price-value,
    .ready-label,
    .ready-text,
    .url-pill {
      font-family: var(--font-main);
    }

    .subheadline {
      margin: 0;
      width: 100%;
      font-family: var(--font-main);
      font-size: var(--size-available);
      line-height: 1.05;
      color: var(--copy-soft);
      font-weight: 400;
      letter-spacing: -0.015em;
      text-align: center;
      animation: subtitleIn var(--scene-duration) linear both paused;
    }

    .price-block {
      position: absolute;
      top: 35.1%;
      left: 50%;
      width: 68%;
      display: grid;
      gap: calc(10px * var(--scale));
      justify-items: center;
      transform: translateX(-50%);
    }

    .price-label,
    .includes-label,
    .ready-label {
      margin: 0;
      color: var(--copy-faint);
      line-height: 1;
    }

    .price-label {
      font-family: var(--font-main);
      font-size: var(--size-price-label);
      font-weight: 400;
      letter-spacing: -0.015em;
    }

    .includes-label {
      font-family: var(--font-main);
      font-size: var(--size-includes-label);
      font-weight: 400;
      letter-spacing: -0.015em;
    }

    .ready-label {
      font-family: var(--font-main);
      font-size: var(--size-ready-label);
      line-height: 1;
      font-weight: 400;
      letter-spacing: -0.015em;
    }

    .price-reveal {
      position: relative;
      width: 100%;
      min-height: calc(82px * var(--scale));
      overflow: hidden;
      display: grid;
      place-items: center;
    }

    .price-wipe {
      position: absolute;
      left: 15%;
      top: calc(11px * var(--scale));
      width: 70%;
      height: calc(52px * var(--scale));
      border-radius: calc(2px * var(--scale));
      background: rgba(255, 255, 255, 0.98);
      box-shadow: 0 10px 22px rgba(255, 255, 255, 0.08);
      transform-origin: left center;
      animation: priceWipe var(--scene-duration) linear both paused;
    }

    .price-value {
      position: relative;
      z-index: 1;
      margin: 0;
      font-family: var(--font-main);
      font-size: var(--size-price);
      line-height: 1;
      font-weight: 800;
      letter-spacing: -0.055em;
      color: var(--copy);
      animation: priceIn var(--scene-duration) linear both paused;
    }

    .includes-block {
      position: absolute;
      top: 52.4%;
      left: 50%;
      width: 68%;
      display: grid;
      gap: calc(8px * var(--scale));
      justify-items: center;
      transform: translateX(-50%);
    }

    .includes {
      display: grid;
      gap: calc(4px * var(--scale));
      margin: 0;
      padding: 0;
      list-style: none;
      width: 100%;
      justify-items: center;
    }

    .include {
      font-family: var(--font-main);
      color: var(--copy);
      font-size: var(--size-includes);
      line-height: 1.16;
      font-weight: 700;
      letter-spacing: -0.02em;
      overflow-wrap: anywhere;
      text-align: center;
    }

    .include--1 {
      animation: include1 var(--scene-duration) linear both paused;
    }

    .include--2 {
      animation: include2 var(--scene-duration) linear both paused;
    }

    .include--3 {
      animation: include3 var(--scene-duration) linear both paused;
    }

    .ready-block {
      position: absolute;
      top: 70%;
      left: 50%;
      width: 66%;
      display: grid;
      gap: calc(6px * var(--scale));
      justify-items: center;
      transform: translateX(-50%);
      animation: readyIn var(--scene-duration) linear both paused;
    }

    .ready-text {
      margin: 0;
      font-family: var(--font-main);
      font-size: var(--size-ready-value);
      line-height: 1.08;
      color: var(--copy-soft);
      font-weight: 400;
      letter-spacing: -0.02em;
      text-align: center;
    }

    .footer {
      position: absolute;
      left: 50%;
      bottom: 6.7%;
      width: 72%;
      display: grid;
      gap: calc(12px * var(--scale));
      justify-items: center;
      transform: translateX(-50%);
      animation: footerIn var(--scene-duration) linear both paused;
    }

    .footer-brand {
      margin: 0;
      font-family: var(--font-main);
      font-size: var(--size-footer-brand);
      line-height: 0.96;
      font-weight: 900;
      letter-spacing: -0.04em;
      text-transform: uppercase;
      white-space: nowrap;
      color: var(--copy);
    }

    .url-pill {
      width: 100%;
      min-height: calc(78px * var(--scale));
      padding: calc(14px * var(--scale)) calc(28px * var(--scale));
      border-radius: 999px;
      border: 0;
      background: #ffffff;
      box-shadow: none;
      display: grid;
      place-items: center;
      color: var(--pill-blue);
      font-family: var(--font-main);
      font-size: var(--size-url);
      line-height: 1;
      font-weight: 400;
      letter-spacing: -0.02em;
      white-space: nowrap;
      overflow-wrap: anywhere;
      animation: urlIn var(--scene-duration) linear both paused;
    }

    @keyframes backdropShift {
      0%, 100% { opacity: 1; transform: translateY(0); }
      50% { opacity: 1; transform: translateY(-0.4%); }
    }

    @keyframes glowIn {
      0%, 8% { opacity: 0; }
      20%, 100% { opacity: 1; }
    }

    @keyframes logoIn {
      0%, 8% { opacity: 0; transform: translateX(-50%) translateY(-18px) scale(0.92); }
      18%, 100% { opacity: 1; transform: translateX(-50%) translateY(0) scale(1); }
    }

    @keyframes cardIn {
      0%, 14% { opacity: 0; transform: scale(0.985); }
      28%, 100% { opacity: 1; transform: scale(1); }
    }

    @keyframes titleIn {
      0%, 22% { opacity: 0; transform: translateY(22px); filter: blur(8px); }
      38%, 100% { opacity: 1; transform: translateY(0); filter: blur(0); }
    }

    @keyframes subtitleIn {
      0%, 28% { opacity: 0; transform: translateY(14px); }
      42%, 100% { opacity: 1; transform: translateY(0); }
    }

    @keyframes priceWipe {
      0%, 34% { opacity: 0; transform: translateX(0) scaleX(0.18); }
      42% { opacity: 1; transform: translateX(0) scaleX(0.58); }
      49% { opacity: 1; transform: translateX(10%) scaleX(1); }
      57%, 100% { opacity: 0; transform: translateX(24%) scaleX(0.7); }
    }

    @keyframes priceIn {
      0%, 48% { opacity: 0; transform: translateY(6px) scale(0.98); filter: blur(6px); }
      60%, 100% { opacity: 1; transform: translateY(0) scale(1); filter: blur(0); }
    }

    @keyframes include1 {
      0%, 56% { opacity: 0; transform: translateY(14px); }
      68%, 100% { opacity: 1; transform: translateY(0); }
    }

    @keyframes include2 {
      0%, 60% { opacity: 0; transform: translateY(14px); }
      72%, 100% { opacity: 1; transform: translateY(0); }
    }

    @keyframes include3 {
      0%, 64% { opacity: 0; transform: translateY(14px); }
      76%, 100% { opacity: 1; transform: translateY(0); }
    }

    @keyframes readyIn {
      0%, 70% { opacity: 0; transform: translateX(-50%) translateY(18px); }
      84%, 100% { opacity: 1; transform: translateX(-50%) translateY(0); }
    }

    @keyframes footerIn {
      0%, 78% { opacity: 0; transform: translateX(-50%) translateY(20px); }
      90%, 100% { opacity: 1; transform: translateX(-50%) translateY(0); }
    }

    @keyframes urlIn {
      0%, 84% { opacity: 0; transform: translateY(18px) scale(0.92); }
      96%, 100% { opacity: 1; transform: translateY(0) scale(1); }
    }
  </style>
</head>
<body>
  <main class="scene">
    <div class="brand-row">
      <div class="brand">CARBARN</div>
    </div>

    <section class="card">
      <div class="card__content">
        <div class="title-block">
          <h1 class="headline" style="--headline-fit-scale: ${headline.scale.toFixed(3)};">${headlineHtml}</h1>
          <p class="subheadline">is Available Now!</p>
        </div>

        <div class="price-block">
          <p class="price-label">Price</p>
          <div class="price-reveal">
            <div class="price-wipe"></div>
            <p class="price-value">${escapeHtml(meta.priceLine)}</p>
          </div>
        </div>

        <div class="includes-block">
          <p class="includes-label">Price Includes</p>
          <ul class="includes">
            ${includesHtml}
          </ul>
        </div>

        <div class="ready-block">
          <p class="ready-label">Ready for</p>
          <p class="ready-text">Pickup/Door Delivery</p>
        </div>

        <footer class="footer">
          <p class="footer-brand">CARBARN AUSTRALIA</p>
          <div class="url-pill">www.carbarn.com.au</div>
        </footer>
      </div>
    </section>
  </main>

  <script>
    (function () {
      const settle = () => new Promise((resolve) => {
        requestAnimationFrame(() => requestAnimationFrame(resolve));
      });

      const fitHeadline = async () => {
        const headline = document.querySelector(".headline");
        const lines = headline ? Array.from(headline.querySelectorAll(".headline__line")) : [];
        if (!headline || !lines.length) {
          window.__headlineFitted = true;
          return;
        }

        const initialScale = Number.parseFloat(
          getComputedStyle(headline).getPropertyValue("--headline-fit-scale"),
        ) || 1;
        const sceneScale = Number.parseFloat(
          getComputedStyle(document.documentElement).getPropertyValue("--scale"),
        ) || 1;
        const minimumScale = Math.max(0.82, initialScale - 0.18);
        let currentScale = initialScale;
        let attempts = 0;

        const fits = () => {
          const safeInset = 24 * sceneScale;
          const limit = headline.getBoundingClientRect().width - safeInset;
          return lines.every((line) => line.getBoundingClientRect().width <= limit);
        };

        while (!fits() && currentScale > minimumScale && attempts < 24) {
          currentScale = Math.max(minimumScale, Number((currentScale - 0.01).toFixed(3)));
          headline.style.setProperty("--headline-fit-scale", String(currentScale));
          await settle();
          attempts += 1;
        }

        window.__headlineFitted = true;
      };

      window.__setSceneTime = async function (ms) {
        for (const animation of document.getAnimations()) {
          animation.pause();
          animation.currentTime = ms;
        }
        await settle();
      };

      window.__headlineFitted = false;
      Promise.resolve()
        .then(async () => {
          if (document.fonts?.ready) {
            await document.fonts.ready;
          }
          await fitHeadline();
        })
        .catch(() => {
          window.__headlineFitted = true;
        });

      window.__setSceneTime(0);
    }());
  </script>
</body>
</html>`;
}

function buildEndSceneAss(meta, width, height, durationSeconds) {
  const end = formatAssTime(durationSeconds);
  const headline = createHeadlineLayout(meta.listingTitle);
  const title = buildHeadlineAss(headline.lines);
  const price = esc(meta.priceLine);
  const b1 = esc(meta.priceIncludes[0] ?? DEFAULT_PRICE_INCLUDES[0]);
  const b2 = esc(meta.priceIncludes[1] ?? DEFAULT_PRICE_INCLUDES[1]);
  const b3 = meta.priceIncludes[2] ? esc(meta.priceIncludes[2]) : "";

  const cx = Math.round(width / 2);
  const h = height;
  const minDim = Math.min(width, height);
  const fsLogo = scaleTypography(40, minDim);
  const fsHead = scaleTypography(Math.round(74 * headline.scale), minDim);
  const fsSub = scaleTypography(31, minDim);
  const fsPriceLab = scaleTypography(31, minDim);
  const fsPrice = scaleTypography(74, minDim);
  const fsIncLab = scaleTypography(31, minDim);
  const fsInc = scaleTypography(36, minDim);
  const fsReadyLab = scaleTypography(31, minDim);
  const fsDel = scaleTypography(31, minDim);
  const fsFoot = scaleTypography(44, minDim);
  const fsUrl = scaleTypography(35, minDim);
  const fontMain = "Montserrat";

  const yLogo = Math.round(h * 0.067);
  const yTitle = Math.round(h * 0.228);
  const yAvail = Math.round(h * 0.305);
  const yPLab = Math.round(h * 0.401);
  const yPVal = Math.round(h * 0.443);
  const yILab = Math.round(h * 0.546);
  const yB1 = Math.round(h * 0.585);
  const yB2 = Math.round(h * 0.619);
  const yB3 = Math.round(h * 0.653);
  const yReady = Math.round(h * 0.723);
  const yPick = Math.round(h * 0.761);
  const yAus = Math.round(h * 0.828);
  const yUrl = Math.round(h * 0.872);

  const lines = [
    `[Script Info]`,
    `Title: Carbarn end scene`,
    `ScriptType: v4.00+`,
    `PlayResX: ${width}`,
    `PlayResY: ${height}`,
    `WrapStyle: 0`,
    `ScaledBorderAndShadow: yes`,
    ``,
    `[V4+ Styles]`,
    `Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding`,
    `Style: Default,${fontMain},${fsHead},&H00FFFFFF,&H000000FF,&H00000000,&H00000000,0,0,0,0,0,0,2,40,40,30,1`,
    `Style: UrlPill,${fontMain},${fsUrl},&H00E87F34,&H000000FF,&H00000000,&H00FFFFFF,0,0,3,0,0,2,40,40,30,1`,
    ``,
    `[Events]`,
    `Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text`,
    `Dialogue: 0,0:00:00.00,${end},Default,,0,0,0,,{\\an5\\pos(${cx},${yLogo})\\fn${fontMain}\\b1\\fs${fsLogo}\\c&HFFFFFF&\\bord3\\3c&HFFFFFF&\\shad0}CARBARN`,
    `Dialogue: 0,0:00:00.00,${end},Default,,0,0,0,,{\\an5\\pos(${cx},${yTitle})\\q2\\fn${fontMain}\\b1\\fs${fsHead}\\c&HFFFFFF&\\bord0}${title}`,
    `Dialogue: 0,0:00:00.00,${end},Default,,0,0,0,,{\\an5\\pos(${cx},${yAvail})\\fn${fontMain}\\fs${fsSub}\\c&HFFFFFF&\\bord0}is Available Now!`,
    `Dialogue: 0,0:00:00.00,${end},Default,,0,0,0,,{\\an5\\pos(${cx},${yPLab})\\fn${fontMain}\\fs${fsPriceLab}\\c&HFFFFFF&\\bord0}Price`,
    `Dialogue: 0,0:00:00.00,${end},Default,,0,0,0,,{\\an5\\pos(${cx},${yPVal})\\fn${fontMain}\\b1\\fs${fsPrice}\\c&HFFFFFF&\\bord0}${price}`,
    `Dialogue: 0,0:00:00.00,${end},Default,,0,0,0,,{\\an5\\pos(${cx},${yILab})\\fn${fontMain}\\fs${fsIncLab}\\c&HFFFFFF&\\bord0}Price Includes`,
    `Dialogue: 0,0:00:00.00,${end},Default,,0,0,0,,{\\an5\\pos(${cx},${yB1})\\fn${fontMain}\\b1\\fs${fsInc}\\c&HFFFFFF&\\bord0}${b1}`,
    `Dialogue: 0,0:00:00.00,${end},Default,,0,0,0,,{\\an5\\pos(${cx},${yB2})\\fn${fontMain}\\b1\\fs${fsInc}\\c&HFFFFFF&\\bord0}${b2}`,
    `Dialogue: 0,0:00:00.00,${end},Default,,0,0,0,,{\\an5\\pos(${cx},${yReady})\\fn${fontMain}\\fs${fsReadyLab}\\c&HFFFFFF&\\bord0}Ready for`,
    `Dialogue: 0,0:00:00.00,${end},Default,,0,0,0,,{\\an5\\pos(${cx},${yPick})\\fn${fontMain}\\fs${fsDel}\\c&HFFFFFF&\\bord0}Pickup/Door Delivery`,
    `Dialogue: 0,0:00:00.00,${end},Default,,0,0,0,,{\\an5\\pos(${cx},${yAus})\\fn${fontMain}\\b1\\fs${fsFoot}\\c&HFFFFFF&\\bord0}CARBARN AUSTRALIA`,
    `Dialogue: 0,0:00:00.00,${end},UrlPill,,0,0,0,,{\\an5\\pos(${cx},${yUrl})\\fn${fontMain}\\b0\\fs${fsUrl}}www.carbarn.com.au`,
  ];

  if (b3) {
    lines.splice(lines.length - 4, 0, `Dialogue: 0,0:00:00.00,${end},Default,,0,0,0,,{\\an5\\pos(${cx},${yB3})\\fn${fontMain}\\b1\\fs${fsInc}\\c&HFFFFFF&\\bord0}${b3}`);
  }

  return `\ufeff${lines.join("\n")}\n`;
}

function esc(s) {
  return String(s)
    .replace(/\\/gu, "\\\\")
    .replace(/\{/gu, "\\{")
    .replace(/\}/gu, "\\}")
    .replace(/\r?\n/gu, " ");
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/gu, "&amp;")
    .replace(/</gu, "&lt;")
    .replace(/>/gu, "&gt;")
    .replace(/"/gu, "&quot;");
}

function buildHeadlineHtml(lines) {
  return lines.map((line) => `<span class="headline__line">${escapeHtml(line)}</span>`).join("");
}

function buildHeadlineAss(lines) {
  return lines.map((line) => esc(line)).join("\\N");
}

function createHeadlineLayout(value) {
  const lines = splitHeadlineLines(value);
  return {
    lines,
    scale: computeHeadlineScale(lines),
  };
}

function splitHeadlineLines(value) {
  const normalized = String(value ?? "").trim();
  if (!normalized) {
    return ["Vehicle"];
  }

  const words = normalized.split(/\s+/u).filter(Boolean);
  if (words.length < 4) {
    return [normalized];
  }

  const candidates = [findBestHeadlineSplit(words, 2)];
  const twoLine = candidates[0];

  if (words.length >= 7 || twoLine.longestLength > 22) {
    candidates.push(findBestHeadlineSplit(words, 3));
  }
  candidates.sort((left, right) => left.score - right.score);
  const best = candidates[0]?.lines ?? [normalized];
  return best.slice(0, 3);
}

function findBestHeadlineSplit(words, lineCount) {
  const safeCount = Math.max(1, Math.min(lineCount, words.length));
  if (safeCount === 1) {
    const singleLine = words.join(" ");
    return {
      lines: [singleLine],
      longestLength: normalizedHeadlineLength(singleLine),
      score: Number.POSITIVE_INFINITY,
    };
  }

  let best = null;
  const current = [];

  const finalize = () => {
    const lines = [...current];
    const lengths = lines.map((line) => normalizedHeadlineLength(line));
    const longestLength = Math.max(...lengths);
    const shortestLength = Math.min(...lengths);
    const balancePenalty = longestLength - shortestLength;
    const lineCountPenalty = Math.max(0, lines.length - 2) * 12;
    const score = longestLength * 12 + balancePenalty * 2 + lineCountPenalty;
    if (!best || score < best.score) {
      best = {
        lines,
        longestLength,
        score,
      };
    }
  };

  const walk = (startIndex, linesRemaining) => {
    if (linesRemaining === 1) {
      const tail = words.slice(startIndex).join(" ");
      if (!tail) {
        return;
      }
      current.push(tail);
      finalize();
      current.pop();
      return;
    }

    const minBreak = startIndex + 1;
    const maxBreak = words.length - (linesRemaining - 1);
    for (let breakIndex = minBreak; breakIndex <= maxBreak; breakIndex += 1) {
      const line = words.slice(startIndex, breakIndex).join(" ");
      current.push(line);
      walk(breakIndex, linesRemaining - 1);
      current.pop();
    }
  };

  walk(0, safeCount);
  return best ?? findBestHeadlineSplit(words, 1);
}

function normalizedHeadlineLength(line) {
  return String(line ?? "")
    .replace(/\s+/gu, " ")
    .trim()
    .length;
}

function computeHeadlineScale(lines) {
  const longestLength = lines.reduce((max, line) => Math.max(max, normalizedHeadlineLength(line)), 0);
  const extraCharacters = Math.max(0, longestLength - 19);
  const linePenalty = Math.max(0, lines.length - 3) * 0.05;
  const scale = 1 - Math.min(0.3, extraCharacters * 0.016 + linePenalty);
  return Math.max(0.72, Number(scale.toFixed(3)));
}

async function getEndSceneSourceVersion() {
  const [sourceStats, templateStats] = await Promise.all([
    fs.stat(new URL(import.meta.url)),
    fs.stat(END_SCENE_TEMPLATE_FILE),
  ]);
  return [
    `${sourceStats.size}:${Math.trunc(sourceStats.mtimeMs)}`,
    `${templateStats.size}:${Math.trunc(templateStats.mtimeMs)}`,
  ].join("|");
}

function escapeFfmpegFilterValue(value) {
  return String(value)
    .replace(/\\/gu, "/")
    .replace(/:/gu, "\\:")
    .replace(/,/gu, "\\,");
}

function scaleTypography(sizePxAt1080, minDimension) {
  return Math.max(1, Math.round((sizePxAt1080 * minDimension) / 1080));
}

function formatAssTime(seconds) {
  const s = Math.max(0.05, seconds);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  const whole = Math.floor(sec);
  const cs = Math.round((sec - whole) * 100);
  return `${h}:${String(m).padStart(2, "0")}:${String(whole).padStart(2, "0")}.${String(cs).padStart(2, "0")}`;
}

async function renderEndSceneWebm({ ffmpegPath, cwd, assRelativePath, outRelativePath, durationSeconds, width, height }) {
  const panelX = Math.round(width * 0.082);
  const panelY = Math.round(height * 0.113);
  const panelWidth = Math.round(width * 0.836);
  const panelHeight = Math.round(height * 0.783);
  const draw = `drawbox=x=${panelX}:y=${panelY}:w=${panelWidth}:h=${panelHeight}:color=white@1:t=4`;
  const fontsDir = escapeFfmpegFilterValue(path.resolve(fileURLToPath(END_SCENE_FONT_DIR)));
  const vf = `${draw},ass=${assRelativePath}:fontsdir=${fontsDir},format=yuv420p`;
  const color = `color=c=0x1677F2:s=${width}x${height}:r=30`;
  const args = [
    "-y",
    "-f",
    "lavfi",
    "-i",
    color,
    "-vf",
    vf,
    "-t",
    String(durationSeconds),
    "-pix_fmt",
    "yuv420p",
  ];
  appendWebmEncodingArgs(args, { codec: "libvpx-vp9", crf: END_SCENE_VP9_CRF });
  args.push(outRelativePath);
  await runProcess(ffmpegPath, args, { cwd });
}

async function encodeFramesToWebm({ config, ffmpegPath, cwd, fps, durationSeconds, framesRelativePattern, outRelativePath, width, height }) {
  const args = [
    "-y",
    "-framerate",
    String(fps),
    "-start_number",
    "0",
    "-i",
    framesRelativePattern,
    "-t",
    String(durationSeconds),
    "-an",
    "-vf",
    `scale=${width}:${height}:flags=lanczos,format=yuv420p`,
  ];
  appendWebmEncodingArgs(args, {
    codec: String(config?.webmCodec || "libvpx-vp9"),
    deadline: String(config?.webmDeadline || "").trim(),
    cpuUsed: config?.webmCpuUsed,
    threads: config?.webmThreads,
    crf: Number.isFinite(Number(config?.webmCrf)) ? Number(config.webmCrf) : END_SCENE_VP9_CRF,
  });
  args.push(outRelativePath);
  await runProcess(ffmpegPath, args, { cwd });
}

async function concatVideosVertical({
  config,
  ffmpegPath,
  cwd,
  mainRelativePath,
  endRelativePath,
  mainDurationSeconds,
  endDurationSeconds,
  outRelativePath,
}) {
  const args = [
    "-y",
    "-t",
    String(mainDurationSeconds),
    "-i",
    mainRelativePath,
    "-t",
    String(endDurationSeconds),
    "-i",
    endRelativePath,
    "-filter_complex",
    "[0:v][1:v]concat=n=2:v=1:a=0[outv]",
    "-map",
    "[outv]",
    "-pix_fmt",
    "yuv420p",
  ];
  appendWebmEncodingArgs(args, {
    codec: String(config?.webmCodec || "libvpx-vp9"),
    deadline: String(config?.webmDeadline || "").trim(),
    cpuUsed: config?.webmCpuUsed,
    threads: config?.webmThreads,
    crf: Number.isFinite(Number(config?.webmCrf)) ? Number(config.webmCrf) : END_SCENE_VP9_CRF,
  });
  args.push(outRelativePath);
  await runProcess(ffmpegPath, args, { cwd });
}

function appendWebmEncodingArgs(args, profile = {}) {
  const codec = String(profile.codec || "libvpx-vp9").trim() || "libvpx-vp9";
  const deadline = String(profile.deadline || "").trim();
  const cpuUsed = Number(profile.cpuUsed);
  const threads = Number(profile.threads);
  const crf = Number(profile.crf);

  args.push("-c:v", codec);
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
    String(Number.isFinite(crf) ? crf : END_SCENE_VP9_CRF),
    "-b:v",
    "0",
  );
}

function runProcess(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd ?? process.cwd(),
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stderr = "";
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`ffmpeg failed (${code}): ${stderr}`.trim()));
    });
  });
}

async function readJsonIfExists(filePath) {
  try {
    const content = await fs.readFile(filePath, "utf8");
    return JSON.parse(content);
  } catch {
    return null;
  }
}

async function fileIfExists(filePath) {
  try {
    await fs.access(filePath);
    return filePath;
  } catch {
    return null;
  }
}

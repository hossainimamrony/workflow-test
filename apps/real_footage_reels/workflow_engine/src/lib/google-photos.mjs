import fs from "node:fs/promises";
import path from "node:path";

import { sanitizeSegment } from "./fs-utils.mjs";

const ALBUM_READY_TIMEOUT_MS = 30_000;
const VIDEO_READY_TIMEOUT_MS = 20_000;

export async function openAlbumPage(context, albumUrl) {
  const page = await context.newPage();
  await page.goto(albumUrl, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(4_000);
  await dismissOverlays(page);
  await page.waitForLoadState("networkidle", { timeout: ALBUM_READY_TIMEOUT_MS }).catch(() => {});
  return page;
}

export async function captureAlbumFrames(page, options) {
  await scrollAlbumToEnd(page, options.maxClips);

  const tileCount = await markVideoTiles(page);
  if (!tileCount) {
    await writeDiscoveryDebug(page, options.debugDir, "no-tiles");
    return [];
  }

  const clipCount = options.maxClips ? Math.min(tileCount, options.maxClips) : tileCount;
  const captured = [];

  for (let index = 0; index < clipCount; index += 1) {
    await page.bringToFront();
    await dismissOverlays(page);
    await markVideoTiles(page);

    const tileSelector = `[data-au-video-tile="${index}"]`;
    const tile = page.locator(tileSelector).first();

    if (!(await tile.count())) {
      continue;
    }

    await tile.scrollIntoViewIfNeeded().catch(() => {});
    await page.waitForTimeout(300);
    await tile.click({ delay: 60 }).catch(async () => {
      await tile.locator("img").first().click({ delay: 60 }).catch(() => {});
    });

    const viewer = await waitForVideoViewer(page);
    if (!viewer) {
      await writeDiscoveryDebug(page, options.debugDir, `viewer-not-found-${index + 1}`);
      await closeViewer(page);
      continue;
    }

    const viewerHandle = await viewer.elementHandle();
    if (!viewerHandle) {
      await writeDiscoveryDebug(page, options.debugDir, `viewer-handle-missing-${index + 1}`);
      await closeViewer(page);
      continue;
    }

    const metadata = await inspectViewerVideo(viewerHandle);
    if (!metadata) {
      await writeDiscoveryDebug(page, options.debugDir, `viewer-metadata-failed-${index + 1}`);
      await closeViewer(page);
      continue;
    }

    const clipId = [
      sanitizeSegment(metadata.title || "clip"),
      String(index + 1).padStart(3, "0"),
    ]
      .filter(Boolean)
      .join("-");

    const clipDir = path.join(options.framesDir, clipId);
    await fs.mkdir(clipDir, { recursive: true });

    const shotTimes = buildShotTimeline(metadata.durationSeconds, options.shotsPerClip);
    const framePaths = [];

    for (let shotIndex = 0; shotIndex < shotTimes.length; shotIndex += 1) {
      const timeSeconds = shotTimes[shotIndex];
      await seekViewerVideo(viewerHandle, timeSeconds);
      await page.waitForTimeout(180);
      const framePath = path.join(clipDir, `frame-${shotIndex + 1}.jpg`);
      await viewerHandle.screenshot({ path: framePath, type: "jpeg", quality: 88 });
      framePaths.push(framePath);
    }

    const sourceLocator = await viewerHandle.evaluate((video) => ({
      currentSrc: video.currentSrc || video.src || "",
      poster: video.poster || "",
    }));

    captured.push({
      clipId,
      albumUrl: options.albumUrl,
      tileIndex: index,
      title: metadata.title || clipId,
      durationSeconds: metadata.durationSeconds,
      framePaths,
      viewerSource: sourceLocator.currentSrc,
      posterSource: sourceLocator.poster,
    });

    await closeViewer(page);
  }

  return captured;
}

export async function downloadViewerClip(context, selection, downloadsDir) {
  const extension = guessVideoExtension(selection.viewerSource);
  const outputPath = path.join(downloadsDir, `${selection.role}-${sanitizeSegment(selection.clipId)}${extension}`);
  const page = await openAlbumPage(context, selection.albumUrl);

  try {
    await scrollAlbumToEnd(page, selection.tileIndex + 1);
    await markVideoTiles(page);
    const tile = page.locator(`[data-au-video-tile="${selection.tileIndex}"]`).first();
    await tile.scrollIntoViewIfNeeded().catch(() => {});
    await tile.click({ delay: 50 });
    const viewer = await waitForVideoViewer(page);
    if (!viewer) {
      throw new Error(`Could not reopen clip ${selection.clipId} for download.`);
    }

    const viewerHandle = await viewer.elementHandle();
    if (!viewerHandle) {
      throw new Error(`Could not resolve a stable video element for ${selection.clipId}.`);
    }

    const bytes = await extractVideoBytes(page, viewerHandle);
    await fs.writeFile(outputPath, Buffer.from(bytes));
    return outputPath;
  } finally {
    await page.close();
  }
}

async function dismissOverlays(page) {
  const labels = ["Got it", "Not now", "Accept all", "Accept", "OK"];
  for (const label of labels) {
    const button = page.getByRole("button", { name: new RegExp(`^${escapeForRegex(label)}$`, "i") });
    if (await button.count()) {
      await button.first().click().catch(() => {});
      await page.waitForTimeout(250);
    }
  }
}

async function scrollAlbumToEnd(page, maxClips) {
  let stableRounds = 0;
  let previousCount = -1;
  const maxRounds = maxClips ? 4 : 10;

  for (let round = 0; round < maxRounds; round += 1) {
    const count = await markVideoTiles(page);
    if (count === previousCount) {
      stableRounds += 1;
    } else {
      stableRounds = 0;
      previousCount = count;
    }

    if (stableRounds >= 2) {
      break;
    }

    await page.mouse.wheel(0, 2200);
    await page.waitForTimeout(1000);
  }

  await page.evaluate(() => window.scrollTo(0, 0));
  await page.waitForTimeout(400);
}

async function markVideoTiles(page) {
  return page.evaluate(() => {
    const all = Array.from(document.querySelectorAll("*"));
    for (const element of all) {
      element.removeAttribute("data-au-video-tile");
    }

    const anchors = Array.from(document.querySelectorAll('a[href*="/photo/"][aria-label*="Video"]')).filter(
      (element) => element instanceof HTMLElement,
    );

    if (anchors.length) {
      anchors
        .sort((left, right) => {
          const leftRect = left.getBoundingClientRect();
          const rightRect = right.getBoundingClientRect();
          return leftRect.top - rightRect.top || leftRect.left - rightRect.left;
        })
        .forEach((element, index) => {
          element.setAttribute("data-au-video-tile", String(index));
        });

      return anchors.length;
    }

    const candidates = all.filter((element) => {
      if (!(element instanceof HTMLElement)) {
        return false;
      }

      const text = element.innerText?.trim() ?? "";
      const rect = element.getBoundingClientRect();
      const isLargeEnough = rect.width >= 120 && rect.height >= 120;
      const isVisible = rect.width > 0 && rect.height > 0;
      const hasDuration = /\b\d{1,2}:\d{2}\b/u.test(text);
      const hasVisual =
        Boolean(element.querySelector("img, video")) ||
        /background-image:/iu.test(element.getAttribute("style") || "");
      const isClickable =
        element.tagName === "A" ||
        element.tagName === "BUTTON" ||
        element.getAttribute("role") === "button" ||
        element.tabIndex >= 0 ||
        Boolean(element.closest("a, button"));

      return hasDuration && hasVisual && isLargeEnough && isVisible && isClickable;
    });

    candidates
      .sort((left, right) => {
        const leftRect = left.getBoundingClientRect();
        const rightRect = right.getBoundingClientRect();
        return leftRect.top - rightRect.top || leftRect.left - rightRect.left;
      })
      .forEach((element, index) => {
        element.setAttribute("data-au-video-tile", String(index));
      });

    return candidates.length;
  });
}

async function waitForVideoViewer(page) {
  const candidateSelectors = [
    "video",
    "[role='dialog'] video",
    "div[aria-modal='true'] video",
  ];

  for (const selector of candidateSelectors) {
    const locator = page.locator(selector).last();
    try {
      await locator.waitFor({ state: "visible", timeout: VIDEO_READY_TIMEOUT_MS });
      return locator;
    } catch {
      continue;
    }
  }

  return null;
}

async function inspectViewerVideo(viewerHandle) {
  try {
    return await viewerHandle.evaluate(async (video) => {
      if (video.readyState < 1) {
        await new Promise((resolve, reject) => {
          const timeout = window.setTimeout(() => reject(new Error("metadata timeout")), 12_000);
          video.addEventListener(
            "loadedmetadata",
            () => {
              window.clearTimeout(timeout);
              resolve();
            },
            { once: true },
          );
        });
      }

      const titleSource =
        video.getAttribute("aria-label") ||
        video.closest("[role='dialog']")?.querySelector("h1, h2, [data-title]")?.textContent ||
        "";

      return {
        durationSeconds: Number.isFinite(video.duration) ? Number(video.duration) : 0,
        title: titleSource.trim(),
      };
    });
  } catch {
    return null;
  }
}

async function seekViewerVideo(viewerHandle, timeSeconds) {
  await viewerHandle.evaluate(
    async (video, nextTime) => {
      video.pause();
      if (!Number.isFinite(video.duration) || video.duration <= 0) {
        return;
      }
      const clampedTime = Math.max(0, Math.min(video.duration - 0.1, nextTime));
      if (Math.abs(video.currentTime - clampedTime) < 0.05) {
        return;
      }

      await new Promise((resolve, reject) => {
        const timeout = window.setTimeout(() => reject(new Error("seek timeout")), 8_000);
        const handler = () => {
          window.clearTimeout(timeout);
          resolve();
        };
        video.addEventListener("seeked", handler, { once: true });
        video.currentTime = clampedTime;
      });
    },
    timeSeconds,
  );
}

async function closeViewer(page) {
  await page.keyboard.press("Escape").catch(() => {});
  await page.waitForTimeout(350);
}

function buildShotTimeline(durationSeconds, shotsPerClip) {
  const safeDuration = Number.isFinite(durationSeconds) && durationSeconds > 0 ? durationSeconds : 4;
  const points = [];
  for (let index = 0; index < shotsPerClip; index += 1) {
    const ratio = (index + 1) / (shotsPerClip + 1);
    points.push(Math.max(0.15, Math.min(safeDuration - 0.15, safeDuration * ratio)));
  }
  return points;
}

function guessVideoExtension(source) {
  try {
    const url = new URL(source);
    const pathname = url.pathname.toLowerCase();
    if (pathname.endsWith(".webm")) {
      return ".webm";
    }
    if (pathname.endsWith(".mov")) {
      return ".mov";
    }
  } catch {
    return ".mp4";
  }

  return ".mp4";
}

async function extractVideoBytes(page, viewerHandle) {
  const currentSrc = await viewerHandle.evaluate((video) => video.currentSrc || video.src || "");
  if (!currentSrc) {
    throw new Error("Could not resolve a video source URL from the viewer.");
  }

  if (currentSrc.startsWith("blob:")) {
    const byteValues = await page.evaluate(async (blobUrl) => {
      const response = await fetch(blobUrl);
      const buffer = await response.arrayBuffer();
      return Array.from(new Uint8Array(buffer));
    }, currentSrc);
    return Uint8Array.from(byteValues);
  }

  try {
    const response = await page.context().request.get(currentSrc);
    const body = await response.body();
    return new Uint8Array(body);
  } catch {
    const byteValues = await page.evaluate(async (sourceUrl) => {
      const response = await fetch(sourceUrl, { credentials: "include" });
      const buffer = await response.arrayBuffer();
      return Array.from(new Uint8Array(buffer));
    }, currentSrc);
    return Uint8Array.from(byteValues);
  }
}

function escapeForRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}

async function writeDiscoveryDebug(page, debugDir, label) {
  if (!debugDir) {
    return;
  }

  const safeLabel = sanitizeSegment(label || "debug");
  const screenshotPath = path.join(debugDir, `debug-${safeLabel}.jpg`);
  const htmlPath = path.join(debugDir, `debug-${safeLabel}.html`);
  const summaryPath = path.join(debugDir, `debug-${safeLabel}.json`);

  await page.screenshot({ path: screenshotPath, fullPage: true }).catch(() => {});
  await fs.writeFile(htmlPath, await page.content(), "utf8").catch(() => {});

  const summary = await page.evaluate(() => {
    const candidates = Array.from(document.querySelectorAll("*"))
      .filter((element) => element instanceof HTMLElement)
      .map((element) => {
        const rect = element.getBoundingClientRect();
        return {
          tag: element.tagName,
          text: (element.innerText || "").trim().slice(0, 120),
          role: element.getAttribute("role") || "",
          href: element.getAttribute("href") || "",
          width: Math.round(rect.width),
          height: Math.round(rect.height),
        };
      })
      .filter((element) => /\b\d{1,2}:\d{2}\b/u.test(element.text))
      .slice(0, 30);

    return {
      title: document.title,
      url: window.location.href,
      bodyTextStart: (document.body?.innerText || "").slice(0, 3000),
      durationLikeElements: candidates,
    };
  });

  await fs.writeFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8").catch(() => {});
}

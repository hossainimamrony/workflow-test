import fs from "node:fs/promises";
import path from "node:path";

import { ensureDir, sanitizeSegment, writeJson } from "./fs-utils.mjs";

const GOOGLE_PHOTOS_BASE = "https://photos.google.com/";
const ALBUM_READY_TIMEOUT_MS = 30_000;
const VIDEO_DOWNLOAD_TIMEOUT_MS = 120_000;
const VIDEO_DOWNLOAD_MAX_ATTEMPTS = 3;

export async function downloadAlbumVideos(context, albumUrl, downloadsDir, options = {}) {
  await ensureDir(downloadsDir);

  const log = options.log ?? (() => {});
  const { resolvedAlbumUrl, albumHtml, albumEntries } = await inspectAlbumPage(context, albumUrl, options);
  const discoveredEntries = mergeAlbumEntries(
    albumEntries,
    extractAlbumVideoEntries(albumHtml, resolvedAlbumUrl),
  );
  const limitedEntries = options.maxClips
    ? discoveredEntries.slice(0, options.maxClips)
    : discoveredEntries;

  const downloaded = [];
  const failures = [];
  let skippedNonVideoItems = 0;
  const strictVideoDownloads = options.strictVideoDownloads !== false;

  log(`Album scan found ${limitedEntries.length} item(s).`);

  for (let index = 0; index < limitedEntries.length; index += 1) {
    const entry = limitedEntries[index];

    try {
      const photoHtml = await fetchText(context, entry.photoUrl);
      const directVideoUrl = extractDirectVideoUrl(photoHtml);

      if (!directVideoUrl) {
        skippedNonVideoItems += 1;
        continue;
      }

      const response = await downloadVideoWithRetries(context, directVideoUrl, entry.photoUrl);

      const extension = guessVideoExtension(
        response.headers()["content-type"] || "",
        directVideoUrl,
      );
      const labelSlug = sanitizeSegment(entry.ariaLabel.replace(/^video\s*-\s*/iu, ""));
      const clipId = `${String(downloaded.length + 1).padStart(3, "0")}-${sanitizeSegment(entry.mediaKey)}`;
      const fileName = `${clipId}-${labelSlug || "clip"}${extension}`;
      const filePath = path.join(downloadsDir, fileName);
      const body = await response.body();

      await fs.writeFile(filePath, body);

      downloaded.push({
        clipId,
        albumUrl,
        mediaKey: entry.mediaKey,
        photoUrl: entry.photoUrl,
        ariaLabel: entry.ariaLabel,
        directVideoUrl,
        videoPath: filePath,
        fileName,
        contentType: response.headers()["content-type"] || "",
        sizeBytes: body.length,
      });
    } catch (error) {
      failures.push({
        photoUrl: entry.photoUrl,
        message: error?.message ?? String(error),
      });
    }
  }

  log(
    `Album result: downloaded ${downloaded.length} video(s), skipped ${skippedNonVideoItems} non-video item(s)` +
      (failures.length ? `, ${failures.length} failed item(s).` : "."),
  );

  if (failures.length) {
    log(`Album warning: ${failures[0].message}`);
  }

  if (strictVideoDownloads && failures.length) {
    const sample = failures
      .slice(0, 3)
      .map((failure) => `${failure.photoUrl} -> ${failure.message}`)
      .join(" | ");
    throw new Error(
      `Failed to download all videos from album. ${failures.length} item(s) failed. ${sample}`,
    );
  }

  await writeJson(path.join(downloadsDir, "downloads-manifest.json"), {
    createdAt: new Date().toISOString(),
    albumUrl,
    resolvedAlbumUrl,
    summary: {
      discoveredItems: discoveredEntries.length,
      attemptedItems: limitedEntries.length,
      downloadedVideos: downloaded.length,
      skippedNonVideoItems,
      failedItems: failures.length,
    },
    failures,
    videos: downloaded,
  });

  return downloaded;
}

async function downloadVideoWithRetries(context, directVideoUrl, photoUrl) {
  let lastError = null;
  for (let attempt = 1; attempt <= VIDEO_DOWNLOAD_MAX_ATTEMPTS; attempt += 1) {
    try {
      const response = await context.request.get(directVideoUrl, {
        timeout: VIDEO_DOWNLOAD_TIMEOUT_MS,
      });
      if (!response.ok()) {
        throw new Error(
          `Video download failed for ${photoUrl} (${response.status()} ${response.statusText()})`,
        );
      }
      return response;
    } catch (error) {
      lastError = error;
      if (attempt < VIDEO_DOWNLOAD_MAX_ATTEMPTS) {
        await new Promise((resolve) => setTimeout(resolve, attempt * 1000));
      }
    }
  }
  throw new Error(lastError?.message ?? `Video download failed for ${photoUrl}`);
}

export function extractAlbumVideoEntries(html, albumUrl) {
  const entries = [];
  const seen = new Set();
  const patterns = [
    /<a[^>]+href="([^"]*\/photo\/[^"]+)"[^>]+aria-label="([^"]*)"/giu,
    /<a[^>]+aria-label="([^"]*)"[^>]+href="([^"]*\/photo\/[^"]+)"/giu,
  ];

  for (const pattern of patterns) {
    let match;
    while ((match = pattern.exec(html)) !== null) {
      const rawHref = decodeHtml(pattern === patterns[0] ? match[1] : match[2]);
      const ariaLabel = decodeHtml(pattern === patterns[0] ? match[2] : match[1]);
      const photoUrl = new URL(rawHref, GOOGLE_PHOTOS_BASE).href;
      const mediaKey = extractMediaKey(photoUrl);

      if (!mediaKey || seen.has(mediaKey)) {
        continue;
      }

      seen.add(mediaKey);
      entries.push({
        albumUrl,
        photoUrl,
        mediaKey,
        ariaLabel,
      });
    }
  }

  return entries;
}

export function extractDirectVideoUrl(html) {
  const match = html.match(/https:\/\/video-downloads\.googleusercontent\.com[^"'\\\s<]+/u);
  if (!match) {
    return null;
  }

  return decodeJavascriptEscapes(decodeHtml(match[0]));
}

async function fetchText(context, url) {
  const response = await context.request.get(url);
  if (!response.ok()) {
    throw new Error(`Request failed for ${url} (${response.status()} ${response.statusText()})`);
  }
  return response.text();
}

async function inspectAlbumPage(context, albumUrl, options = {}) {
  const page = await context.newPage();

  try {
    await page.goto(albumUrl, { waitUntil: "domcontentloaded" });
    await page.waitForTimeout(4_000);
    await dismissOverlays(page);
    await page.waitForLoadState("networkidle", { timeout: ALBUM_READY_TIMEOUT_MS }).catch(() => {});
    await scrollAlbumToLoadItems(page, options.maxClips);

    return {
      resolvedAlbumUrl: page.url(),
      albumHtml: await page.content(),
      albumEntries: await collectAlbumEntries(page, options.maxClips),
    };
  } finally {
    await page.close();
  }
}

async function collectAlbumEntries(page, maxClips) {
  return page.evaluate((limit) => {
    const anchors = Array.from(document.querySelectorAll('a[href*="/photo/"]'))
      .filter((element) => element instanceof HTMLElement)
      .sort((left, right) => {
        const leftRect = left.getBoundingClientRect();
        const rightRect = right.getBoundingClientRect();
        return leftRect.top - rightRect.top || leftRect.left - rightRect.left;
      });

    const seen = new Set();
    const entries = [];

    for (const anchor of anchors) {
      const href = anchor.getAttribute("href") || anchor.href || "";
      const photoUrl = new URL(href, "https://photos.google.com/").href;
      const mediaKeyMatch = /\/photo\/([^?&#/]+)/u.exec(photoUrl);
      const mediaKey = mediaKeyMatch?.[1] ?? "";

      if (!mediaKey || seen.has(mediaKey)) {
        continue;
      }

      seen.add(mediaKey);
      entries.push({
        albumUrl: window.location.href,
        photoUrl,
        mediaKey,
        ariaLabel: anchor.getAttribute("aria-label") || anchor.getAttribute("title") || "",
      });

      if (limit && entries.length >= limit) {
        break;
      }
    }

    return entries;
  }, maxClips ?? null);
}

async function scrollAlbumToLoadItems(page, maxClips) {
  let stableRounds = 0;
  let previousCount = -1;
  const maxRounds = maxClips ? 4 : 10;

  for (let round = 0; round < maxRounds; round += 1) {
    const count = await page.locator('a[href*="/photo/"]').count();
    if (count === previousCount) {
      stableRounds += 1;
    } else {
      stableRounds = 0;
      previousCount = count;
    }

    if ((maxClips && count >= maxClips) || stableRounds >= 2) {
      break;
    }

    await page.mouse.wheel(0, 2200);
    await page.waitForTimeout(1000);
  }

  await page.evaluate(() => window.scrollTo(0, 0)).catch(() => {});
  await page.waitForTimeout(400);
}

function mergeAlbumEntries(...entryLists) {
  const merged = [];
  const seen = new Set();

  for (const entryList of entryLists) {
    for (const entry of entryList ?? []) {
      if (!entry?.mediaKey || seen.has(entry.mediaKey)) {
        continue;
      }

      seen.add(entry.mediaKey);
      merged.push(entry);
    }
  }

  return merged;
}

function extractMediaKey(photoUrl) {
  return /\/photo\/([^?&#/]+)/u.exec(photoUrl)?.[1] ?? "";
}

function guessVideoExtension(contentType, url) {
  const type = contentType.toLowerCase();
  if (type.includes("webm")) {
    return ".webm";
  }
  if (type.includes("quicktime")) {
    return ".mov";
  }

  try {
    const pathname = new URL(url).pathname.toLowerCase();
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

function decodeHtml(value) {
  return value
    .replaceAll("&amp;", "&")
    .replaceAll("&quot;", "\"")
    .replaceAll("&#39;", "'")
    .replaceAll("&lt;", "<")
    .replaceAll("&gt;", ">");
}

function decodeJavascriptEscapes(value) {
  return value
    .replaceAll("\\u003d", "=")
    .replaceAll("\\u0026", "&")
    .replaceAll("\\/", "/");
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

function escapeForRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}

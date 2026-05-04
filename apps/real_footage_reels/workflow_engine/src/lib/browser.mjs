import fs from "node:fs/promises";
import path from "node:path";

import { chromium } from "playwright-core";

const KNOWN_BROWSER_PATHS = [
  "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe",
  "C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe",
  "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
  "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
];

export async function launchWorkflowBrowser(config) {
  const executablePath = await resolveBrowserPath(config.browserPath);
  const userDataDir = path.resolve(config.browserProfileDir);

  await fs.mkdir(userDataDir, { recursive: true });

  const context = await chromium.launchPersistentContext(userDataDir, {
    headless: config.headless,
    executablePath,
    viewport: { width: 1440, height: 1100 },
    args: [
      "--disable-blink-features=AutomationControlled",
      "--autoplay-policy=no-user-gesture-required",
    ],
  });

  if (!context.pages().length) {
    await context.newPage();
  }

  return {
    context,
    browserType: chromium,
    close: async () => {
      await context.close();
    },
  };
}

export async function launchRenderBrowser(config) {
  const executablePath = await resolveBrowserPath(config.browserPath);
  const browser = await chromium.launch({
    headless: true,
    executablePath,
    args: [
      "--disable-blink-features=AutomationControlled",
      "--autoplay-policy=no-user-gesture-required",
    ],
  });
  const context = await browser.newContext();

  return {
    context,
    browserType: chromium,
    close: async () => {
      await context.close().catch(() => {});
      await browser.close().catch(() => {});
    },
  };
}

export async function resolveBrowserPath(preferredPath) {
  const candidates = preferredPath ? [preferredPath, ...KNOWN_BROWSER_PATHS] : KNOWN_BROWSER_PATHS;
  for (const candidate of candidates) {
    if (!candidate) {
      continue;
    }

    try {
      await fs.access(candidate);
      return candidate;
    } catch {
      continue;
    }
  }

  throw new Error(
    "Could not locate a Chromium-based browser. Pass --browser <path> to an installed Edge or Chrome executable.",
  );
}

import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";

import { chromium } from "playwright-core";

const KNOWN_BROWSER_PATHS = [
  // Linux / PythonAnywhere common locations.
  "/usr/bin/chromium",
  "/usr/bin/chromium-browser",
  "/usr/bin/google-chrome",
  "/usr/bin/google-chrome-stable",
  "/snap/bin/chromium",
  "/usr/lib/chromium-browser/chromium-browser",
  "/usr/lib/chromium/chromium",
  // Playwright-managed cache locations (Linux).
  "/home/carbarnau/.cache/ms-playwright/chromium-*/chrome-linux/chrome",
  "/home/carbarnau/.cache/ms-playwright/chromium_headless_shell-*/chrome-linux/headless_shell",
  // Windows locations.
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
  const envCandidates = [
    process.env.BROWSER_PATH,
    process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE,
    process.env.CHROME_BIN,
    process.env.GOOGLE_CHROME_BIN,
  ].filter(Boolean);

  const candidates = preferredPath
    ? [preferredPath, ...envCandidates, ...KNOWN_BROWSER_PATHS]
    : [...envCandidates, ...KNOWN_BROWSER_PATHS];

  for (const candidate of candidates) {
    if (!candidate) {
      continue;
    }
    if (candidate.includes("*")) {
      const resolved = await resolveGlobCandidate(candidate);
      if (resolved) {
        return resolved;
      }
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
    "Could not locate a Chromium-based browser. Set BROWSER_PATH (or PLAYWRIGHT_CHROMIUM_EXECUTABLE) to your Chromium/Chrome executable path.",
  );
}

async function resolveGlobCandidate(pattern) {
  const normalized = String(pattern || "").trim();
  if (!normalized || !normalized.includes("*")) {
    return null;
  }

  const marker = normalized.indexOf("*");
  const slash = normalized.lastIndexOf("/", marker);
  if (slash <= 0) {
    return null;
  }
  const baseDir = normalized.slice(0, slash);
  const rest = normalized.slice(slash + 1);

  let entries = [];
  try {
    entries = await fs.readdir(baseDir, { withFileTypes: true });
  } catch {
    return null;
  }

  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const name = entry.name;
    const [prefix, suffix] = rest.split("*");
    if (!name.startsWith(prefix || "")) continue;
    if (suffix && !name.endsWith(suffix.split("/")[0] || "")) continue;
    const remainder = rest.replace("*", name);
    const fullPath = path.join(baseDir, remainder);
    try {
      await fs.access(fullPath);
      return fullPath;
    } catch {
      continue;
    }
  }

  return null;
}

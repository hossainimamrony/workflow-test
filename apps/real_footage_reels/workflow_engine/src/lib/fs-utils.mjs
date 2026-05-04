import fs from "node:fs/promises";
import path from "node:path";

export async function ensureDir(dirPath) {
  await fs.mkdir(dirPath, { recursive: true });
}

export function makeRunDirectory(cwd) {
  const stamp = new Date().toISOString().replaceAll(":", "-").replace(/\..+$/u, "");
  return path.join(cwd, "runs", stamp);
}

export async function writeJson(filePath, value) {
  await ensureDir(path.dirname(filePath));
  await fs.writeFile(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

export function sanitizeSegment(value) {
  return String(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, "-")
    .replace(/^-+|-+$/gu, "")
    .slice(0, 80);
}

export async function fileToBase64(filePath) {
  const buffer = await fs.readFile(filePath);
  return buffer.toString("base64");
}

export function printJson(value) {
  console.log(JSON.stringify(value, null, 2));
}

import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";

import { build } from "esbuild";

const rootDir = process.cwd();
const outDir = path.join(rootDir, "ui", "dist");

await fs.mkdir(outDir, { recursive: true });

await build({
  entryPoints: [path.join(rootDir, "ui", "src", "main.jsx")],
  bundle: true,
  outfile: path.join(outDir, "app.js"),
  format: "esm",
  jsx: "automatic",
  target: ["es2020"],
  sourcemap: false,
  minify: false,
  legalComments: "none",
  loader: {
    ".png": "dataurl",
    ".svg": "dataurl",
  },
});

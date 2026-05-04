import process from "node:process";

import { loadConfigFromCli } from "./lib/config.mjs";
import { executeWorkflow } from "./lib/workflow.mjs";

async function main() {
  const config = loadConfigFromCli(process.argv.slice(2));

  if (!["run", "prepare", "download", "help"].includes(config.command)) {
    printHelp();
    process.exitCode = 1;
    return;
  }

  if (config.command === "help") {
    printHelp();
    return;
  }

  await executeWorkflow(config, {
    log: (message) => {
      console.log(message);
    },
  });
}

function printHelp() {
  console.log(`Usage:
  node src/cli.mjs <run|prepare|download> --url "<google-photos-share-url>" [options]

Options:
  --url <value>           Shared Google Photos album URL. Repeatable.
  --out <path>            Output directory for this run.
  --max-clips <number>    Limit clips processed per album.
  --compose               Compose the selected local clips into a final WebM reel.
  --headful               Launch a visible browser window.
  --browser <path>        Explicit browser executable path.
  --python <path>         Explicit Python executable path.
  --ffmpeg <path>         Explicit ffmpeg executable path.
  --width <number>        Reel width. Default: 1080.
  --height <number>       Reel height. Default: 1920.
  --clip-start <seconds>  Skip this many seconds at the start of each source clip. Default: 2, capped at 2.
  --model <name>          Gemini model. Default: gemini-2.5-pro
  --help                  Show this help text.
`);
}

main().catch((error) => {
  console.error(error?.stack ?? String(error));
  process.exitCode = 1;
});

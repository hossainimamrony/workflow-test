# AU Real Footage Reels

This project builds short car reels from shared Google Photos album URLs.

Current workflow:

1. Read a shared Google Photos album URL.
2. Extract every video item page from the shared album.
3. Open each item page and resolve its direct downloadable video URL.
4. Download all raw videos locally.
5. Run a Python script that uses `ffmpeg` to extract several sampled frames from each downloaded video.
6. Send those sampled frame sets to the configured classification model for structured visual analysis.
7. Classify each clip with a precise view label such as `front_left_exterior`, `rear_left_exterior`, or `driver_door_interior_reveal`.
8. Enforce the locked Carbarn reel pattern:
   - front exterior
   - driver door opening / interior reveal
   - interior / odometer
   - backside exterior
   - end scene
9. Save a reel plan as JSON.
10. Optionally compose the selected local clips into a 9:16 WebM reel.
11. Save voice-over script drafts for approval before any ElevenLabs TTS audio is generated.

## Why this design

- It works with the tools already available on this machine.
- It downloads the raw videos first, which matches your editing workflow better.
- It keeps Google Photos link resolution isolated from frame extraction and classification.
- It uses Python for sampled-frame extraction, as requested.

## Setup

1. Install dependencies:

```powershell
npm.cmd install
```

2. Copy `.env.example` to `.env` and add `GEMINI_API_KEY`.

3. Start the UI:

```powershell
npm.cmd run ui
```

Then open `http://127.0.0.1:4173`.

### Expose over internet (Cloudflare Tunnel)

1. Install `cloudflared` (one time, Windows):

```powershell
winget install --id Cloudflare.cloudflared
```

2. Keep UI server running:

```powershell
npm.cmd run ui
```

3. In a second terminal, start tunnel:

```powershell
npm.cmd run tunnel
```

`cloudflared` will print a public `https://...trycloudflare.com` URL you can share.

The UI is intentionally server-driven:

- no API key or model fields are shown in the browser
- classification mode is enabled only when `.env` is configured
- secrets stay on the machine and never appear in the frontend

4. Download and frame-extract without classification:

```powershell
node src/cli.mjs prepare --url "https://photos.app.goo.gl/your-share-url"
```

5. Run the full pipeline with classification enabled:

```powershell
node src/cli.mjs run --url "https://photos.app.goo.gl/your-share-url"
```

Recommended for the first run:

- Use `--headful` so you can see Google Photos open.
- If Google asks you to sign in or confirm access, the persistent browser profile keeps that session.

## Useful options

```powershell
node src/cli.mjs prepare ^
  --url "https://photos.app.goo.gl/your-share-url" ^
  --out ".\\runs\\job-001"
```

## Output

Each run creates:

- `downloads/`: downloaded source videos
- `frames/`: sampled JPEG frames generated from the local videos
- `downloads-manifest.json`: all downloaded clip metadata
- `frames-manifest.json`: all frame paths mapped back to the downloaded videos
- `analysis.json`: classification results
- `reel-plan.json`: chosen front, driver-door reveal, interior, and rear clips plus locked rulebook metadata
- `final-reel.webm`: composed output when `--compose` is enabled

## Reel defaults

- Final reels render at `1080x1920` (9:16).
- Source clips start from `2s` before frame sampling and final composition, and the pipeline does not allow a later default trim than that.
- The reel order is hard-locked in `src/lib/reel-rules.mjs`: `front_exterior -> driver_door_interior_reveal -> interior -> rear_exterior -> end_scene`.
- Voice-over always requires script approval first. The silent reel and script drafts are created, then TTS runs only after you approve or edit a script in Runs.

## Notes

- Google Photos link structure can change. The downloader currently resolves direct video URLs from the item page HTML.
- The browser-based composer currently outputs WebM because that is the most reliable format from `MediaRecorder`.

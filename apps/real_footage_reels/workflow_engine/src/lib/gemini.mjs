import { fileToBase64 } from "./fs-utils.mjs";
import {
  INTERIOR_GUIDANCE,
  LOCKED_REEL_AI_INSTRUCTION,
  renderLockedReelOrder,
} from "./reel-rules.mjs";

const PRIMARY_LABELS = new Set([
  "front_left_exterior",
  "front_center_exterior",
  "front_right_exterior",
  "rear_left_exterior",
  "rear_center_exterior",
  "rear_right_exterior",
  "side_left_exterior",
  "side_right_exterior",
  "front_exterior",
  "rear_exterior",
  "side_exterior",
  "driver_door_interior_reveal",
  "interior",
  "engine_bay",
  "wheel",
  "other",
]);

const ROLE_LABELS = new Set([
  "front_exterior",
  "driver_door_interior_reveal",
  "rear_exterior",
  "side_exterior",
  "interior",
  "engine_bay",
  "wheel",
  "other",
]);

const ANGLE_SCORE_KEYS = [
  "front_left_exterior",
  "front_center_exterior",
  "front_right_exterior",
  "rear_left_exterior",
  "rear_center_exterior",
  "rear_right_exterior",
  "side_left_exterior",
  "side_right_exterior",
];

const LOCKED_MONTAGE_ORDER = renderLockedReelOrder({ includeEndScene: false });

const DEFAULT_PROMPT = `You are classifying one or more frame images from short raw car footage for a vehicle reel editor.

The provided frames are ordered from earlier in the clip to later in the clip.
Classify the clip as a whole, not just the first image.
If a clearer angle appears in the middle or late frames, prefer that over an ambiguous opening frame.

${LOCKED_REEL_AI_INSTRUCTION}

Locked montage order before the end scene:
1. front_exterior
2. driver_door_interior_reveal
3. interior
4. rear_exterior

The branded end scene is appended later and is not chosen from the raw clips.
Only score these three reel-role keys: front_exterior, driver_door_interior_reveal, and rear_exterior.
Interior placement is enforced in code from the clip label and interior visibility signals.

Return strict JSON with this shape:
{
  "primaryLabel": "front_left_exterior | front_center_exterior | front_right_exterior | rear_left_exterior | rear_center_exterior | rear_right_exterior | side_left_exterior | side_right_exterior | driver_door_interior_reveal | interior | engine_bay | wheel | other",
  "roleLabel": "front_exterior | driver_door_interior_reveal | rear_exterior | side_exterior | interior | engine_bay | wheel | other",
  "secondaryLabels": ["string"],
  "confidence": 0,
  "reason": "short explanation",
  "scores": {
    "front_exterior": 0,
    "driver_door_interior_reveal": 0,
    "rear_exterior": 0
  },
  "angleScores": {
    "front_left_exterior": 0,
    "front_center_exterior": 0,
    "front_right_exterior": 0,
    "rear_left_exterior": 0,
    "rear_center_exterior": 0,
    "rear_right_exterior": 0,
    "side_left_exterior": 0,
    "side_right_exterior": 0
  },
  "vehicleSide": "left | right | center | unknown",
  "doorOpen": false,
  "interiorVisible": false,
  "rearVisible": false,
  "frontVisible": false
}

Scoring rules:
- Use 0 to 100 integers.
- Give the highest score to the most suitable reel role for these screenshots.
- If only a single frame image is provided, classify from that one frame only.
- Use the vehicle's own left/right side, not the viewer's left/right side.
- Be precise with exterior angles. Examples:
  - front-left corner view -> "front_left_exterior"
  - rear-left corner view -> "rear_left_exterior"
  - full left side profile -> "side_left_exterior"
- If the rear of the vehicle is visible together with the left side, prefer "rear_left_exterior" over a generic side label.
- If the front of the vehicle is visible together with the left side, prefer "front_left_exterior" over a generic side label.
- Do not use "other" for a normal exterior car shot. Use the closest precise exterior label instead.
- "driver_door_interior_reveal" should be high only when the driver-side door is opening or open and the interior is visible.
- ${INTERIOR_GUIDANCE}
- The reel order is fixed in code as ${renderLockedReelOrder()}. Do not suggest another order.
- Keep reasoning short and factual.`;

const RETRY_PROMPT = `You are retrying a vehicle shot classification because the first answer was too weak.

Choose the single best exact shot label for this car frame.
The provided frames are ordered from earlier in the clip to later in the clip.
Judge the full clip using all frames together.

${LOCKED_REEL_AI_INSTRUCTION}
Locked montage order before the end scene: ${LOCKED_MONTAGE_ORDER}.
The branded end scene is appended after rear_exterior.

Return strict JSON with the same shape as before:
{
  "primaryLabel": "front_left_exterior | front_center_exterior | front_right_exterior | rear_left_exterior | rear_center_exterior | rear_right_exterior | side_left_exterior | side_right_exterior | driver_door_interior_reveal | interior | engine_bay | wheel | other",
  "roleLabel": "front_exterior | driver_door_interior_reveal | rear_exterior | side_exterior | interior | engine_bay | wheel | other",
  "secondaryLabels": ["string"],
  "confidence": 0,
  "reason": "short explanation",
  "scores": {
    "front_exterior": 0,
    "driver_door_interior_reveal": 0,
    "rear_exterior": 0
  },
  "angleScores": {
    "front_left_exterior": 0,
    "front_center_exterior": 0,
    "front_right_exterior": 0,
    "rear_left_exterior": 0,
    "rear_center_exterior": 0,
    "rear_right_exterior": 0,
    "side_left_exterior": 0,
    "side_right_exterior": 0
  },
  "vehicleSide": "left | right | center | unknown",
  "doorOpen": false,
  "interiorVisible": false,
  "rearVisible": false,
  "frontVisible": false
}

Rules:
- Use the vehicle's own left/right side.
- If the front and the left side are both visible, prefer "front_left_exterior".
- If the rear and the left side are both visible, prefer "rear_left_exterior".
- Use "side_left_exterior" or "side_right_exterior" only when the shot is mainly a side profile or side door view.
- Use "other" only if the car is not visible enough to classify.
- ${INTERIOR_GUIDANCE}
- Keep reasoning short and factual.`;

export async function classifyClipFrames(config, clip) {
  const imageParts = [];
  for (let index = 0; index < clip.framePaths.length; index += 1) {
    const framePath = clip.framePaths[index];
    imageParts.push({
      text: frameCaption(index, clip.framePaths.length),
    });
    imageParts.push({
      inline_data: {
        mime_type: "image/jpeg",
        data: await fileToBase64(framePath),
      },
    });
  }
  let classification = normalizeClassification(
    await requestGeminiClassification(config, clip, imageParts, DEFAULT_PROMPT),
  );

  if (shouldRetryClassification(classification)) {
    const retried = normalizeClassification(
      await requestGeminiClassification(config, clip, imageParts, RETRY_PROMPT),
    );
    if (classificationStrength(retried) > classificationStrength(classification)) {
      classification = retried;
    }
  }

  return classification;
}

async function requestGeminiClassification(config, clip, imageParts, prompt) {
  const parts = [
    ...imageParts,
    {
      text: [
        prompt,
        "",
        `Clip id: ${clip.clipId}`,
        `Duration seconds: ${clip.durationSeconds}`,
      ].join("\n"),
    },
  ];

  const response = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/${config.geminiModel}:generateContent`,
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-goog-api-key": config.geminiApiKey,
      },
      body: JSON.stringify({
        contents: [
          {
            role: "user",
            parts,
          },
        ],
        generationConfig: {
          temperature: 0,
          responseMimeType: "application/json",
        },
      }),
    },
  );

  if (!response.ok) {
    const details = await response.text();
    throw new Error(`Gemini request failed (${response.status}): ${details}`);
  }

  const payload = await response.json();
  const rawText = payload?.candidates?.[0]?.content?.parts?.map((part) => part.text).filter(Boolean).join("\n");
  if (!rawText) {
    throw new Error(`Gemini returned no text for clip ${clip.clipId}.`);
  }

  return parseLooseJson(rawText);
}

function parseLooseJson(rawText) {
  try {
    return JSON.parse(rawText);
  } catch {
    const firstBrace = rawText.indexOf("{");
    const lastBrace = rawText.lastIndexOf("}");
    if (firstBrace >= 0 && lastBrace > firstBrace) {
      return JSON.parse(rawText.slice(firstBrace, lastBrace + 1));
    }
    throw new Error(`Could not parse Gemini JSON response: ${rawText}`);
  }
}

function normalizeClassification(result) {
  const primaryLabel = normalizePrimaryLabel(stringValue(result.primaryLabel, "other"));
  const reportedRoleLabel = normalizeRoleLabel(stringValue(result.roleLabel, ""));
  const derivedRoleLabel = deriveRoleLabel(primaryLabel);
  const roleLabel =
    shouldUseReportedRoleLabel(reportedRoleLabel, result?.scores)
      ? reportedRoleLabel
      : derivedRoleLabel;
  const angleScores = normalizeAngleScores(result?.angleScores);
  const confidence = normalizePercent(result.confidence);
  const doorOpen = Boolean(result.doorOpen);
  const interiorVisible = Boolean(result.interiorVisible);

  return {
    primaryLabel,
    roleLabel,
    secondaryLabels: Array.isArray(result.secondaryLabels)
      ? result.secondaryLabels.map((value) => String(value))
      : [],
    confidence,
    reason: stringValue(result.reason, ""),
    scores: normalizeRoleScores(result?.scores, angleScores, {
      roleLabel,
      primaryLabel,
      confidence,
      doorOpen,
      interiorVisible,
    }),
    angleScores,
    vehicleSide: normalizeVehicleSide(result.vehicleSide, primaryLabel),
    doorOpen,
    interiorVisible,
    rearVisible: Boolean(result.rearVisible),
    frontVisible: Boolean(result.frontVisible),
  };
}

function shouldRetryClassification(classification) {
  return classification.primaryLabel === "other" || strongestRoleScore(classification) === 0;
}

function classificationStrength(classification) {
  return (
    strongestRoleScore(classification) +
    classification.confidence +
    (classification.primaryLabel !== "other" ? 15 : 0)
  );
}

function strongestRoleScore(classification) {
  return Math.max(
    classification.scores.front_exterior,
    classification.scores.driver_door_interior_reveal,
    classification.scores.rear_exterior,
  );
}

function normalizeRoleScores(rawScores, angleScores, options) {
  const frontExterior = Math.max(
    normalizePercent(rawScores?.front_exterior),
    angleScores.front_left_exterior,
    angleScores.front_center_exterior,
    angleScores.front_right_exterior,
  );
  const rearExterior = Math.max(
    normalizePercent(rawScores?.rear_exterior),
    angleScores.rear_left_exterior,
    angleScores.rear_center_exterior,
    angleScores.rear_right_exterior,
  );

  let driverDoorInteriorReveal = normalizePercent(rawScores?.driver_door_interior_reveal);
  if (
    !driverDoorInteriorReveal &&
    options.roleLabel === "driver_door_interior_reveal" &&
    (options.doorOpen || options.interiorVisible || options.primaryLabel === "driver_door_interior_reveal")
  ) {
    driverDoorInteriorReveal = Math.max(70, options.confidence);
  }

  return {
    front_exterior: frontExterior,
    driver_door_interior_reveal: driverDoorInteriorReveal,
    rear_exterior: rearExterior,
  };
}

function normalizeAngleScores(rawScores) {
  return Object.fromEntries(
    ANGLE_SCORE_KEYS.map((key) => [key, normalizePercent(rawScores?.[key])]),
  );
}

function normalizePrimaryLabel(value) {
  const canonical = canonicalizeLabel(value);
  return PRIMARY_LABELS.has(canonical) ? canonical : "other";
}

function normalizeRoleLabel(value) {
  const canonical = canonicalizeLabel(value);
  return ROLE_LABELS.has(canonical) ? canonical : "other";
}

function deriveRoleLabel(primaryLabel) {
  if (primaryLabel.startsWith("front_") || primaryLabel === "front_exterior") {
    return "front_exterior";
  }

  if (primaryLabel.startsWith("rear_") || primaryLabel === "rear_exterior") {
    return "rear_exterior";
  }

  if (primaryLabel.startsWith("side_") || primaryLabel === "side_exterior") {
    return "side_exterior";
  }

  if (ROLE_LABELS.has(primaryLabel)) {
    return primaryLabel;
  }

  return "other";
}

function shouldUseReportedRoleLabel(reportedRoleLabel, rawScores) {
  if (!reportedRoleLabel || reportedRoleLabel === "other") {
    return false;
  }

  if (reportedRoleLabel === "front_exterior") {
    return normalizePercent(rawScores?.front_exterior) > 0;
  }

  if (reportedRoleLabel === "driver_door_interior_reveal") {
    return normalizePercent(rawScores?.driver_door_interior_reveal) > 0;
  }

  if (reportedRoleLabel === "rear_exterior") {
    return normalizePercent(rawScores?.rear_exterior) > 0;
  }

  return true;
}

function frameCaption(index, total) {
  if (total <= 1) {
    return "Clip frame: single available view";
  }

  if (index === 0) {
    return "Clip frame 1: early in the clip";
  }

  if (index === total - 1) {
    return `Clip frame ${index + 1}: late in the clip`;
  }

  return `Clip frame ${index + 1}: middle of the clip`;
}

function normalizeVehicleSide(value, primaryLabel) {
  const normalized = canonicalizeLabel(value);
  if (normalized === "left" || normalized === "right" || normalized === "center") {
    return normalized;
  }

  if (primaryLabel.includes("_left_")) {
    return "left";
  }

  if (primaryLabel.includes("_right_")) {
    return "right";
  }

  if (primaryLabel.includes("_center_")) {
    return "center";
  }

  return "unknown";
}

function canonicalizeLabel(value) {
  const normalized = String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/gu, "_")
    .replace(/_+/gu, "_");

  switch (normalized) {
    case "left_front_exterior":
      return "front_left_exterior";
    case "right_front_exterior":
      return "front_right_exterior";
    case "left_rear_exterior":
      return "rear_left_exterior";
    case "right_rear_exterior":
      return "rear_right_exterior";
    case "left_side_exterior":
      return "side_left_exterior";
    case "right_side_exterior":
      return "side_right_exterior";
    default:
      return normalized;
  }
}

function normalizePercent(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return 0;
  }
  return Math.max(0, Math.min(100, Math.round(numeric)));
}

function stringValue(value, fallback) {
  return typeof value === "string" && value.trim() ? value.trim() : fallback;
}

export async function generateVoiceoverScript(
  config,
  {
    listingTitle,
    stockId,
    carDescription,
    listingPrice = "",
    targetSeconds = 17,
    mainMontageSeconds = 14,
    endSceneSeconds = 3,
  },
) {
  const sec = Math.max(8, Math.min(17, Number(targetSeconds) || 17));
  const mainS = Math.max(1, Number(mainMontageSeconds) || 14);
  const endS = Math.max(1, Number(endSceneSeconds) || 3);
  // Short scripts (~1.1–1.5 words/sec) so ElevenLabs audio fits in real time without trimming.
  const wordLo = Math.max(14, Math.round(sec * 1.05));
  const wordHi = Math.min(26, Math.round(sec * 1.45));
  const hardMaxWords = 28;
  const enforcedListingPrice = sanitizeListingPrice(listingPrice);
  const enforcedScriptPrice = normalizeListingPriceForScript(enforcedListingPrice);
  const prompt = `You write Australian used-car dealer voice-over scripts for vertical social reels.

The narration is heard across the **entire** video (${sec} seconds total): roughly the first ${mainS} seconds play over moving footage, and the voice **continues through** the final ${endS} seconds while a static Carbarn-branded end card is on screen. Write **one continuous** script—do not imply the voice stops when the end card appears, and do not say "end card", "graphic", or "screen".

Output strict JSON only with this exact shape:
{"script":"your script as one line here"}

Rules:
- "script" is a single paragraph (no line breaks).
- **Brevity is mandatory:** use **${wordLo}–${wordHi} words** (hard maximum **${hardMaxWords} words**). One short sentence per idea. No filler, no repeated facts, no long lists. The spoken audio must stay short enough that a normal voice reads it in under ${sec} seconds — we do **not** speed up or compress the voice; if the script is too long, speech gets cut off.
- If unsure, use **fewer** words, not more.
- Pace the content so vehicle facts and features land during the opening, then flow into availability, value, and a confident close that still works when the viewer reads the end card.
- Style: factual and punchy, similar to the samples. Use the listing title as the vehicle opener when it fits.
- Work from the car description: include drivetrain, transmission, odometer (KM), engine size/type, stand-out features, and price if given.
- Write all numbers as words (no numeric digits anywhere). Example style: "two thousand twenty one", "thirty two thousand nine hundred", "three hundred and sixty", "eighty two thousand kilometres".
- Do not invent a price; only include "Priced at ..." if a price appears in the car description.
- If listing price is provided below, include exactly: "Priced at ${enforcedScriptPrice || "<listing price>"}." right before the closing line.
- Always end the script with: Check details at carbarn.com.au
- Stock ID ${stockId || "(none)"} may be ignored for the VO unless useful.

Listing title: ${listingTitle || "Unknown"}
Listing price: ${enforcedScriptPrice || "(not provided)"}
Car description (use these facts):
${carDescription}

Samples:
2021 Toyota Hiace DX Package. 4WD, Automatic, 82k KM. 2.8L turbo diesel with multi-function steering wheel and reversing camera. Priced at $37,900. Check details at carbarn.com.au

2021 Honda Vezel Z Hybrid. 51k KM. 1.5L hybrid with half leather seats and Honda Sensing features. Priced at $28,900. Check details at carbarn.com.au

1997 Isuzu ELF Camper Truck. Automatic, 168k KM. 4.3L diesel 2WD full-furnished motorhome with seating for 9. Priced at $44,900. Check details at carbarn.com.au`;

  const response = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/${config.geminiModel}:generateContent`,
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-goog-api-key": config.geminiApiKey,
      },
      body: JSON.stringify({
        contents: [
          {
            role: "user",
            parts: [{ text: prompt }],
          },
        ],
        generationConfig: {
          temperature: 0.25,
          responseMimeType: "application/json",
        },
      }),
    },
  );

  if (!response.ok) {
    const details = await response.text();
    throw new Error(`Gemini voice-over script failed (${response.status}): ${details}`);
  }

  const payload = await response.json();
  const rawText = payload?.candidates?.[0]?.content?.parts?.map((part) => part.text).filter(Boolean).join("\n");
  if (!rawText) {
    throw new Error("Gemini returned no voice-over script.");
  }

  const parsed = parseLooseJson(rawText);
  let script = String(parsed.script ?? "").trim().replace(/\s+/gu, " ");
  if (!script) {
    throw new Error("Gemini returned an empty script.");
  }

  script = finalizeVoiceoverScript(script, hardMaxWords);
  if (enforcedListingPrice) {
    script = ensurePricedAtBeforeSuffix(script, enforcedListingPrice, hardMaxWords);
  }

  return { script };
}

const VOICEOVER_SUFFIX = "Check details at carbarn.com.au";
const VOICEOVER_HARD_MAX_WORDS = 28;

/** Enforce closing line and word cap for VO scripts (single line). */
export function finalizeVoiceoverScript(script, hardMaxWords = VOICEOVER_HARD_MAX_WORDS) {
  const suffix = VOICEOVER_SUFFIX;
  const suffixWords = suffix.split(/\s+/u);
  let bodyWords = String(script ?? "")
    .trim()
    .replace(/\s+/gu, " ")
    .split(/\s+/u)
    .filter(Boolean);
  if (bodyWords.length >= suffixWords.length) {
    const tail = bodyWords.slice(-suffixWords.length).join(" ").toLowerCase();
    if (tail === suffix.toLowerCase()) {
      bodyWords = bodyWords.slice(0, -suffixWords.length);
    }
  }
  if (bodyWords.length + suffixWords.length > hardMaxWords) {
    const headCount = Math.max(0, hardMaxWords - suffixWords.length);
    return [...bodyWords.slice(0, headCount), ...suffixWords].join(" ");
  }
  return [...bodyWords, ...suffixWords].join(" ");
}

function sanitizeListingPrice(value) {
  const text = String(value ?? "").trim().replace(/\s+/gu, " ");
  if (!text || /^au\s*\$?$/iu.test(text)) {
    return "";
  }
  return text;
}

function normalizeListingPriceForScript(listingPrice) {
  const raw = sanitizeListingPrice(listingPrice);
  if (!raw) {
    return "";
  }
  const match = raw.match(/(\d[\d,]*(?:\.\d{1,2})?)/u);
  if (!match) {
    return raw;
  }
  const numeric = String(match[1] || "").replace(/,/gu, "");
  const parsed = Number(numeric);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return raw;
  }
  return `${parsed.toLocaleString("en-AU")} dollar`;
}

function ensurePricedAtBeforeSuffix(script, listingPrice, hardMaxWords = VOICEOVER_HARD_MAX_WORDS) {
  const suffix = VOICEOVER_SUFFIX;
  const normalizedPrice = normalizeListingPriceForScript(listingPrice) || listingPrice;
  const pricedAtSentence = `Priced at ${normalizedPrice}.`;
  const source = String(script ?? "").trim().replace(/\s+/gu, " ");
  const withoutSuffix = source.toLowerCase().endsWith(suffix.toLowerCase())
    ? source.slice(0, -suffix.length).trim()
    : source;
  const withoutTrailingPrice = withoutSuffix.replace(/priced at[^.?!]*[.?!]?\s*$/iu, "").trim();
  const merged = `${withoutTrailingPrice}${withoutTrailingPrice ? " " : ""}${pricedAtSentence} ${suffix}`.trim();
  return finalizeVoiceoverScript(merged, hardMaxWords);
}

/**
 * Several distinct short scripts for user review before TTS (same rules as single-script generation).
 */
export async function generateVoiceoverScriptVariants(
  config,
  {
    listingTitle,
    stockId,
    carDescription,
    listingPrice = "",
    targetSeconds = 17,
    mainMontageSeconds = 14,
    endSceneSeconds = 3,
  },
  variantCount = 3,
) {
  const sec = Math.max(8, Math.min(17, Number(targetSeconds) || 17));
  const mainS = Math.max(1, Number(mainMontageSeconds) || 14);
  const endS = Math.max(1, Number(endSceneSeconds) || 3);
  const n = Math.min(5, Math.max(2, Math.floor(Number(variantCount) || 3)));
  const wordLo = Math.max(14, Math.round(sec * 1.05));
  const wordHi = Math.min(26, Math.round(sec * 1.45));
  const hardMaxWords = VOICEOVER_HARD_MAX_WORDS;
  const enforcedListingPrice = sanitizeListingPrice(listingPrice);
  const enforcedScriptPrice = normalizeListingPriceForScript(enforcedListingPrice);

  const prompt = `You write Australian used-car dealer voice-over scripts for vertical social reels.

The narration is heard across the **entire** video (${sec} seconds total): roughly the first ${mainS} seconds play over moving footage, and the voice **continues through** the final ${endS} seconds while a static Carbarn-branded end card is on screen. Write **one continuous** script per variant—do not imply the voice stops when the end card appears, and do not say "end card", "graphic", or "screen".

Output strict JSON only with this exact shape:
{"variants":[{"id":"a","script":"first script one line"},{"id":"b","script":"second option"},{"id":"c","script":"third option"}]}

Rules:
- Provide exactly ${n} objects in "variants", with "id" values "a" through ${String.fromCharCode(96 + n)} (letters only).
- Each "script" is one paragraph (no line breaks), **${wordLo}–${wordHi} words**, hard max **${hardMaxWords} words** per script.
- Make the ${n} options **meaningfully different** in emphasis (e.g. one facts-heavy, one value/urgency, one minimal punchy)—not small word swaps of the same text.
- Brevity is mandatory: one short sentence per idea. No filler. The voice is **not** sped up; if a script is too long, speech will be cut off in production.
- Always end each script with: Check details at carbarn.com.au
- Style: factual and punchy, similar to the samples. Use the listing title as the vehicle opener when it fits.
- Work from the car description: include drivetrain, transmission, odometer (KM), engine size/type, stand-out features, and price if given.
- Write all numbers as words (no numeric digits anywhere). Example style: "two thousand twenty one", "thirty two thousand nine hundred", "three hundred and sixty", "eighty two thousand kilometres".
- Do not invent a price; only include "Priced at ..." if a price appears in the car description.
- If listing price is provided below, you must include exactly: "Priced at ${enforcedScriptPrice || "<listing price>"}." right before the closing line.
- Stock ID ${stockId || "(none)"} may be ignored for the VO unless useful.

Listing title: ${listingTitle || "Unknown"}
Listing price: ${enforcedScriptPrice || "(not provided)"}
Car description (use these facts):
${carDescription}

Samples:
2021 Toyota Hiace DX Package. 4WD, Automatic, 82k KM. 2.8L turbo diesel with multi-function steering wheel and reversing camera. Priced at $37,900. Check details at carbarn.com.au

2021 Honda Vezel Z Hybrid. 51k KM. 1.5L hybrid with half leather seats and Honda Sensing features. Priced at $28,900. Check details at carbarn.com.au

1997 Isuzu ELF Camper Truck. Automatic, 168k KM. 4.3L diesel 2WD full-furnished motorhome with seating for 9. Priced at $44,900. Check details at carbarn.com.au`;

  const response = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/${config.geminiModel}:generateContent`,
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-goog-api-key": config.geminiApiKey,
      },
      body: JSON.stringify({
        contents: [
          {
            role: "user",
            parts: [{ text: prompt }],
          },
        ],
        generationConfig: {
          temperature: 0.35,
          responseMimeType: "application/json",
        },
      }),
    },
  );

  if (!response.ok) {
    const details = await response.text();
    throw new Error(`Gemini voice-over variants failed (${response.status}): ${details}`);
  }

  const payload = await response.json();
  const rawText = payload?.candidates?.[0]?.content?.parts?.map((part) => part.text).filter(Boolean).join("\n");
  if (!rawText) {
    throw new Error("Gemini returned no voice-over variants.");
  }

  const parsed = parseLooseJson(rawText);
  const rawVariants = Array.isArray(parsed.variants) ? parsed.variants : [];
  if (!rawVariants.length) {
    throw new Error("Gemini returned no variants array.");
  }

  const labelFor = (id, index) => String(id || "").trim() || String.fromCharCode(65 + index);
  const variants = rawVariants.slice(0, n).map((entry, index) => {
    const id = String(entry?.id ?? "").trim() || String.fromCharCode(97 + index);
    let script = finalizeVoiceoverScript(String(entry?.script ?? "").trim().replace(/\s+/gu, " "), hardMaxWords);
    if (enforcedListingPrice) {
      script = ensurePricedAtBeforeSuffix(script, enforcedListingPrice, hardMaxWords);
    }
    if (!script) {
      throw new Error(`Gemini returned an empty script for variant ${id}.`);
    }
    return { id, label: labelFor(id, index).toUpperCase(), script };
  });

  return { variants };
}

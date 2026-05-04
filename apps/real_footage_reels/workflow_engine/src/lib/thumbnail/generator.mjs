import fs from "node:fs/promises";
import path from "node:path";

const DEFAULT_IMAGE_MODEL = "gemini-3-pro-image-preview";

export async function generateRunThumbnail({
  runDir,
  geminiApiKey,
  imageModel,
  referenceImageDataUrl,
  title,
  subtitle,
  price,
}) {
  const preparedTitle = String(title ?? "").trim();
  const preparedSubtitle = String(subtitle ?? "").trim();
  const preparedPrice = String(price ?? "").trim() || "AU ";
  if (!preparedTitle) {
    throw new Error("Title is required.");
  }
  if (!preparedSubtitle) {
    throw new Error("Subtitle is required.");
  }
  if (!referenceImageDataUrl) {
    throw new Error("Reference image is required.");
  }

  const parsedImage = parseImageDataUrl(referenceImageDataUrl);
  const prompt = buildThumbnailPrompt({
    title: preparedTitle,
    subtitle: preparedSubtitle,
    price: preparedPrice,
  });

  const payload = await requestGeminiThumbnail({
    geminiApiKey,
    imageModel: imageModel || DEFAULT_IMAGE_MODEL,
    prompt,
    referenceMimeType: parsedImage.mimeType,
    referenceBase64: parsedImage.base64,
  });

  const outputImage = extractGeneratedImage(payload);
  if (!outputImage) {
    throw new Error("Gemini did not return an image.");
  }

  const thumbnailsDir = path.join(runDir, "thumbnails");
  await fs.mkdir(thumbnailsDir, { recursive: true });
  const extension = extensionForMimeType(outputImage.mimeType);
  const stamp = Date.now();
  const imageFileName = `thumbnail-${stamp}.${extension}`;
  const imagePath = path.join(thumbnailsDir, imageFileName);
  await fs.writeFile(imagePath, Buffer.from(outputImage.base64, "base64"));

  const metaPath = path.join(thumbnailsDir, `thumbnail-${stamp}.json`);
  await fs.writeFile(
    metaPath,
    `${JSON.stringify(
      {
        createdAt: new Date().toISOString(),
        title: preparedTitle,
        subtitle: preparedSubtitle,
        price: preparedPrice,
        model: imageModel || DEFAULT_IMAGE_MODEL,
        prompt,
        mimeType: outputImage.mimeType,
        fileName: imageFileName,
      },
      null,
      2,
    )}\n`,
    "utf8",
  );

  return {
    imagePath,
    imageMimeType: outputImage.mimeType,
    prompt,
  };
}

function parseImageDataUrl(dataUrl) {
  const value = String(dataUrl ?? "").trim();
  const match = /^data:(image\/[a-zA-Z0-9.+-]+);base64,([A-Za-z0-9+/=\r\n]+)$/u.exec(value);
  if (!match) {
    throw new Error("Reference image must be a valid base64 data URL.");
  }
  return {
    mimeType: match[1].toLowerCase(),
    base64: match[2].replace(/\s+/gu, ""),
  };
}

function buildThumbnailPrompt({ title, subtitle, price }) {
  return `Create a 3:4 ratio social media thumbnail for a car dealership post.

Car:
- Use the provided car image exactly as supplied.
- Do NOT change the car colour, body shape, wheels, trim, or proportions.
- Keep realistic reflections, paint texture, and glass clarity.

Composition:
- Position the car in a clean front 3/4 angle or the natural supplied angle.
- Car centered or slightly lower-third.
- Add a soft realistic ground shadow.

Background:
- Create a unique premium studio-style background.
- Use luxury colors, gradients, cinematic light, metallic texture, soft glow, or light streaks.
- No streets, no buildings, no people, no scenery.
- Background must never distract from the car.

Text & Typography:
- Large headline at top: Montserrat ExtraBold 800, white.
- Secondary line below: Montserrat Medium 500, white.
- Price at bottom: Montserrat Bold 700, white.
- High contrast, mobile readable, premium alignment.

Text content:
- Headline: ${title}
- Subheading: ${subtitle}
- Price: ${price}

Number plate:
- Add a front plate only if the plate area is clearly visible and valid.
- Plate text: CARBARN
- Style: clean NSW dealership plate look
- Font appearance: Manrope Bold style
- Text colour: #4073EA
- Plate background: white
- Number plate only should contain word carbarn
- Correct perspective, scale, and slight contact shadow
- Must look physically mounted
- If the original car image has no designated number plate area, do not add, generate, or imply any number plate. Preserve the original bodywork exactly as shown.


Design style:
- High-end dealership aesthetic.
- Instagram, Facebook, and reel cover ready.
- Cinematic lighting, sharp edges, professional finish.
- No emojis, stickers, random icons, extra logos, or watermarks.

Consistency:
- Keep layout hierarchy consistent.
- Car and text style consistent.
- Background should vary each generation.

Technical:
- Aspect ratio 3:4.
- Ultra-sharp high resolution.
- Natural shadows/reflections.
- Do not crop wheels or mirrors.`;
}

async function requestGeminiThumbnail({
  geminiApiKey,
  imageModel,
  prompt,
  referenceMimeType,
  referenceBase64,
}) {
  const response = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/${imageModel}:generateContent`,
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-goog-api-key": geminiApiKey,
      },
      body: JSON.stringify({
        contents: [
          {
            role: "user",
            parts: [
              { text: prompt },
              {
                inline_data: {
                  mime_type: referenceMimeType,
                  data: referenceBase64,
                },
              },
            ],
          },
        ],
        generationConfig: {
          responseModalities: ["TEXT", "IMAGE"],
        },
      }),
    },
  );

  if (!response.ok) {
    const details = await response.text();
    throw new Error(`Gemini image generation failed (${response.status}): ${details}`);
  }

  return response.json();
}

function extractGeneratedImage(payload) {
  const candidates = Array.isArray(payload?.candidates) ? payload.candidates : [];
  for (const candidate of candidates) {
    const parts = Array.isArray(candidate?.content?.parts) ? candidate.content.parts : [];
    for (const part of parts) {
      const inlineData = part?.inlineData || part?.inline_data;
      const mimeType = inlineData?.mimeType || inlineData?.mime_type;
      const data = inlineData?.data;
      if (mimeType && data && String(mimeType).startsWith("image/")) {
        return {
          mimeType: String(mimeType),
          base64: String(data),
        };
      }
    }
  }
  return null;
}

function extensionForMimeType(mimeType) {
  const value = String(mimeType ?? "").toLowerCase();
  if (value === "image/png") {
    return "png";
  }
  if (value === "image/webp") {
    return "webp";
  }
  return "jpg";
}

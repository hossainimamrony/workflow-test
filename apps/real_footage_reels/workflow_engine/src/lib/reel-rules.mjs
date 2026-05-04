export const LOCKED_REEL_PATTERN_ID = "carbarn-au-front-door-interior-rear-end";
export const LOCKED_REEL_PATTERN_VERSION = 1;
export const LOCKED_REEL_LOCK_MODE = "hard";

export const LOCKED_MONTAGE_ROLE_ORDER = Object.freeze([
  "front_exterior",
  "driver_door_interior_reveal",
  "interior",
  "rear_exterior",
]);

export const LOCKED_FULL_REEL_ORDER = Object.freeze([
  ...LOCKED_MONTAGE_ROLE_ORDER,
  "end_scene",
]);

const LOCKED_STAGE_DEFINITIONS = Object.freeze([
  Object.freeze({
    role: "front_exterior",
    label: "Front Exterior",
    required: true,
    lockedPosition: 1,
    notes: Object.freeze([
      "This must always be the first live-action shot.",
    ]),
  }),
  Object.freeze({
    role: "driver_door_interior_reveal",
    label: "Driver Door Opening / Interior Reveal",
    required: true,
    lockedPosition: 2,
    notes: Object.freeze([
      "Use the driver-side door opening or the first interior reveal.",
      "If an odometer or dashboard view is part of that reveal, it still belongs in this stage.",
    ]),
  }),
  Object.freeze({
    role: "interior",
    label: "Interior / Odometer",
    required: true,
    lockedPosition: 3,
    notes: Object.freeze([
      "Any clean cabin, dashboard, odometer, steering wheel, or seat-detail clip goes here.",
      "All extra interior clips must stay between the driver-door reveal and the rear exterior.",
    ]),
  }),
  Object.freeze({
    role: "rear_exterior",
    label: "Backside Exterior",
    required: true,
    lockedPosition: 4,
    notes: Object.freeze([
      "This must be the last live-action vehicle shot before the end scene.",
    ]),
  }),
  Object.freeze({
    role: "end_scene",
    label: "End Scene",
    required: true,
    lockedPosition: 5,
    notes: Object.freeze([
      "The branded end scene is always appended after the live-action montage.",
    ]),
  }),
]);

export const LOCKED_REEL_RULEBOOK = Object.freeze({
  patternId: LOCKED_REEL_PATTERN_ID,
  version: LOCKED_REEL_PATTERN_VERSION,
  lockMode: LOCKED_REEL_LOCK_MODE,
  summary:
    "Front exterior -> driver door opening/interior reveal -> interior/odometer -> backside exterior -> end scene",
  montageOrder: LOCKED_MONTAGE_ROLE_ORDER,
  fullOrder: LOCKED_FULL_REEL_ORDER,
  stages: LOCKED_STAGE_DEFINITIONS,
});

export const LOCKED_REEL_AI_INSTRUCTION = [
  "The reel order is hard-locked in code and must never change.",
  `Locked order: ${renderLockedReelOrder()}.`,
  "The model may classify clips, but it may not invent or suggest a different montage order.",
].join(" ");

export const INTERIOR_GUIDANCE = [
  "Odometer, dashboard, steering wheel, center-console, and cabin-detail shots count as interior.",
  "If the driver-side door opening or open-door reveal is the dominant action and the interior is visible, use driver_door_interior_reveal instead of plain interior.",
].join(" ");

export function getLockedTargetSequence() {
  return [...LOCKED_MONTAGE_ROLE_ORDER];
}

export function renderLockedReelOrder(options = {}) {
  const includeEndScene = options.includeEndScene !== false;
  const roles = includeEndScene ? LOCKED_FULL_REEL_ORDER : LOCKED_MONTAGE_ROLE_ORDER;
  return roles.join(" -> ");
}

export function ensureLockedTargetSequence(sequenceRoles = LOCKED_MONTAGE_ROLE_ORDER) {
  const normalizedRoles = Array.isArray(sequenceRoles)
    ? sequenceRoles.map((role) => String(role ?? "").trim())
    : [];

  const expected = LOCKED_MONTAGE_ROLE_ORDER;
  const matches =
    normalizedRoles.length === expected.length &&
    normalizedRoles.every((role, index) => role === expected[index]);

  if (!matches) {
    throw new Error(
      `Locked reel order violation. Expected ${renderLockedReelOrder({ includeEndScene: false })}.`,
    );
  }

  return getLockedTargetSequence();
}

export function buildRulebookMetadata() {
  return {
    patternId: LOCKED_REEL_PATTERN_ID,
    version: LOCKED_REEL_PATTERN_VERSION,
    lockMode: LOCKED_REEL_LOCK_MODE,
    montageOrder: getLockedTargetSequence(),
    fullOrder: [...LOCKED_FULL_REEL_ORDER],
    summary: LOCKED_REEL_RULEBOOK.summary,
  };
}

export function assertCompositionFollowsLockedPattern(segments = []) {
  const seenCounts = new Map();
  let highestStageIndex = -1;

  for (const segment of segments) {
    const role = String(segment?.purpose ?? segment?.role ?? "").trim();
    if (!LOCKED_MONTAGE_ROLE_ORDER.includes(role)) {
      throw new Error(`Locked reel order violation. Unexpected segment role "${role}".`);
    }

    const stageIndex = LOCKED_MONTAGE_ROLE_ORDER.indexOf(role);
    if (stageIndex < highestStageIndex) {
      throw new Error(
        `Locked reel order violation. Segment "${role}" appeared after a later stage.`,
      );
    }

    highestStageIndex = stageIndex;
    seenCounts.set(role, (seenCounts.get(role) ?? 0) + 1);

    if (role !== "interior" && seenCounts.get(role) > 1) {
      throw new Error(`Locked reel order violation. Segment "${role}" was repeated.`);
    }
  }
}

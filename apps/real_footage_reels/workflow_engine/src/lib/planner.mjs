import {
  LOCKED_MONTAGE_ROLE_ORDER,
  assertCompositionFollowsLockedPattern,
  buildRulebookMetadata,
  ensureLockedTargetSequence,
  renderLockedReelOrder,
} from "./reel-rules.mjs";

export function buildReelPlan(clips, sequenceRoles = LOCKED_MONTAGE_ROLE_ORDER, options = {}) {
  const lockedSequenceRoles = ensureLockedTargetSequence(sequenceRoles);
  const remaining = clips.map((clip, index) => ({ ...clip, __timelineIndex: index }));
  const sequence = [];
  let lastChosenTimelineIndex = -1;

  for (const role of lockedSequenceRoles) {
    const eligible = remaining.filter((clip) => isEligibleForRole(clip, role));
    const inOrderPool = eligible.filter((clip) => clip.__timelineIndex > lastChosenTimelineIndex);
    const pool = inOrderPool.length ? inOrderPool : eligible;
    const ranked = [...pool].sort((left, right) => {
      return scoreForRole(right, role) - scoreForRole(left, role);
    });

    const chosen = ranked[0];
    const chosenScore = chosen ? scoreForRole(chosen, role) : 0;
    if (!chosen || chosenScore <= 0) {
      continue;
    }

    const { __timelineIndex: _timelineIndex, ...chosenPublic } = chosen;
    sequence.push({
      ...chosenPublic,
      role,
      score: chosenScore,
    });
    lastChosenTimelineIndex = chosen.__timelineIndex;

    const chosenIndex = remaining.findIndex((clip) => clip.clipId === chosen.clipId);
    if (chosenIndex >= 0) {
      remaining.splice(chosenIndex, 1);
    }
  }

  const composition = buildCompositionPlan(clips, sequence, {
    totalDurationSeconds: options.totalDurationSeconds ?? 15,
  });
  const selectedPurposes = new Set(composition.segments.map((item) => item.purpose));
  const missingRoles = lockedSequenceRoles.filter((role) => !selectedPurposes.has(role));

  return {
    createdAt: new Date().toISOString(),
    rulebook: buildRulebookMetadata(),
    sequenceRoles: lockedSequenceRoles,
    missingRoles,
    patternComplete: missingRoles.length === 0,
    sequence,
    composition,
    alternates: remaining
      .sort((left, right) => overallScore(right) - overallScore(left))
      .slice(0, 10)
      .map((clip) => ({
        clipId: clip.clipId,
        title: clip.title,
        primaryLabel: clip.analysis.primaryLabel,
        roleLabel: clip.analysis.roleLabel ?? deriveRoleLabel(clip.analysis.primaryLabel),
        reason: clip.analysis.reason,
        scores: clip.analysis.scores,
      })),
  };
}

function isEligibleForRole(clip, role) {
  const roleLabel = clip.analysis.roleLabel ?? deriveRoleLabel(clip.analysis.primaryLabel);
  const primaryLabel = clip.analysis.primaryLabel ?? "";
  const scores = clip.analysis.scores ?? {};

  if (role === "front_exterior") {
    return (
      roleLabel === "front_exterior" ||
      primaryLabel.startsWith("front_") ||
      Number(scores.front_exterior || 0) > 0
    );
  }

  if (role === "driver_door_interior_reveal") {
    return (
      roleLabel === "driver_door_interior_reveal" ||
      primaryLabel === "driver_door_interior_reveal" ||
      Number(scores.driver_door_interior_reveal || 0) > 0 ||
      Boolean(clip.analysis.doorOpen && clip.analysis.interiorVisible) ||
      middleRevealRoleScore(clip) >= 60
    );
  }

  if (role === "interior") {
    return interiorRoleScore(clip) > 0;
  }

  if (role === "rear_exterior") {
    return (
      roleLabel === "rear_exterior" ||
      primaryLabel.startsWith("rear_") ||
      Number(scores.rear_exterior || 0) > 0
    );
  }

  return scoreForRole(clip, role) > 0;
}

export function renderPlanSummary(plan) {
  const lines = [`Locked reel pattern: ${renderLockedReelOrder()}`, "Selected reel sequence:"];
  for (const item of plan.sequence) {
    lines.push(
      `- ${item.role}: ${item.clipId} (score ${item.score}, view ${item.analysis.primaryLabel})`,
    );
  }
  if (plan.missingRoles?.length) {
    lines.push(`Pattern gaps: ${plan.missingRoles.join(", ")}`);
  }
  if (plan.composition?.segments?.length) {
    const sec = plan.composition?.totalDurationSeconds ?? 14;
    lines.push(
      `${sec}s composition order: ${plan.composition.segments
        .map((item) => `${item.purpose}:${item.clipId}`)
        .join(" -> ")}`,
    );
  }
  return lines.join("\n");
}

function scoreForRole(clip, role) {
  const base = roleBaseScore(clip, role);
  const bonus =
    role === (clip.analysis.roleLabel ?? deriveRoleLabel(clip.analysis.primaryLabel))
      ? 10
      : 0;
  const confidenceWeight = base > 0 || bonus > 0 ? clip.analysis.confidence * 0.15 : 0;
  const penalty = rolePenalty(clip, role);
  return Math.max(0, Math.round(base + bonus + confidenceWeight - penalty));
}

function overallScore(clip) {
  return Math.max(
    clip.analysis.scores?.front_exterior ?? 0,
    clip.analysis.scores?.driver_door_interior_reveal ?? 0,
    clip.analysis.scores?.rear_exterior ?? 0,
    interiorRoleScore(clip),
  );
}

function deriveRoleLabel(primaryLabel) {
  if (primaryLabel?.startsWith("front_") || primaryLabel === "front_exterior") {
    return "front_exterior";
  }

  if (primaryLabel?.startsWith("rear_") || primaryLabel === "rear_exterior") {
    return "rear_exterior";
  }

  if (primaryLabel?.startsWith("side_") || primaryLabel === "side_exterior") {
    return "side_exterior";
  }

  return primaryLabel ?? "other";
}

function buildCompositionPlan(clips, sequence, options) {
  const selectedByRole = new Map(sequence.map((item) => [item.role, item]));
  const front = selectedByRole.get("front_exterior") ?? null;
  const driverReveal = selectedByRole.get("driver_door_interior_reveal") ?? null;
  const leadInterior = selectedByRole.get("interior") ?? null;

  const usedClipIds = new Set(
    [front, driverReveal, leadInterior].filter(Boolean).map((item) => item.clipId),
  );
  const rear = chooseRearCompositionClip(clips, selectedByRole.get("rear_exterior") ?? null, usedClipIds);
  if (rear) {
    usedClipIds.add(rear.clipId);
  }

  const interiorCandidates = driverReveal
    ? [
      ...(leadInterior ? [leadInterior] : []),
      ...chooseInteriorCompositionClips(clips, usedClipIds),
    ]
    : [];

  const segments = [];
  if (front) {
    segments.push({
      ...front,
      role: "front_exterior",
      purpose: "front_exterior",
      startRatio: 0,
      weight: 4,
    });
  }

  if (driverReveal) {
    segments.push({
      ...driverReveal,
      role: "driver_door_interior_reveal",
      purpose: "driver_door_interior_reveal",
      startRatio: 0,
      weight: 4,
    });
  }

  for (const clip of interiorCandidates) {
    segments.push({
      ...clip,
      role: "interior",
      purpose: "interior",
      startRatio: 0.35,
      weight: 2.5,
    });
  }

  if (rear) {
    segments.push({
      ...rear,
      role: "rear_exterior",
      purpose: "rear_exterior",
      startRatio: rear.analysis.interiorVisible || rear.analysis.doorOpen ? 0.45 : 0.15,
      weight: 4,
    });
  }

  assertCompositionFollowsLockedPattern(segments);

  const totalWeight = segments.reduce((sum, item) => sum + item.weight, 0) || 1;
  let remainingDuration = options.totalDurationSeconds;
  let remainingWeight = totalWeight;

  return {
    totalDurationSeconds: options.totalDurationSeconds,
    segments: segments.map((segment, index) => {
      const rawDuration = remainingDuration * (segment.weight / Math.max(remainingWeight, 0.001));
      const durationSeconds =
        index === segments.length - 1
          ? roundDuration(remainingDuration)
          : roundDuration(rawDuration);
      remainingDuration -= durationSeconds;
      remainingWeight -= segment.weight;

      return {
        clipId: segment.clipId,
        role: segment.role,
        purpose: segment.purpose,
        title: segment.title,
        primaryLabel: segment.analysis.primaryLabel,
        startRatio: segment.startRatio,
        durationSeconds,
        videoPath: segment.videoPath,
        framePath: segment.framePath,
      };
    }),
  };
}

function chooseRearCompositionClip(clips, selectedRear, usedClipIds) {
  if (selectedRear && !usedClipIds.has(selectedRear.clipId)) {
    return selectedRear;
  }

  const rearCandidates = clips
    .filter((clip) => !usedClipIds.has(clip.clipId))
    .filter((clip) => clip.analysis.scores.rear_exterior > 0 || clip.analysis.roleLabel === "rear_exterior")
    .sort((left, right) => compositionRearScore(right) - compositionRearScore(left));

  return rearCandidates[0] ?? null;
}

function chooseInteriorCompositionClips(clips, usedClipIds) {
  return clips
    .map((clip, index) => ({ clip, index }))
    .filter((item) => !usedClipIds.has(item.clip.clipId))
    .filter((item) => isInteriorCompositionClip(item.clip))
    .sort((left, right) => left.index - right.index)
    .map((item) => item.clip);
}

function isInteriorCompositionClip(clip) {
  const roleLabel = clip.analysis.roleLabel ?? deriveRoleLabel(clip.analysis.primaryLabel);
  const primaryLabel = clip.analysis.primaryLabel ?? "";
  const secondaryLabels = normalizeSecondaryLabels(clip);
  if (
    roleLabel === "front_exterior" ||
    roleLabel === "rear_exterior" ||
    roleLabel === "driver_door_interior_reveal" ||
    roleLabel === "engine_bay" ||
    roleLabel === "wheel" ||
    primaryLabel.startsWith("front_") ||
    primaryLabel.startsWith("rear_") ||
    primaryLabel === "front_exterior" ||
    primaryLabel === "rear_exterior" ||
    primaryLabel === "driver_door_interior_reveal" ||
    primaryLabel === "engine_bay" ||
    primaryLabel === "wheel"
  ) {
    return false;
  }

  return (
    roleLabel === "interior" ||
    primaryLabel === "interior" ||
    clip.analysis.interiorVisible ||
    secondaryLabels.includes("interior") ||
    secondaryLabels.includes("odometer") ||
    secondaryLabels.includes("dashboard")
  );
}

function compositionRearScore(clip) {
  let score = scoreForRole(clip, "rear_exterior");
  if (clip.analysis.rearVisible) {
    score += 12;
  }
  if (clip.analysis.interiorVisible) {
    score -= 20;
  }
  if (clip.analysis.doorOpen) {
    score -= 12;
  }
  return score;
}

function rolePenalty(clip, role) {
  if (role === "front_exterior" || role === "rear_exterior") {
    return (clip.analysis.interiorVisible ? 18 : 0) + (clip.analysis.doorOpen ? 12 : 0);
  }

  if (role === "driver_door_interior_reveal") {
    return clip.analysis.interiorVisible && clip.analysis.doorOpen ? 0 : 18;
  }

  if (role === "interior") {
    return (clip.analysis.frontVisible ? 14 : 0) + (clip.analysis.rearVisible ? 14 : 0);
  }

  return 0;
}

function roundDuration(value) {
  return Math.max(0.05, Math.round(value * 1000) / 1000);
}

function roleBaseScore(clip, role) {
  if (role === "driver_door_interior_reveal") {
    return Math.max(clip.analysis.scores?.[role] ?? 0, middleRevealRoleScore(clip));
  }
  if (role === "interior") {
    return interiorRoleScore(clip);
  }

  return clip.analysis.scores?.[role] ?? 0;
}

function interiorRoleScore(clip) {
  const roleLabel = clip.analysis.roleLabel ?? deriveRoleLabel(clip.analysis.primaryLabel);
  const primaryLabel = clip.analysis.primaryLabel ?? "";
  const secondaryLabels = normalizeSecondaryLabels(clip);

  if (
    roleLabel === "front_exterior" ||
    roleLabel === "rear_exterior" ||
    roleLabel === "side_exterior" ||
    roleLabel === "engine_bay" ||
    roleLabel === "wheel" ||
    primaryLabel.startsWith("front_") ||
    primaryLabel.startsWith("rear_") ||
    primaryLabel.startsWith("side_") ||
    primaryLabel === "engine_bay" ||
    primaryLabel === "wheel"
  ) {
    return 0;
  }

  let score = 0;
  if (roleLabel === "interior") {
    score += 75;
  }
  if (primaryLabel === "interior") {
    score += 10;
  }
  if (clip.analysis.interiorVisible) {
    score += 55;
  }
  if (clip.analysis.doorOpen) {
    score += 10;
  }
  if (secondaryLabels.includes("interior")) {
    score += 10;
  }
  if (secondaryLabels.includes("odometer") || secondaryLabels.includes("dashboard")) {
    score += 12;
  }

  return Math.max(0, Math.min(100, score));
}

function middleRevealRoleScore(clip) {
  const roleLabel = clip.analysis.roleLabel ?? deriveRoleLabel(clip.analysis.primaryLabel);
  const primaryLabel = clip.analysis.primaryLabel ?? "";
  const secondaryLabels = normalizeSecondaryLabels(clip);

  // Keep native driver-door reveal strongest when present.
  let score = 0;
  if (roleLabel === "driver_door_interior_reveal" || primaryLabel === "driver_door_interior_reveal") {
    score += 95;
  }
  if (clip.analysis.doorOpen && clip.analysis.interiorVisible) {
    score += 70;
  }

  // Accept steering/odometer/dashboard as the stage-2 reveal fallback.
  if (secondaryLabels.includes("odometer")) {
    score += 82;
  }
  if (secondaryLabels.includes("dashboard")) {
    score += 76;
  }
  if (secondaryLabels.includes("steering") || secondaryLabels.includes("steering_wheel")) {
    score += 78;
  }

  if (secondaryLabels.includes("driver") || secondaryLabels.includes("driver-seat") || secondaryLabels.includes("driver_seat")) {
    score += 74;
  }
  if (secondaryLabels.includes("steering-wheel")) {
    score += 78;
  }

  // Avoid exterior clips in stage-2 fallback.
  if (
    roleLabel === "front_exterior" ||
    roleLabel === "rear_exterior" ||
    primaryLabel.startsWith("front_") ||
    primaryLabel.startsWith("rear_")
  ) {
    score = Math.max(0, score - 60);
  }

  return Math.max(0, Math.min(100, score));
}

function normalizeSecondaryLabels(clip) {
  return Array.isArray(clip.analysis.secondaryLabels)
    ? clip.analysis.secondaryLabels.map((label) => String(label).trim().toLowerCase())
    : [];
}

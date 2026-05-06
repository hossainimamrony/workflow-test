import assert from "node:assert/strict";

import { buildReelPlan } from "./planner.mjs";
import {
  LOCKED_MONTAGE_ROLE_ORDER,
  LOCKED_REEL_PATTERN_ID,
  ensureLockedTargetSequence,
} from "./reel-rules.mjs";

runTest("buildReelPlan keeps the locked Carbarn order with interior clips in the middle", () => {
  const clips = [
    makeClip({
      clipId: "front-1",
      primaryLabel: "front_left_exterior",
      scores: { front_exterior: 96, driver_door_interior_reveal: 0, rear_exterior: 0 },
      frontVisible: true,
    }),
    makeClip({
      clipId: "door-1",
      primaryLabel: "driver_door_interior_reveal",
      roleLabel: "driver_door_interior_reveal",
      scores: { front_exterior: 0, driver_door_interior_reveal: 98, rear_exterior: 0 },
      doorOpen: true,
      interiorVisible: true,
    }),
    makeClip({
      clipId: "odometer-1",
      primaryLabel: "interior",
      roleLabel: "interior",
      secondaryLabels: ["odometer", "dashboard"],
      interiorVisible: true,
    }),
    makeClip({
      clipId: "seats-1",
      primaryLabel: "interior",
      roleLabel: "interior",
      secondaryLabels: ["interior"],
      interiorVisible: true,
    }),
    makeClip({
      clipId: "rear-1",
      primaryLabel: "rear_left_exterior",
      scores: { front_exterior: 0, driver_door_interior_reveal: 0, rear_exterior: 95 },
      rearVisible: true,
    }),
  ];

  const plan = buildReelPlan(clips, LOCKED_MONTAGE_ROLE_ORDER, {
    totalDurationSeconds: 14,
  });

  assert.deepEqual(
    plan.sequence.map((item) => item.role),
    LOCKED_MONTAGE_ROLE_ORDER,
  );
  assert.deepEqual(
    plan.composition.segments.map((item) => item.purpose),
    [
      "front_exterior",
      "driver_door_interior_reveal",
      "interior",
      "interior",
      "rear_exterior",
    ],
  );
  assert.equal(plan.composition.segments[2].clipId, "odometer-1");
  assert.equal(plan.patternComplete, true);
  assert.deepEqual(plan.missingRoles, []);
  assert.equal(plan.rulebook.patternId, LOCKED_REEL_PATTERN_ID);
});

runTest("buildReelPlan leaves a stage empty instead of breaking the locked order", () => {
  const clips = [
    makeClip({
      clipId: "front-1",
      primaryLabel: "front_left_exterior",
      scores: { front_exterior: 96, driver_door_interior_reveal: 0, rear_exterior: 0 },
      frontVisible: true,
    }),
    makeClip({
      clipId: "door-1",
      primaryLabel: "driver_door_interior_reveal",
      roleLabel: "driver_door_interior_reveal",
      scores: { front_exterior: 0, driver_door_interior_reveal: 98, rear_exterior: 0 },
      doorOpen: true,
      interiorVisible: true,
    }),
    makeClip({
      clipId: "rear-1",
      primaryLabel: "rear_left_exterior",
      scores: { front_exterior: 0, driver_door_interior_reveal: 0, rear_exterior: 95 },
      rearVisible: true,
    }),
  ];

  const plan = buildReelPlan(clips, LOCKED_MONTAGE_ROLE_ORDER, {
    totalDurationSeconds: 14,
  });

  assert.deepEqual(
    plan.sequence.map((item) => item.role),
    [
      "front_exterior",
      "driver_door_interior_reveal",
      "rear_exterior",
    ],
  );
  assert.deepEqual(
    plan.composition.segments.map((item) => item.purpose),
    [
      "front_exterior",
      "driver_door_interior_reveal",
      "rear_exterior",
    ],
  );
  assert.equal(plan.patternComplete, false);
  assert.deepEqual(plan.missingRoles, ["interior"]);
});

runTest("ensureLockedTargetSequence rejects any reordered pattern", () => {
  assert.throws(
    () =>
      ensureLockedTargetSequence([
        "front_exterior",
        "rear_exterior",
        "driver_door_interior_reveal",
        "interior",
      ]),
    /Locked reel order violation/u,
  );
});

runTest("buildReelPlan does not place interior before stage-2 reveal when reveal is missing", () => {
  const clips = [
    makeClip({
      clipId: "front-1",
      primaryLabel: "front_left_exterior",
      scores: { front_exterior: 96, driver_door_interior_reveal: 0, rear_exterior: 0 },
      frontVisible: true,
    }),
    makeClip({
      clipId: "interior-1",
      primaryLabel: "interior",
      roleLabel: "interior",
      secondaryLabels: ["interior"],
      interiorVisible: true,
    }),
    makeClip({
      clipId: "rear-1",
      primaryLabel: "rear_left_exterior",
      scores: { front_exterior: 0, driver_door_interior_reveal: 0, rear_exterior: 95 },
      rearVisible: true,
    }),
  ];

  const plan = buildReelPlan(clips, LOCKED_MONTAGE_ROLE_ORDER, {
    totalDurationSeconds: 14,
  });

  assert.deepEqual(
    plan.composition.segments.map((item) => item.purpose),
    [
      "front_exterior",
      "rear_exterior",
    ],
  );
  assert.deepEqual(plan.missingRoles, ["driver_door_interior_reveal", "interior"]);
});

function runTest(name, fn) {
  try {
    fn();
    console.log(`ok - ${name}`);
  } catch (error) {
    console.error(`not ok - ${name}`);
    throw error;
  }
}

function makeClip({
  clipId,
  primaryLabel,
  roleLabel = null,
  secondaryLabels = [],
  confidence = 92,
  scores = {},
  doorOpen = false,
  interiorVisible = false,
  rearVisible = false,
  frontVisible = false,
}) {
  return {
    clipId,
    title: clipId,
    videoPath: `C:/tmp/${clipId}.mp4`,
    framePath: `C:/tmp/${clipId}.jpg`,
    analysis: {
      primaryLabel,
      roleLabel,
      secondaryLabels,
      confidence,
      reason: "",
      scores: {
        front_exterior: scores.front_exterior ?? 0,
        driver_door_interior_reveal: scores.driver_door_interior_reveal ?? 0,
        rear_exterior: scores.rear_exterior ?? 0,
      },
      doorOpen,
      interiorVisible,
      rearVisible,
      frontVisible,
    },
  };
}

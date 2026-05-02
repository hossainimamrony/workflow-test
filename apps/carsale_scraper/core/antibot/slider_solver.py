#!/usr/bin/env python3

"""
Sync Playwright helper for slider-style anti-bot challenges.
Forces the slider to be dragged all the way to the right end of the track.
Improved success detection and frame‑detachment handling.
"""

from __future__ import annotations

import math
import random
import time
from typing import Any


def _is_visible(locator: Any) -> bool:
    try:
        if locator.count() <= 0:
            return False
        return bool(locator.first.is_visible())
    except Exception:
        return False


def _challenge_still_visible(root: Any, button_selector: str, track_selector: str) -> bool:
    """Return True only if the challenge elements are definitely still visible.
    If anything fails (frame detached, etc.), assume success."""
    try:
        btn = root.locator(button_selector)
        trk = root.locator(track_selector)
        if _is_visible(btn) and _is_visible(trk):
            return True
        dd = root.locator("#ddv1-captcha-container")
        if dd.count() > 0 and dd.first.is_visible():
            return True
    except Exception:
        # Frame detached or other error → challenge is gone
        return False
    return False


def _iter_roots(page: Any) -> list[Any]:
    roots: list[Any] = [page]
    try:
        for fr in page.frames:
            if fr is not None:
                roots.append(fr)
    except Exception:
        pass
    return roots


def _human_bezier(t: float) -> float:
    """Easing: fast start, slow middle, fast end (human-like)."""
    if t < 0.5:
        return 4 * t * t * t
    else:
        return 1 - pow(-2 * t + 2, 3) / 2


def _human_drag_full_right(
    page: Any,
    start_x: float,
    start_y: float,
    end_x: float,
    end_y: float,
    max_y_deviation: float = 8.0,
) -> None:
    """
    Drag from start to end with human-like curved path, overshoot, and micro-correction.
    """
    distance = end_x - start_x
    if distance <= 0:
        return

    steps = random.randint(40, 70)
    time.sleep(random.uniform(0.05, 0.15))

    prev_x = start_x
    prev_y = start_y

    for i in range(1, steps + 1):
        t = i / steps
        eased_t = _human_bezier(t)

        x = start_x + distance * eased_t
        y_dev = max_y_deviation * math.sin(math.pi * t)
        y_jitter = random.uniform(-2.0, 2.0)
        y = start_y + y_dev + y_jitter
        x += random.uniform(-0.8, 0.8)

        if random.random() < 0.08:
            time.sleep(random.uniform(0.02, 0.07))

        page.mouse.move(x, y)
        time.sleep(random.uniform(0.008, 0.025))

    # Overshoot slightly
    overshoot_x = end_x + random.uniform(3.0, 12.0)
    overshoot_y = end_y + random.uniform(-3.0, 3.0)
    page.mouse.move(overshoot_x, overshoot_y)
    time.sleep(random.uniform(0.04, 0.12))
    # Correct back to exact end
    page.mouse.move(end_x, end_y)
    time.sleep(random.uniform(0.05, 0.15))
    page.mouse.up()


def _drag_once(page: Any, root: Any, button_selector: str, track_selector: str) -> bool:
    btn = root.locator(button_selector).first
    trk = root.locator(track_selector).first

    btn_box = btn.bounding_box()
    trk_box = trk.bounding_box()
    if not btn_box or not trk_box:
        print("❌ Cannot get bounding boxes for button or track.")
        return False

    start_x = float(btn_box["x"]) + (float(btn_box["width"]) * 0.5)
    start_y = float(btn_box["y"]) + (float(btn_box["height"]) * 0.5)
    track_left = float(trk_box["x"])
    track_width = float(trk_box["width"])
    handle_width = float(btn_box["width"])
    end_x = track_left + track_width - (handle_width / 2)
    end_y = float(trk_box["y"]) + (float(trk_box["height"]) * 0.5)

    if end_x <= start_x:
        print("⚠️ end_x is not to the right – check track and button positions.")
        return False

    # Human approach
    approach_x = start_x + random.uniform(-30, 30)
    approach_y = start_y + random.uniform(-20, 20)
    page.mouse.move(approach_x, approach_y)
    time.sleep(random.uniform(0.1, 0.3))
    page.mouse.move(start_x, start_y)
    time.sleep(random.uniform(0.1, 0.25))
    page.mouse.down()
    time.sleep(random.uniform(0.08, 0.22))

    _human_drag_full_right(page, start_x, start_y, end_x, end_y, max_y_deviation=random.uniform(5.0, 12.0))

    # Wait a little longer for the page to react
    time.sleep(random.uniform(2.0, 3.0))

    # Success check: if the challenge elements are no longer visible (including frame detached) → success
    if not _challenge_still_visible(root, button_selector, track_selector):
        print("✅ Drag successful – challenge no longer visible.")
        return True

    # Additional check: sometimes the iframe detaches after drag, causing _challenge_still_visible to throw, but we already caught that above.
    # Double check if the main page navigated away from captcha domain.
    try:
        page_url = str(getattr(page, "url", "") or "").lower()
        if "captcha-delivery.com/captcha" not in page_url:
            print("✅ Drag likely succeeded – page navigated away from captcha domain.")
            return True
    except Exception:
        pass

    print("⚠️ Drag completed but challenge still visible (may be blocked).")
    return False


def _drag_once_via_locator(root: Any, button_selector: str, track_selector: str) -> bool:
    """Fallback – not recommended for full‑width drags, but kept."""
    btn = root.locator(button_selector).first
    trk = root.locator(track_selector).first
    try:
        btn.drag_to(trk, force=True)
        time.sleep(random.uniform(1.5, 2.5))
    except Exception as e:
        print(f"❌ Native drag_to failed: {e}")
        return False
    success = not _challenge_still_visible(root, button_selector, track_selector)
    if success:
        print("✅ Native drag_to succeeded.")
    else:
        print("⚠️ Native drag_to completed but not far enough?")
    return success


def solve_slider(
    *,
    page: Any,
    slider_button_selector: str,
    slider_track_selector: str,
    max_attempts: int = 2,
) -> bool:
    """
    Attempts to solve a slider challenge by dragging the handle all the way to the right.
    Waits 5 seconds before starting.
    """
    attempts = max(1, int(max_attempts or 1))

    print("⏳ Waiting 5 seconds before attempting to drag the slider...")
    time.sleep(5)

    roots = _iter_roots(page)

    for attempt in range(1, attempts + 1):
        print(f"\n--- Attempt {attempt}/{attempts} ---")
        found_any = False

        for idx, root in enumerate(roots):
            try:
                btn = root.locator(slider_button_selector)
                trk = root.locator(slider_track_selector)
                btn_visible = _is_visible(btn)
                trk_visible = _is_visible(trk)

                if not (btn_visible and trk_visible):
                    missing = []
                    if not btn_visible:
                        missing.append(f"button '{slider_button_selector}'")
                    if not trk_visible:
                        missing.append(f"track '{slider_track_selector}'")
                    print(f"🔍 Frame {idx}: {', '.join(missing)} not visible.")
                    continue

                found_any = True
                print(f"🎯 Found both elements in frame {idx}. Dragging full width to the right...")

                if _drag_once(page, root, slider_button_selector, slider_track_selector):
                    return True

                # If drag_once failed, try the native locator alternative
                if _drag_once_via_locator(root, slider_button_selector, slider_track_selector):
                    return True

                print("❌ Both drag strategies failed for this frame.")
            except Exception as e:
                print(f"⚠️ Error in frame {idx}: {e}")
                continue

        if not found_any:
            print("❌ Could not find a visible slider button and track in any frame. Check your selectors and page state.")

    print(f"\n❌ Failed to solve slider after {attempts} attempt(s).")
    return False
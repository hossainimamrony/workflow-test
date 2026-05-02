from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

DEFAULT_SESSION_HEALTH_FILE = "session_health.json"


def add_session_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--storage-state",
        default="",
        help="Path to a Playwright storage state JSON file to load/save for session reuse.",
    )
    parser.add_argument(
        "--session-pool-dir",
        default="",
        help="Directory of storage state JSON files. If --storage-state is empty, one session is selected automatically.",
    )
    parser.add_argument(
        "--session-health-file",
        default=DEFAULT_SESSION_HEALTH_FILE,
        help="JSON file name under --out-dir for tracking session health.",
    )
    parser.add_argument(
        "--challenge-cooldown-base-seconds",
        type=int,
        default=25,
        help="Base cooldown in seconds after challenge/timeout signals (multiplies with repeated failures).",
    )
    parser.add_argument(
        "--save-storage-state-on-exit",
        action="store_true",
        help="Save refreshed storage state back to the selected --storage-state or pool file at run end.",
    )


def load_session_health_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"sessions": {}}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {"sessions": {}}
        sessions = data.get("sessions")
        if not isinstance(sessions, dict):
            data["sessions"] = {}
        return data
    except Exception:
        return {"sessions": {}}


def save_session_health_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    tmp.replace(path)


def choose_session_from_pool(
    session_pool_dir: Path,
    health_data: dict[str, Any],
    now_ts: float,
) -> Path | None:
    if not session_pool_dir.exists():
        return None
    candidates = sorted([p for p in session_pool_dir.glob("*.json") if p.is_file()])
    if not candidates:
        return None
    sessions = health_data.setdefault("sessions", {})
    best: tuple[int, float, str, Path] | None = None
    for p in candidates:
        rec = sessions.get(p.name, {}) if isinstance(sessions.get(p.name, {}), dict) else {}
        cool_until = float(rec.get("cooldown_until_epoch", 0) or 0)
        in_cooldown = cool_until > now_ts
        fails = int(rec.get("consecutive_failures", 0) or 0)
        score = (1 if in_cooldown else 0, fails, str(rec.get("last_used_at", "")))
        cand = (score[0], float(score[1]), score[2], p)
        if best is None or cand < best:
            best = cand
    return best[3] if best else None


def resolve_active_session_state(args: Any, out_dir: Path) -> tuple[Path, dict[str, Any], Path | None]:
    session_health_path = out_dir / args.session_health_file
    session_pool_dir = Path(args.session_pool_dir) if args.session_pool_dir else None
    active_session_state: Path | None = None
    session_health = load_session_health_json(session_health_path)
    now_epoch = time.time()
    if args.storage_state and str(args.storage_state).strip():
        active_session_state = Path(args.storage_state).expanduser()
    elif session_pool_dir is not None:
        active_session_state = choose_session_from_pool(session_pool_dir, session_health, now_epoch)
        if active_session_state is None and bool(getattr(args, "save_storage_state_on_exit", False)):
            # Bootstrap first pool state automatically for UI-first workflows.
            active_session_state = session_pool_dir / "state1.json"
    elif bool(getattr(args, "save_storage_state_on_exit", False)):
        # No explicit state/pool provided: still persist one reusable UI state under out_dir.
        active_session_state = out_dir / "sessions" / "ui_auto_state.json"
    return session_health_path, session_health, active_session_state


def register_challenge_timeout(meta: dict[str, Any], challenge_failures: int) -> tuple[int, bool]:
    timed_out = str(meta.get("wait_state", "")).strip().lower() == "timeout"
    if timed_out:
        return challenge_failures + 1, True
    return 0, False


def compute_cooldown_seconds(base_seconds: int, challenge_failures: int) -> int:
    return int(base_seconds) * max(1, int(challenge_failures))


def update_session_health_after_run(
    *,
    session_health_path: Path,
    session_health: dict[str, Any],
    active_session_state: Path | None,
    mode: str,
    pages_done: int,
    base_cooldown_seconds: int,
) -> None:
    if not active_session_state:
        return
    sessions = session_health.setdefault("sessions", {})
    rec = sessions.setdefault(active_session_state.name, {})
    rec["path"] = str(active_session_state)
    rec["last_used_at"] = datetime.now().isoformat(timespec="seconds")
    rec["last_used_epoch"] = int(time.time())
    if mode == "full":
        if int(pages_done or 0) == 0:
            rec["consecutive_failures"] = int(rec.get("consecutive_failures", 0) or 0) + 1
            rec["last_status"] = "failed_no_pages"
        else:
            rec["consecutive_failures"] = 0
            rec["last_status"] = "ok"
    rec["cooldown_until_epoch"] = int(time.time()) + (
        int(base_cooldown_seconds) * int(rec.get("consecutive_failures", 0) or 0)
    )
    save_session_health_json(session_health_path, session_health)

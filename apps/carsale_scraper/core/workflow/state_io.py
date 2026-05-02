from __future__ import annotations

import csv
import os
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


def target_key(year: Any, make: Any, model: Any, url: Any) -> str:
    parts = [
        str(year or "").strip().lower(),
        str(make or "").strip().lower(),
        str(model or "").strip().lower(),
        str(url or "").strip().lower(),
    ]
    return "|".join(parts)


def row_unique_key(row: dict[str, Any]) -> str:
    for k in ["detail_url", "network_id", "card_id"]:
        v = str(row.get(k, "") or "").strip().lower()
        if v:
            return v
    return ""


def value_is_empty(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and pd.isna(v):
        return True
    return str(v).strip() == ""


def _read_records_map_csv(path: Path) -> dict[str, dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        out: dict[str, dict[str, Any]] = {}
        for r in reader:
            key = row_unique_key(r)
            if key:
                out[key] = dict(r)
        return out


def load_skipped_targets_map(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    out: dict[str, dict[str, Any]] = {}
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                k = str(r.get("target_key", "")).strip().lower()
                if not k:
                    k = target_key(r.get("year", ""), r.get("make", ""), r.get("model", ""), r.get("search_url", ""))
                if k:
                    out[k] = dict(r)
    except Exception:
        return {}
    return out


def load_records_map(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    candidates = [path]
    bak = path.with_suffix(path.suffix + ".bak")
    if bak.exists():
        candidates.append(bak)
    snapshots_dir = path.parent / "state" / "snapshots"
    if snapshots_dir.exists():
        snap_files = sorted(
            snapshots_dir.glob(f"{path.stem}__*.csv"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        candidates.extend(snap_files[:5])

    for candidate in candidates:
        try:
            data = _read_records_map_csv(candidate)
            if candidate != path:
                print(f"[recovery] Loaded {path.name} from {candidate.name}")
            return data
        except Exception:
            continue
    return {}


def _atomic_replace_with_retries(src: Path, dst: Path, retries: int = 8, delay_sec: float = 0.25) -> bool:
    for i in range(retries):
        try:
            os.replace(src, dst)
            return True
        except PermissionError:
            if i >= retries - 1:
                break
            time.sleep(delay_sec * (i + 1))
        except Exception:
            if i >= retries - 1:
                break
            time.sleep(delay_sec)
    return False


def save_records_map(
    path: Path,
    data: dict[str, dict[str, Any]],
    preferred_columns: list[str] | None = None,
    *,
    state_dir: Path | None = None,
):
    rows = list(data.values())

    temp_dir = (state_dir or (path.parent / "state")) / "tmp"
    snapshots_dir = (state_dir or (path.parent / "state")) / "snapshots"
    temp_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    cols_set: set[str] = set()
    if rows:
        for r in rows:
            cols_set.update(r.keys())

    ordered: list[str] = []
    if rows:
        for c in (preferred_columns or []):
            if c in cols_set:
                ordered.append(c)
        for c in sorted(cols_set):
            if c not in ordered:
                ordered.append(c)
    elif preferred_columns:
        ordered = list(preferred_columns)

    tmp_path = temp_dir / f"{path.name}.{os.getpid()}.{int(datetime.now().timestamp() * 1000)}.tmp"
    with tmp_path.open("w", encoding="utf-8-sig", newline="") as f:
        if ordered:
            writer = csv.DictWriter(f, fieldnames=ordered)
            writer.writeheader()
            for r in rows:
                out = {}
                for c in ordered:
                    v = r.get(c, "")
                    out[c] = "" if v is None else str(v)
                writer.writerow(out)
        f.flush()
        os.fsync(f.fileno())

    backup_path = path.with_suffix(path.suffix + ".bak")
    if path.exists():
        try:
            shutil.copy2(path, backup_path)
        except Exception:
            pass

    replaced = _atomic_replace_with_retries(tmp_path, path, retries=12, delay_sec=0.2)
    if not replaced:
        stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        degraded = snapshots_dir / f"{path.stem}__lock_fallback__{stamp}.csv"
        try:
            shutil.copy2(tmp_path, degraded)
            tmp_path.unlink(missing_ok=True)
            print(
                f"[warn] Could not replace locked file {path.name}; "
                f"saved fallback snapshot {degraded.name}."
            )
            return
        except Exception as e:
            raise PermissionError(f"Failed to write {path}: {e}") from e

    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    snapshot_path = snapshots_dir / f"{path.stem}__{stamp}.csv"
    try:
        shutil.copy2(path, snapshot_path)
    except Exception:
        pass

    all_snaps = sorted(
        snapshots_dir.glob(f"{path.stem}__*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for old in all_snaps[20:]:
        try:
            old.unlink()
        except Exception:
            pass


def save_skipped_targets_map(path: Path, data: dict[str, dict[str, Any]], *, state_dir: Path | None = None):
    save_records_map(
        path,
        data,
        preferred_columns=[
            "target_key",
            "year",
            "make",
            "model",
            "search_url",
            "total_pages",
            "skip_reason",
            "updated_at",
        ],
        state_dir=state_dir,
    )


def upsert_record(
    table: dict[str, dict[str, Any]],
    row: dict[str, Any],
    *,
    updated_at: str,
) -> str | None:
    key = row_unique_key(row)
    if not key:
        return None
    current = dict(table.get(key, {}))
    for k, v in row.items():
        if value_is_empty(v):
            continue
        current[k] = v
    current["_row_key"] = key
    current["updated_at"] = updated_at
    table[key] = current
    return key


def remove_record(table: dict[str, dict[str, Any]], key: str):
    if key in table:
        table.pop(key, None)

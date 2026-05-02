#!/usr/bin/env python3
"""
Find Carsales cards that match Carbarn inventory.

Matching logic:
1) Exact match on (price, odometer_km)
2) For matched suspects, open detail page and check whether "carbarn" exists in page text
"""

import argparse
import asyncio
import csv
import json
import os
import random
import re
import shutil
import time
import unicodedata
from datetime import datetime, date
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import pandas as pd
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

from . import state_io
from . import sessions as session_ops
from ..antibot import slider_solver as slider_automate
DEFAULT_SEARCH_URL = "https://www.carsales.com.au/cars/dealer/"
DEFAULT_INVENTORY = "carbarn_inventory.csv.xlsx"
FIXED_LOCATION_SLUG = "new-south-wales-state"
ALL_CARDS_FILE = "current/all_cards.csv"
SUSPECTED_FILE = "current/suspected_cars.csv"
MY_CARS_FILE = "current/my_cars.csv"
SKIPPED_TARGETS_FILE = "current/skipped_targets.csv"
MAX_TARGET_TOTAL_PAGES = 30
PROGRESS_PREFIX = "PROGRESS_JSON:"


def _read_bool_env(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _running_on_pythonanywhere() -> bool:
    return any(
        bool(str(os.environ.get(key, "")).strip())
        for key in ("PYTHONANYWHERE_SITE", "PYTHONANYWHERE_DOMAIN", "PYTHONANYWHERE_HOME")
    )


def _default_headless_mode() -> bool:
    # Default to headless on PythonAnywhere and allow override via env.
    return _read_bool_env("CARSALE_SCRAPER_HEADLESS", _running_on_pythonanywhere())


def _resolve_chromium_executable_path() -> str | None:
    explicit = str(
        os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE")
        or os.environ.get("CARSALE_CHROMIUM_EXECUTABLE")
        or ""
    ).strip()
    if explicit:
        return explicit
    if _running_on_pythonanywhere():
        return "/usr/bin/chromium"
    return None


def _playwright_launch_kwargs(headless: bool) -> dict[str, Any]:
    launch_args: list[str] = []
    if not headless:
        launch_args.append("--disable-features=CalculateNativeWinOcclusion")
    if headless:
        launch_args.append("--disable-gpu")
    # Linux hosts (including PythonAnywhere) usually need these flags.
    if os.name != "nt" or _running_on_pythonanywhere():
        launch_args.extend(["--no-sandbox", "--disable-dev-shm-usage"])

    launch_kwargs: dict[str, Any] = {"headless": bool(headless)}
    if launch_args:
        launch_kwargs["args"] = list(dict.fromkeys(launch_args))
    executable_path = _resolve_chromium_executable_path()
    if executable_path:
        launch_kwargs["executable_path"] = executable_path
    return launch_kwargs


def _read_records_map_csv(path: Path) -> dict[str, dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        out: dict[str, dict[str, Any]] = {}
        for r in reader:
            key = row_unique_key(r)
            if key:
                out[key] = dict(r)
        return out


def _target_key(year: Any, make: Any, model: Any, url: Any) -> str:
    parts = [
        str(year or "").strip().lower(),
        str(make or "").strip().lower(),
        str(model or "").strip().lower(),
        str(url or "").strip().lower(),
    ]
    return "|".join(parts)


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
                    k = _target_key(r.get("year", ""), r.get("make", ""), r.get("model", ""), r.get("search_url", ""))
                if k:
                    out[k] = dict(r)
    except Exception:
        return {}
    return out


def parse_int(value: Any) -> int | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    m = re.findall(r"\d+", s.replace(",", ""))
    if not m:
        return None
    try:
        return int("".join(m))
    except Exception:
        return None


def normalize_price(value: Any) -> int | None:
    return parse_int(value)


def normalize_km(value: Any) -> int | None:
    return parse_int(value)


def slugify_carsales(value: Any) -> str:
    s = str(value or "").strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s


def remove_crop_param_from_image_url(url: str) -> str:
    """
    For pxcrush image URLs, remove only pxc_method=crop so full image variant can be used.
    """
    try:
        p = urlparse(url or "")
        if "pxcrush.net" not in (p.netloc or "").lower():
            return url
        pairs = parse_qsl(p.query, keep_blank_values=True)
        filtered = [(k, v) for (k, v) in pairs if not (k == "pxc_method" and str(v).lower() == "crop")]
        new_q = urlencode(filtered, doseq=True)
        return urlunparse((p.scheme, p.netloc, p.path, p.params, new_q, p.fragment))
    except Exception:
        return url


def walk(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from walk(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from walk(v)


def find_inventory_columns(df: pd.DataFrame) -> tuple[str | None, str | None]:
    cols = list(df.columns)
    low = {c: str(c).strip().lower() for c in cols}

    price_priority = [
        "price",
        "sale price",
        "sell price",
        "asking price",
        "list price",
    ]
    odo_priority = [
        "odometer",
        "odo",
        "odometer km",
        "kms",
        "km",
        "mileage",
        "kilometres",
        "kilometers",
    ]

    def pick(priority_words: list[str]) -> str | None:
        for w in priority_words:
            for c in cols:
                if w in low[c]:
                    return c
        return None

    price_col = pick(price_priority)
    odo_col = pick(odo_priority)

    return price_col, odo_col


def load_inventory_pairs(
    path: Path,
    *,
    odometer_col_override: str | None = None,
    allow_price_only: bool = False,
) -> tuple[pd.DataFrame, set[tuple[int, int]], set[int], str]:
    if not path.exists():
        raise FileNotFoundError(f"Inventory file not found: {path}")

    if path.suffix.lower() in [".xlsx", ".xls"] or path.name.lower().endswith(".csv.xlsx"):
        inv = pd.read_excel(path)
    elif path.suffix.lower() == ".csv":
        inv = pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported inventory file type: {path.suffix}")

    price_col, odo_col = find_inventory_columns(inv)
    if odometer_col_override:
        if odometer_col_override not in inv.columns:
            raise ValueError(
                f"--odometer-col '{odometer_col_override}' not found in inventory columns: {list(inv.columns)}"
            )
        odo_col = odometer_col_override

    if not price_col:
        raise ValueError(
            f"Could not detect a price column.\nColumns found: {list(inv.columns)}\n"
            "Add/rename a column to include 'price'."
        )

    if not odo_col and not allow_price_only:
        raise ValueError(
            "Could not detect an odometer column in inventory.\n"
            f"Columns found: {list(inv.columns)}\n"
            "For exact matching, add a column like 'Odometer'/'KM' and rerun.\n"
            "Temporary fallback: rerun with --allow-price-only"
        )
    inv = inv.copy()
    inv["_match_price"] = inv[price_col].map(normalize_price)
    if odo_col:
        inv["_match_odo"] = inv[odo_col].map(normalize_km)
        inv = inv.dropna(subset=["_match_price", "_match_odo"])
        inv["_match_odo"] = inv["_match_odo"].astype(int)
        match_mode = "price+odometer"
    else:
        inv = inv.dropna(subset=["_match_price"])
        inv["_match_odo"] = pd.NA
        match_mode = "price-only"

    inv["_match_price"] = inv["_match_price"].astype(int)

    pair_set = set()
    if odo_col:
        pair_set = set(zip(inv["_match_price"].tolist(), inv["_match_odo"].tolist()))
    price_set = set(inv["_match_price"].tolist())
    return inv, pair_set, price_set, match_mode


def build_inventory_search_targets(
    inventory_df: pd.DataFrame,
    *,
    location_slug: str | None = None,
) -> list[tuple[str, str, str, str]]:
    """
    Build Carsales dealer URLs from inventory Year+Make combinations.
    Preferred format (as requested):
      /cars/dealer/{year}/{make}/{location}
    Returns list of (year, make, model, url).
    """
    cols = list(inventory_df.columns)
    low = {c: str(c).strip().lower() for c in cols}

    make_col = next((c for c in cols if "make" in low[c]), None)
    model_col = next((c for c in cols if "model" in low[c]), None)
    year_col = next((c for c in cols if low[c] == "year" or "year" in low[c]), None)
    if not make_col:
        return []

    pairs = set()
    for _, row in inventory_df.iterrows():
        year = str(row.get(year_col, "")).strip() if year_col else ""
        make = str(row.get(make_col, "")).strip()
        model = str(row.get(model_col, "")).strip() if model_col else ""
        if not make or make.lower() == "nan":
            continue
        if year.lower() == "nan":
            year = ""
        pairs.add((year, make, model))

    targets: list[tuple[str, str, str, str]] = []
    seen_urls: set[str] = set()
    for year, make, model in sorted(pairs):
        syear = re.sub(r"[^0-9]", "", str(year))
        smake = slugify_carsales(make)
        smodel = slugify_carsales(model) if model else ""
        if not smake:
            continue

        def add_target(url: str):
            norm = (url or "").strip().lower()
            if not norm or norm in seen_urls:
                return
            seen_urls.add(norm)
            targets.append((syear, make, model, url))

        if location_slug:
            sloc = slugify_carsales(location_slug)
            # Prefer make+model URLs first when model exists.
            if smodel:
                add_target(f"https://www.carsales.com.au/cars/dealer/{smake}/{smodel}/{sloc}/")
            # Keep year+make as fallback/coverage URL.
            if syear:
                add_target(f"https://www.carsales.com.au/cars/dealer/{syear}/{smake}/{sloc}/")
            if not smodel and not syear:
                add_target(f"https://www.carsales.com.au/cars/dealer/{smake}/{sloc}/")
        else:
            if smodel:
                add_target(f"https://www.carsales.com.au/cars/dealer/{smake}/{smodel}/")
            if syear:
                add_target(f"https://www.carsales.com.au/cars/dealer/{syear}/{smake}/")
            if not smodel and not syear:
                add_target(f"https://www.carsales.com.au/cars/dealer/{smake}/")

    return targets


def parse_ymd(value: str | None) -> date | None:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        raise ValueError(f"Invalid date '{value}'. Use YYYY-MM-DD format.")


def parse_human_date(value: str) -> date | None:
    v = (value or "").strip()
    if not v:
        return None
    fmts = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d %b %Y",
        "%d %B %Y",
        "%b %d %Y",
        "%B %d %Y",
    ]
    for f in fmts:
        try:
            return datetime.strptime(v, f).date()
        except Exception:
            continue
    return None


def extract_inventory_date_range(inventory_df: pd.DataFrame) -> tuple[date | None, date | None, str | None]:
    """
    Detect a date-like column in inventory and return (min_date, max_date, column_name).
    """
    cols = list(inventory_df.columns)
    low = {c: str(c).strip().lower() for c in cols}
    keywords = ["date", "listed", "list date", "added", "created", "stock in", "stockin"]
    candidate_cols = [c for c in cols if any(k in low[c] for k in keywords)]

    for col in candidate_cols:
        parsed: list[date] = []
        for v in inventory_df[col].tolist():
            if pd.isna(v):
                continue
            if isinstance(v, datetime):
                parsed.append(v.date())
                continue
            # pandas Timestamp without direct import guard
            if hasattr(v, "date") and callable(getattr(v, "date")):
                try:
                    parsed.append(v.date())
                    continue
                except Exception:
                    pass
            d = parse_human_date(str(v))
            if d:
                parsed.append(d)
        if parsed:
            return min(parsed), max(parsed), col
    return None, None, None


async def wait_for_listings(page, timeout_ms: int = 240000) -> str:
    print("\nIf anti-bot challenge appears, solve it manually in browser.")
    print("Waiting for listing cards...")
    waited = 0
    step = 1500
    while waited < timeout_ms:
        try:
            state = await page.evaluate(
                """
                () => {
                  const html = document.documentElement ? document.documentElement.innerHTML : '';
                  const listingCount = (html.match(/"type":"ListingCard"/g) || []).length;
                  const bodyText = (document.body && document.body.innerText ? document.body.innerText : '').toLowerCase();
                  const noResults =
                    bodyText.includes('no results this time') ||
                    /\\b0\\s+dealer\\b/.test(bodyText);
                  return { listingCount, noResults };
                }
                """
            )
        except Exception:
            # Happens during redirects/challenge reloads ("Execution context was destroyed").
            state = {"listingCount": 0, "noResults": False}

        count = int(state.get("listingCount", 0) or 0)
        if bool(state.get("noResults")):
            print("No results detected for this target. Moving to next one.")
            return "no_results"
        if count > 0:
            print(f"Listing cards detected in page payload: {count}")
            return "ok"
        await page.wait_for_timeout(step)
        waited += step
    print("Timeout waiting for cards, continuing anyway.")
    return "timeout"


def extract_cards_from_html(html: str, base_url: str) -> list[dict]:
    m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.S)
    if not m:
        return []

    try:
        data = json.loads(m.group(1))
    except Exception:
        return []

    cards = []
    for d in walk(data):
        if not isinstance(d, dict):
            continue
        if d.get("type") != "ListingCard":
            continue

        heading = d.get("heading") or {}
        heading_children = heading.get("children") if isinstance(heading, dict) else []
        heading_parts = []
        for c in heading_children or []:
            if isinstance(c, dict) and c.get("type") == "Text":
                v = c.get("value")
                if v:
                    heading_parts.append(str(v))
        title = " ".join(heading_parts).strip()

        price_block = d.get("price") or {}
        price_children = price_block.get("children") if isinstance(price_block, dict) else []
        price_text = ""
        for c in price_children or []:
            if isinstance(c, dict) and c.get("type") == "Text" and c.get("value"):
                price_text = str(c["value"])
                break
        price = normalize_price(price_text)

        odo_text = ""
        key_details = d.get("keyDetailsTexts") or []
        for item in key_details:
            t = str(item)
            if "km" in t.lower():
                odo_text = t
                break
        odometer_km = normalize_km(odo_text)

        # Prefer "View details" CTA URL when present (more reliable mapping per card).
        detail_path = None
        ctas = d.get("ctas") or []
        for cta in ctas:
            if not isinstance(cta, dict):
                continue
            label = str(cta.get("label", "")).strip().lower()
            action = cta.get("action") if isinstance(cta, dict) else {}
            action_data = action.get("data") if isinstance(action, dict) else {}
            url_candidate = action_data.get("url") if isinstance(action_data, dict) else None
            if url_candidate and ("view details" in label or "/cars/details/" in str(url_candidate).lower()):
                detail_path = url_candidate
                break

        # Fallback to card-level action URL.
        if not detail_path:
            action = d.get("action") or {}
            action_data = action.get("data") if isinstance(action, dict) else {}
            detail_path = action_data.get("url") if isinstance(action_data, dict) else None
        detail_url = urljoin(base_url, detail_path) if detail_path else ""

        cards.append(
            {
                "card_id": d.get("id", ""),
                "title": title,
                "price_text": price_text,
                "price": price,
                "odometer_text": odo_text,
                "odometer_km": odometer_km,
                "detail_url": detail_url,
            }
        )
    return cards


async def extract_cards_from_dom(page, base_url: str) -> list[dict]:
    """
    Extract cards from currently rendered DOM (page-specific), not hydration JSON.
    """
    rows = await page.evaluate(
        """
        () => {
          const out = [];
          const seen = new Set();

          const detailsAnchors = Array.from(document.querySelectorAll('a[href*="/cars/details/"]'));
          const pickCard = (el) => {
            let node = el.closest('article, li, [data-testid*="listing"], [class*="listing"], [class*="card"], div');
            // Climb up to find a reasonable card boundary with price + km.
            for (let i = 0; i < 8 && node; i++) {
              const t = (node.innerText || '');
              if (/\\$\\s?[\\d,]+/.test(t) && /km\\b/i.test(t)) return node;
              node = node.parentElement;
            }
            return el.closest('article, li, div');
          };

          for (const a of detailsAnchors) {
            const hrefRaw = a.getAttribute('href') || '';
            if (!hrefRaw) continue;
            const detail_url = new URL(hrefRaw, location.origin).href;

            // Exclude editorial/etc just in case.
            if (!/\\/cars\\/details\\//i.test(detail_url)) continue;

            const card = pickCard(a);
            if (!card) continue;

            const text = (card.innerText || '').replace(/\\s+/g, ' ').trim();
            const html = card.innerHTML || '';
            if (!text) continue;

            let title = '';
            const h = card.querySelector('h1,h2,h3,[data-testid*="title"],[class*="title"]');
            if (h) title = (h.textContent || '').replace(/\\s+/g, ' ').trim();

            if (!title) {
              const mTitle = text.match(/\\b(19|20)\\d{2}\\s+[A-Za-z]+\\s+[A-Za-z0-9\\-]+/);
              title = mTitle ? mTitle[0] : text.slice(0, 80);
            }

            const mPrice = text.match(/\\$\\s?[\\d,]+/);
            const price_text = mPrice ? mPrice[0] : '';

            const mKm = text.match(/[\\d,]{2,}\\s*km\\b/i);
            const odometer_text = mKm ? mKm[0] : '';

            const mNet = (detail_url || text || html).match(/OAG-AD-\\d+/i);
            const network_id = mNet ? mNet[0].toUpperCase() : '';

            const key = (detail_url || (title + '|' + price_text + '|' + odometer_text)).toLowerCase();
            if (!key || seen.has(key)) continue;
            seen.add(key);

            out.push({ title, price_text, odometer_text, detail_url, network_id });
          }

          // Fallback path: if no detail anchors were found, try View details controls.
          if (out.length === 0) {
            const viewControls = Array.from(document.querySelectorAll('button, a'))
              .filter((b) => /view\\s*details/i.test((b.textContent || '').trim()));
            for (const ctl of viewControls) {
              const card = pickCard(ctl);
              if (!card) continue;
              const text = (card.innerText || '').replace(/\\s+/g, ' ').trim();
              const html = card.innerHTML || '';
              const mHref = html.match(/\\/cars\\/details\\/[^"'\\s<)]+/i);
              const detail_url = mHref ? new URL(mHref[0], location.origin).href : '';
              if (!detail_url) continue;
              const mPrice = text.match(/\\$\\s?[\\d,]+/);
              const mKm = text.match(/[\\d,]{2,}\\s*km\\b/i);
              let title = '';
              const h = card.querySelector('h1,h2,h3,[data-testid*="title"],[class*="title"]');
              if (h) title = (h.textContent || '').replace(/\\s+/g, ' ').trim();
              const key = detail_url.toLowerCase();
              if (seen.has(key)) continue;
              seen.add(key);
              out.push({
                title,
                price_text: mPrice ? mPrice[0] : '',
                odometer_text: mKm ? mKm[0] : '',
                detail_url,
                network_id: ''
              });
            }
          }

          return out;
        }
        """
    )

    cards = []
    for r in rows or []:
        cards.append(
            {
                "card_id": r.get("network_id", "") or r.get("detail_url", ""),
                "title": (r.get("title") or "").strip(),
                "price_text": (r.get("price_text") or "").strip(),
                "price": normalize_price(r.get("price_text")),
                "odometer_text": (r.get("odometer_text") or "").strip(),
                "odometer_km": normalize_km(r.get("odometer_text")),
                "detail_url": urljoin(base_url, r.get("detail_url") or ""),
                "network_id": (r.get("network_id") or "").strip(),
            }
        )
    return cards


async def lazy_load_current_page(page, passes: int = 2):
    """
    Slowly scroll through current results page so lazy components fully render.
    """
    for _ in range(max(1, passes)):
        try:
            total_h = await page.evaluate("() => document.body ? document.body.scrollHeight : 0")
        except Exception:
            total_h = 0
        if not total_h:
            await page.wait_for_timeout(900)
            continue

        step = 550
        y = 0
        while y < total_h:
            try:
                await page.evaluate("(yy) => window.scrollTo(0, yy)", y)
            except Exception:
                break
            await page.wait_for_timeout(280)
            y += step

        await page.wait_for_timeout(1200)

    # Ensure we end near pagination controls while triggering lazy renders.
    try:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    except Exception:
        pass
    await page.wait_for_timeout(900)
    # Some results pages virtualize cards while scrolling; return to top before
    # extraction so the first listings are mounted and can be captured.
    try:
        await page.evaluate("window.scrollTo(0, 0)")
    except Exception:
        pass
    await page.wait_for_timeout(700)


async def anti_bot_scroll_detail_page(page) -> None:
    """
    Human-like scroll behavior for each detail page visit.
    Helps warm up behavioral signals before scraping fields.
    """
    try:
        total_h = await page.evaluate("() => Math.max(document.body?.scrollHeight || 0, document.documentElement?.scrollHeight || 0)")
    except Exception:
        total_h = 0

    if total_h <= 0:
        await page.wait_for_timeout(900)
        return

    # Start with a small natural hesitation.
    await page.wait_for_timeout(random.randint(500, 1200))

    max_depth = min(total_h, random.randint(1200, 3000))
    y = 0
    while y < max_depth:
        y += random.randint(180, 520)
        try:
            await page.evaluate("(yy) => window.scrollTo(0, yy)", min(y, max_depth))
        except Exception:
            break
        await page.wait_for_timeout(random.randint(220, 700))

    # Small bounce-up and settle, common in real browsing.
    for _ in range(random.randint(1, 2)):
        back_y = max(0, y - random.randint(150, 450))
        try:
            await page.evaluate("(yy) => window.scrollTo(0, yy)", back_y)
        except Exception:
            break
        await page.wait_for_timeout(random.randint(180, 450))

    try:
        await page.evaluate("(yy) => window.scrollTo(0, yy)", y)
    except Exception:
        pass
    await page.wait_for_timeout(random.randint(700, 1500))


async def read_page_label(page) -> str:
    try:
        return await page.evaluate(
            """
            () => {
              const nav = document.querySelector('nav[aria-label="pagination"]');
              if (!nav) return '';
              const span = nav.querySelector('span');
              return span ? (span.textContent || '').trim() : '';
            }
            """
        )
    except Exception:
        return ""


def parse_total_pages_from_label(label: str) -> int | None:
    t = (label or "").strip()
    if not t:
        return None
    m = re.search(r"(?i)\bpage\s+\d+\s+of\s+([0-9,]+)\b", t)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except Exception:
        return None


async def click_next_page(page, *, expected_results_url: str | None = None) -> bool:
    # Carsales pagination can be flaky with overlays/challenge refreshes.
    # Retry a few times before deciding there is no next page.
    for _attempt in range(6):
        try:
            await page.keyboard.press("End")
        except Exception:
            try:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            except Exception:
                pass
        await page.wait_for_timeout(700)

        next_btn = page.locator('nav[aria-label="pagination"] button:has-text("Next")').first
        try:
            if await next_btn.count() == 0:
                await page.wait_for_timeout(800)
                continue
            if not await next_btn.is_enabled():
                return False
        except Exception:
            await page.wait_for_timeout(800)
            continue

        old_label = await read_page_label(page)
        old_url = page.url

        clicked = False
        try:
            await next_btn.click(timeout=8000, force=True)
            clicked = True
        except Exception:
            # JS fallback click if normal click is intercepted.
            try:
                clicked = await page.evaluate(
                    """
                    () => {
                      const nav = document.querySelector('nav[aria-label="pagination"]');
                      if (!nav) return false;
                      const buttons = Array.from(nav.querySelectorAll('button'));
                      const next = buttons.find((b) => /next/i.test((b.textContent || '').trim()));
                      if (!next || next.disabled) return false;
                      next.click();
                      return true;
                    }
                    """
                )
            except Exception:
                clicked = False

        if not clicked:
            await page.wait_for_timeout(900)
            continue

        try:
            await page.wait_for_load_state("domcontentloaded", timeout=20000)
        except Exception:
            pass

        # Detect actual page change.
        for _ in range(30):
            await page.wait_for_timeout(500)
            new_label = await read_page_label(page)
            if new_label and old_label and new_label != old_label:
                return True
            if page.url != old_url:
                current_url = (page.url or "").strip().lower()
                if "/cars/details/" in current_url:
                    # Sometimes a flaky click lands on a listing detail page.
                    # Recover back to results and retry pagination click.
                    print(f"[warn] Unexpected navigation to detail page during pagination: {page.url}")
                    try:
                        await page.goto(old_url, wait_until="domcontentloaded", timeout=45000)
                    except Exception:
                        if expected_results_url:
                            try:
                                await page.goto(expected_results_url, wait_until="domcontentloaded", timeout=45000)
                            except Exception:
                                pass
                    break
                return True

        # If click was attempted but no change, retry from scratch.
        await page.wait_for_timeout(1000)

    return False


async def goto_with_retries(
    page,
    url: str,
    *,
    timeout_ms: int = 90000,
    attempts: int = 3,
    label: str = "page",
) -> bool:
    target = (url or "").strip()
    if not target:
        print(f"[error] Empty URL for {label}; navigation skipped.")
        return False

    for attempt in range(1, attempts + 1):
        try:
            print(f"[nav] Opening {label} ({attempt}/{attempts}): {target}")
            await page.goto(target, wait_until="domcontentloaded", timeout=timeout_ms)
            current = (page.url or "").strip()
            if not current or current == "about:blank":
                raise RuntimeError("Navigation ended on about:blank")
            return True
        except Exception as e:
            print(f"[warn] Navigation failed for {label} attempt {attempt}: {e}")
            if attempt < attempts:
                await page.wait_for_timeout(1500)

    print(f"[error] Could not open {label} after {attempts} attempts: {target}")
    return False


async def collect_all_cards(
    page,
    search_url: str,
    max_pages: int = 20,
    on_page_cards: Callable[[list[dict], int], Awaitable[None]] | None = None,
) -> tuple[list[dict], dict[str, Any]]:
    opened = await goto_with_retries(page, search_url, timeout_ms=90000, attempts=3, label="search target")
    if not opened:
        return [], {"skipped": False, "wait_state": "nav_failed"}
    wait_state = await wait_for_listings(page)
    if wait_state == "no_results":
        return [], {"skipped": False, "no_results": True, "wait_state": wait_state}
    await page.wait_for_timeout(1000)

    page_label = await read_page_label(page)
    total_pages = parse_total_pages_from_label(page_label)
    if total_pages is not None and total_pages > MAX_TARGET_TOTAL_PAGES:
        print(
            f"[skip] Target skipped due to too many pages: {total_pages} "
            f"(limit={MAX_TARGET_TOTAL_PAGES}) for {search_url}"
        )
        return [], {
            "skipped": True,
            "skip_reason": "too_many_cards",
            "total_pages": total_pages,
            "page_label": page_label,
            "wait_state": wait_state,
        }

    all_cards: dict[str, dict] = {}
    page_num = 1

    while page_num <= max_pages:
        try:
            current_url = (page.url or "").strip().lower()
            if "/cars/details/" in current_url:
                print(f"[warn] Collector drifted to detail page, returning to results: {page.url}")
                opened = await goto_with_retries(page, search_url, timeout_ms=60000, attempts=2, label="results recovery")
                if not opened:
                    break
                wait_state = await wait_for_listings(page, timeout_ms=120000)
                if wait_state == "no_results":
                    break

            await lazy_load_current_page(page, passes=2)
            cards = await extract_cards_from_dom(page, base_url=search_url)
            if not cards:
                # Fallback when DOM extraction is blocked by render state.
                try:
                    html = await page.content()
                    cards = extract_cards_from_html(html, base_url=search_url)
                except Exception:
                    cards = []
        except Exception as e:
            msg = str(e).lower()
            if "target page, context or browser has been closed" in msg or "targetclosederror" in msg:
                print("[warn] Page/context closed during collect. Skipping current target and continuing.")
                return list(all_cards.values()), {
                    "skipped": False,
                    "total_pages": total_pages,
                    "page_label": page_label,
                    "wait_state": "target_closed",
                }
            raise

        print(f"Page {page_num}: extracted {len(cards)} cards")
        if on_page_cards is not None:
            try:
                await on_page_cards(cards, page_num)
            except Exception as e:
                print(f"[warn] on_page_cards callback error: {e}")
        for c in cards:
            key = c.get("detail_url") or c.get("card_id") or f"page{page_num}_{len(all_cards)}"
            if key:
                all_cards[key] = c

        moved = await click_next_page(page, expected_results_url=search_url)
        if not moved:
            break
        page_num += 1

    return list(all_cards.values()), {
        "skipped": False,
        "total_pages": total_pages,
        "page_label": page_label,
        "wait_state": wait_state,
    }


def extract_listing_date_from_text(text: str) -> date | None:
    t = (text or "").replace("\n", " ")
    patterns = [
        r"(?i)(?:listed|list date|added|published|updated|last updated)\s*[:\-]?\s*([0-9]{4}-[0-9]{2}-[0-9]{2})",
        r"(?i)(?:listed|list date|added|published|updated|last updated)\s*[:\-]?\s*([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{4})",
        r"(?i)(?:listed|list date|added|published|updated|last updated)\s*[:\-]?\s*([0-9]{1,2}\s+[A-Za-z]{3,9}\s+[0-9]{4})",
    ]
    for p in patterns:
        m = re.search(p, t)
        if m:
            d = parse_human_date(m.group(1))
            if d:
                return d

    # Fallback: pick first date-like token in the page.
    fallback_patterns = [
        r"\b([0-9]{4}-[0-9]{2}-[0-9]{2})\b",
        r"\b([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})\b",
        r"\b([0-9]{1,2}\s+[A-Za-z]{3,9}\s+[0-9]{4})\b",
    ]
    for p in fallback_patterns:
        m = re.search(p, t)
        if m:
            d = parse_human_date(m.group(1))
            if d:
                return d
    return None


async def verify_carbarn_on_detail(
    context,
    url: str,
    *,
    page=None,
    keep_tab_open: bool = False,
) -> tuple[bool, str, date | None, str, str]:
    detail_page = page
    created_page = False
    if detail_page is None:
        detail_page = await context.new_page()
        created_page = True
    try:
        opened = await goto_with_retries(detail_page, url, timeout_ms=90000, attempts=3, label="detail page")
        if not opened:
            return False, "detail_error: navigation_failed", None, "", ""

        if keep_tab_open:
            print("Keeper tab opened. If anti-bot challenge appears, solve it there; script will continue after it clears.")
            waited = 0
            step = 1500
            timeout_ms = 600000
            challenge_markers = [
                "are you a robot",
                "verify you are human",
                "security check",
                "please verify",
                "captcha",
                "challenge",
                "cloudflare",
            ]
            while waited < timeout_ms:
                try:
                    body_text = (await detail_page.locator("body").inner_text(timeout=3000)).lower()
                except Exception:
                    body_text = ""
                if body_text and not any(m in body_text for m in challenge_markers):
                    break
                await detail_page.wait_for_timeout(step)
                waited += step
            if waited >= timeout_ms:
                print("[warn] Challenge wait timed out after 10 minutes. Continuing anyway.")

        await anti_bot_scroll_detail_page(detail_page)
        await detail_page.wait_for_timeout(1800)
        text = await detail_page.locator("body").inner_text()
        html = await detail_page.content()
        page_title = await detail_page.title()
        image_url = await detail_page.evaluate(
            """
            () => {
                const og = document.querySelector('meta[property="og:image"]');
                if (og && og.content) return og.content;
              const tw = document.querySelector('meta[name="twitter:image"]');
              if (tw && tw.content) return tw.content;
              const img = document.querySelector('img[src]');
              return img ? img.src : '';
            }
            """
        )
        image_url = remove_crop_param_from_image_url(image_url or "")
        low_text = text.lower()
        low_html = html.lower()
        has = ("carbarn" in low_text) or ("carbarn" in low_html)
        detail_date = extract_listing_date_from_text(text + " " + html)
        evidence = ""
        if has:
            idx = low_text.find("carbarn")
            if idx != -1:
                evidence = text[max(0, idx - 80): idx + 120].replace("\n", " ").strip()
            else:
                idx2 = low_html.find("carbarn")
                evidence = html[max(0, idx2 - 80): idx2 + 120].replace("\n", " ").strip()
        return has, evidence, detail_date, (image_url or ""), (page_title or "")
    except Exception as e:
        return False, f"detail_error: {e}", None, "", ""
    finally:
        if created_page and not keep_tab_open:
            await detail_page.close()


def value_is_empty(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and pd.isna(v):
        return True
    return str(v).strip() == ""


def row_unique_key(row: dict[str, Any]) -> str:
    for k in ["detail_url", "network_id", "card_id"]:
        v = str(row.get(k, "") or "").strip().lower()
        if v:
            return v
    return ""


def load_records_map(path: Path) -> dict[str, dict[str, Any]]:
    backup_path = path.with_suffix(path.suffix + ".bak")
    snapshots_dir = path.parent / "state" / "snapshots"

    candidates: list[Path] = []
    if path.exists():
        candidates.append(path)
    if backup_path.exists():
        candidates.append(backup_path)
    if snapshots_dir.exists():
        snap_files = sorted(
            snapshots_dir.glob(f"{path.stem}__*.csv"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        candidates.extend(snap_files[:5])

    if not candidates:
        return {}

    for candidate in candidates:
        try:
            data = _read_records_map_csv(candidate)
            if candidate != path:
                print(f"[recovery] Loaded {path.name} from {candidate.name}")
            return data
        except Exception:
            continue
    return {}


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

    os.replace(tmp_path, path)

    # Rolling snapshot history for recovery if both primary and .bak are damaged.
    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    snapshot_path = snapshots_dir / f"{path.stem}__{stamp}.csv"
    try:
        shutil.copy2(path, snapshot_path)
    except Exception:
        pass

    # Keep only latest 20 snapshots per file.
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


# Runtime binding to externalized state/persistence module for easier debugging
# and robust file-lock handling on Windows.
_target_key = state_io.target_key
load_skipped_targets_map = state_io.load_skipped_targets_map
value_is_empty = state_io.value_is_empty
row_unique_key = state_io.row_unique_key
load_records_map = state_io.load_records_map
save_records_map = state_io.save_records_map
save_skipped_targets_map = state_io.save_skipped_targets_map
upsert_record = state_io.upsert_record
remove_record = state_io.remove_record


async def run(args):
    search_url = args.search_url
    max_pages = int(args.max_pages or 0)
    if max_pages <= 0:
        print(f"[warn] Invalid --max-pages={args.max_pages}. Falling back to 20.")
        max_pages = 20
    inventory_path = Path(args.inventory_file)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "current").mkdir(parents=True, exist_ok=True)

    all_cards_path = out_dir / ALL_CARDS_FILE
    suspected_path = out_dir / SUSPECTED_FILE
    my_cars_path = out_dir / MY_CARS_FILE
    skipped_targets_path = out_dir / SKIPPED_TARGETS_FILE
    state_dir = out_dir / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    session_health_path, session_health, active_session_state = session_ops.resolve_active_session_state(args, out_dir)

    inventory_df, inventory_pairs, inventory_prices, match_mode = load_inventory_pairs(
        inventory_path,
        odometer_col_override=args.odometer_col,
        allow_price_only=args.allow_price_only,
    )
    print(f"Inventory rows loaded: {len(inventory_df)}")
    if match_mode == "price+odometer":
        print(f"Inventory exact pairs (price, odometer): {len(inventory_pairs)}")
    else:
        print(f"Inventory unique prices: {len(inventory_prices)} (price-only exact match)")

    date_from, date_to, date_col_used = extract_inventory_date_range(inventory_df)

    mode = args.workflow_mode
    verify_on_the_go = not args.no_verify_on_the_go
    explicit_search_url = bool(args.search_url.strip() and args.search_url.strip() != DEFAULT_SEARCH_URL)
    allow_cache_for_targets = not explicit_search_url
    explicit_search_url_norm = str(args.search_url or "").strip().rstrip("/").lower()
    all_cards_map = load_records_map(all_cards_path)
    suspected_map = load_records_map(suspected_path)
    my_cars_map = load_records_map(my_cars_path)
    skipped_targets_map = load_skipped_targets_map(skipped_targets_path)
    now_iso = datetime.now().isoformat(timespec="seconds")

    def _norm_target(v: Any) -> str:
        return str(v or "").strip().lower()

    def row_in_explicit_scope(row: dict[str, Any]) -> bool:
        if not explicit_search_url:
            return True
        row_url = str(row.get("search_url", "") or "").strip().rstrip("/").lower()
        if not row_url:
            return False
        return row_url == explicit_search_url_norm

    cached_cards_by_target: dict[str, list[dict[str, Any]]] = {}
    for _row in all_cards_map.values():
        tkey = _target_key(
            _row.get("inventory_year_filter", ""),
            _row.get("inventory_make_filter", ""),
            _row.get("inventory_model_filter", ""),
            _row.get("search_url", ""),
        )
        if not tkey:
            continue
        cached_cards_by_target.setdefault(tkey, []).append(dict(_row))

    def get_cached_cards_for_target(year: str, make: str, model: str, url: str) -> list[dict[str, Any]]:
        key = _target_key(year, make, model, url)
        rows = cached_cards_by_target.get(key, [])
        if not rows:
            return []
        out: list[dict[str, Any]] = []
        for r in rows:
            if (
                _norm_target(r.get("inventory_year_filter", "")) == _norm_target(year)
                and _norm_target(r.get("inventory_make_filter", "")) == _norm_target(make)
                and _norm_target(r.get("inventory_model_filter", "")) == _norm_target(model)
                and _norm_target(r.get("search_url", "")) == _norm_target(url)
            ):
                out.append(dict(r))
        return out

    def is_active_my_car_row(row: dict[str, Any]) -> bool:
        state = str(row.get("listing_state", "")).strip().lower()
        return state not in {"archived", "inactive"}

    def active_my_cars_count() -> int:
        return sum(1 for r in my_cars_map.values() if is_active_my_car_row(r))

    progress_state: dict[str, Any] = {
        "mode": mode,
        "stage": "init",
        "targets_total": 0,
        "targets_done": 0,
        "pages_done": 0,
        "pages_total_effective": 0,
        "cards_collected": 0,
        "suspected_total": 0,
        "verified_done": 0,
        "my_cars_total": active_my_cars_count(),
        "not_carbarn_total": 0,
        "skipped_targets_total": 0,
        "remaining_count": 0,
        "progress_percent": 0.0,
        "eta_seconds": None,
        "current_target_label": "",
        "current_target_index": 0,
        "current_page": 0,
        "event": "run_started",
    }
    progress_started = time.monotonic()
    progress_history: list[tuple[float, float]] = []

    def _progress_weights() -> tuple[float, float]:
        if mode == "identified-only":
            return 0.0, 1.0
        # full + not-identified-only default to weighted pipeline
        return 0.6, 0.4

    def _progress_units_done() -> float:
        cw, vw = _progress_weights()
        crawl_ratio = 1.0
        if cw > 0:
            crawl_total = max(int(progress_state.get("pages_total_effective", 0)), 1)
            crawl_done = int(progress_state.get("pages_done", 0))
            crawl_ratio = min(1.0, crawl_done / crawl_total)

        verify_ratio = 1.0
        verify_total = max(
            int(progress_state.get("verified_done", 0)) + int(progress_state.get("remaining_count", 0)),
            1,
        )
        if vw > 0:
            verify_done = int(progress_state.get("verified_done", 0))
            verify_ratio = min(1.0, verify_done / verify_total)

        total_w = cw + vw
        if total_w <= 0:
            return 0.0
        return (cw * crawl_ratio + vw * verify_ratio) / total_w

    def emit_progress(event: str, **fields: Any):
        progress_state.update(fields)
        progress_state["event"] = event

        now_t = time.monotonic()
        done_units = max(0.0, min(1.0, _progress_units_done()))
        progress_state["progress_percent"] = round(done_units * 100.0, 2)

        progress_history.append((now_t, done_units))
        cutoff = now_t - 45.0
        while len(progress_history) > 2 and progress_history[0][0] < cutoff:
            progress_history.pop(0)

        eta_seconds: int | None = None
        if len(progress_history) >= 2 and done_units < 0.999:
            t0, u0 = progress_history[0]
            t1, u1 = progress_history[-1]
            du = u1 - u0
            dt = t1 - t0
            if du > 0 and dt > 0:
                rate = du / dt
                rem = 1.0 - done_units
                eta_seconds = int(rem / rate) if rate > 0 else None

        progress_state["eta_seconds"] = eta_seconds
        progress_state["my_cars_total"] = active_my_cars_count()
        progress_state["skipped_targets_total"] = len(skipped_targets_map)
        payload = {
            **progress_state,
            "elapsed_seconds": int(max(0.0, now_t - progress_started)),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        print(f"{PROGRESS_PREFIX}{json.dumps(payload, separators=(',', ':'))}", flush=True)

    def persist_all():
        save_records_map(
            all_cards_path,
            all_cards_map,
            preferred_columns=[
                "title", "price_text", "price", "odometer_text", "odometer_km",
                "detail_url", "network_id", "card_id",
                "inventory_year_filter", "inventory_make_filter", "inventory_model_filter",
                "search_url", "updated_at",
            ],
            state_dir=state_dir,
        )
        save_records_map(
            suspected_path,
            suspected_map,
            preferred_columns=[
                "title", "price_text", "price", "odometer_text", "odometer_km",
                "detail_url", "network_id", "card_id",
                "inventory_year_filter", "inventory_make_filter", "inventory_model_filter",
                "search_url", "verification_status", "verified_at",
                "carbarn_found_in_detail", "carbarn_evidence", "detail_date",
                "first_image_url", "detail_page_title", "in_date_range", "updated_at",
            ],
            state_dir=state_dir,
        )
        save_records_map(
            my_cars_path,
            my_cars_map,
            preferred_columns=[
                "title", "price_text", "price", "odometer_text", "odometer_km",
                "detail_url", "network_id", "card_id",
                "inventory_year_filter", "inventory_make_filter", "inventory_model_filter",
                "search_url", "verified_at", "detail_date", "first_image_url", "detail_page_title",
                "carbarn_evidence", "in_date_range", "updated_at",
            ],
            state_dir=state_dir,
        )
        save_skipped_targets_map(
            skipped_targets_path,
            skipped_targets_map,
            state_dir=state_dir,
        )

    def is_suspect(card: dict[str, Any]) -> bool:
        price = card.get("price")
        odo = card.get("odometer_km")
        if price is None:
            return False
        try:
            iprice = int(price)
        except Exception:
            return False
        if match_mode == "price+odometer":
            if odo is None:
                return False
            try:
                iodo = int(odo)
            except Exception:
                return False
            return (iprice, iodo) in inventory_pairs
        return iprice in inventory_prices

    def mark_target_skipped(
        *,
        year: str,
        make: str,
        model: str,
        url: str,
        total_pages: int | None,
        reason: str,
    ):
        key = _target_key(year, make, model, url)
        skipped_targets_map[key] = {
            "target_key": key,
            "year": str(year or "").strip(),
            "make": str(make or "").strip(),
            "model": str(model or "").strip(),
            "search_url": str(url or "").strip(),
            "total_pages": str(total_pages or ""),
            "skip_reason": str(reason or "too_many_cards"),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        persist_all()

    async def verify_and_persist(
        context,
        src_row: dict[str, Any],
        reason: str,
        keep_page=None,
        keeper_mode: bool = False,
    ):
        nonlocal now_iso
        url = str(src_row.get("detail_url", "") or "").strip()
        emit_progress("verify_start", stage="verify", detail_url=url, verify_reason=reason)
        row_for_suspect = dict(src_row)
        if not url:
            row_for_suspect["verification_status"] = "error"
            row_for_suspect["carbarn_evidence"] = "missing_detail_url"
            upsert_record(suspected_map, row_for_suspect, updated_at=now_iso)
            persist_all()
            progress_state["verified_done"] = int(progress_state.get("verified_done", 0)) + 1
            progress_state["remaining_count"] = max(
                0,
                int(progress_state.get("suspected_total", 0)) - int(progress_state.get("verified_done", 0)),
            )
            emit_progress("verify_end", stage="verify", detail_url=url, verify_status="error")
            return

        has_carbarn, evidence, detail_date, image_url, detail_title = await verify_carbarn_on_detail(
            context,
            url,
            page=keep_page,
            keep_tab_open=keeper_mode,
        )
        in_date_range = True
        if date_from or date_to:
            if detail_date is None:
                in_date_range = False
            else:
                if date_from and detail_date < date_from:
                    in_date_range = False
                if date_to and detail_date > date_to:
                    in_date_range = False

        row_for_suspect["detail_date"] = str(detail_date) if detail_date else ""
        row_for_suspect["carbarn_found_in_detail"] = str(bool(has_carbarn))
        row_for_suspect["carbarn_evidence"] = evidence
        row_for_suspect["first_image_url"] = image_url
        row_for_suspect["detail_page_title"] = detail_title
        row_for_suspect["in_date_range"] = str(bool(in_date_range))
        row_for_suspect["verified_at"] = datetime.now().isoformat(timespec="seconds")
        row_for_suspect["verified_reason"] = reason

        status = "not_carbarn"
        if has_carbarn and in_date_range:
            status = "my_car"
        row_for_suspect["verification_status"] = status
        key = upsert_record(suspected_map, row_for_suspect, updated_at=now_iso)
        if key:
            if status == "my_car":
                row_for_suspect["listing_state"] = "active"
                row_for_suspect["inactive_reason"] = ""
                upsert_record(my_cars_map, row_for_suspect, updated_at=now_iso)
            else:
                existing_my = dict(my_cars_map.get(key, {}))
                if existing_my:
                    # Do not hard-delete previously identified cars. Keep them archived for rechecks.
                    existing_my["listing_state"] = "archived"
                    existing_my["inactive_reason"] = status
                    existing_my["inactive_at"] = datetime.now().isoformat(timespec="seconds")
                    existing_my["verification_status"] = status
                    existing_my["carbarn_evidence"] = evidence
                    existing_my["verified_at"] = row_for_suspect.get("verified_at", "")
                    existing_my["detail_page_title"] = detail_title
                    existing_my["first_image_url"] = image_url
                    upsert_record(my_cars_map, existing_my, updated_at=now_iso)

        progress_state["verified_done"] = int(progress_state.get("verified_done", 0)) + 1
        if status == "not_carbarn":
            progress_state["not_carbarn_total"] = int(progress_state.get("not_carbarn_total", 0)) + 1
        progress_state["remaining_count"] = max(
            0,
            int(progress_state.get("suspected_total", 0)) - int(progress_state.get("verified_done", 0)),
        )
        persist_all()
        emit_progress("verify_end", stage="verify", detail_url=url, verify_status=status)

    print("=" * 70)
    print("Find My Cars: Incremental workflow")
    print("=" * 70)
    print(f"Workflow mode: {mode}")
    if explicit_search_url:
        print("Target mode: explicit --search-url (live fetch, cache bypassed)")
    print(f"Verify suspects on-the-go: {'ON' if verify_on_the_go else 'OFF'}")
    print(f"Persistent files: {all_cards_path.name}, {suspected_path.name}, {my_cars_path.name}")
    if date_from or date_to:
        print(
            f"Date range from inventory ({date_col_used}): "
            f"{date_from or '...'} to {date_to or '...'}"
        )
    else:
        print("Date range from inventory: not found (no parseable dates).")
    print(f"Inventory: {inventory_path}")
    emit_progress("run_started", stage="init")

    async with async_playwright() as p:
        browser = await p.chromium.launch(**_playwright_launch_kwargs(bool(args.headless)))
        context_kwargs: dict[str, Any] = {
            "viewport": {"width": 1600, "height": 1000},
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "locale": "en-AU",
        }
        if active_session_state and active_session_state.exists():
            context_kwargs["storage_state"] = str(active_session_state)
            print(f"Using storage state: {active_session_state}")
        context = await browser.new_context(**context_kwargs)
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        page = await context.new_page()
        if mode in {"identified-only", "not-identified-only"}:
            await page.close()

        if mode == "identified-only":
            rows = [r for r in my_cars_map.values() if is_active_my_car_row(r)]
            print(f"Re-verifying already identified links: {len(rows)}")
            emit_progress(
                "mode_started",
                stage="verify",
                targets_total=len(rows),
                targets_done=0,
                remaining_count=len(rows),
                pages_total_effective=0,
                pages_done=0,
            )
            if rows and not args.headless:
                keeper_page = await context.new_page()
                first = rows[0]
                print(f"Keeper tab (1/{len(rows)}): {first.get('detail_url', '')}")
                emit_progress(
                    "target_started",
                    stage="verify",
                    current_target_label=first.get("detail_url", ""),
                    current_target_index=1,
                )
                await verify_and_persist(
                    context,
                    first,
                    reason="identified-only",
                    keep_page=keeper_page,
                    keeper_mode=True,
                )
                emit_progress("target_finished", stage="verify", targets_done=1, remaining_count=max(0, len(rows) - 1))
                for i, r in enumerate(rows[1:], 2):
                    print(f"Verify identified {i}/{len(rows)}: {r.get('detail_url', '')}")
                    emit_progress(
                        "target_started",
                        stage="verify",
                        current_target_label=r.get("detail_url", ""),
                        current_target_index=i,
                    )
                    await verify_and_persist(context, r, reason="identified-only")
                    emit_progress(
                        "target_finished",
                        stage="verify",
                        targets_done=i,
                        remaining_count=max(0, len(rows) - i),
                    )
            else:
                for i, r in enumerate(rows, 1):
                    print(f"Verify identified {i}/{len(rows)}: {r.get('detail_url', '')}")
                    emit_progress(
                        "target_started",
                        stage="verify",
                        current_target_label=r.get("detail_url", ""),
                        current_target_index=i,
                    )
                    await verify_and_persist(context, r, reason="identified-only")
                    emit_progress(
                        "target_finished",
                        stage="verify",
                        targets_done=i,
                        remaining_count=max(0, len(rows) - i),
                    )

        elif mode == "not-identified-only":
            persist_all()
            pending_by_key: dict[str, dict[str, Any]] = {}
            for r in suspected_map.values():
                if not row_in_explicit_scope(r):
                    continue
                detail_url = str(r.get("detail_url", "") or "").strip()
                if not detail_url:
                    continue
                key = row_unique_key(r)
                status = str(r.get("verification_status", "")).strip().lower()
                if key in my_cars_map:
                    continue
                if status in {"my_car"}:
                    continue
                pending_by_key[key] = r

            pending = list(pending_by_key.values())
            scope_label = args.search_url.strip() if explicit_search_url else "all previously suspected links"
            print(f"Not-identified-only scope: {scope_label}")
            print(f"Verifying not-identified suspects (no crawling): {len(pending)}")
            progress_state["suspected_total"] = len(pending)
            progress_state["remaining_count"] = len(pending)
            emit_progress(
                "mode_started",
                stage="verify",
                targets_total=len(pending),
                targets_done=0,
                pages_done=0,
                pages_total_effective=0,
                remaining_count=len(pending),
            )

            if pending:
                verify_page = await context.new_page()
                try:
                    for i, r in enumerate(pending, 1):
                        label = r.get("detail_url", "")
                        print(f"Verify suspect {i}/{len(pending)}: {label}")
                        emit_progress(
                            "target_started",
                            stage="verify",
                            current_target_label=label,
                            current_target_index=i,
                        )
                        await verify_and_persist(
                            context,
                            r,
                            reason="not-identified-only",
                            keep_page=verify_page,
                        )
                        emit_progress(
                            "target_finished",
                            stage="verify",
                            targets_done=i,
                            remaining_count=max(0, len(pending) - i),
                        )
                finally:
                    try:
                        await verify_page.close()
                    except Exception:
                        pass

        else:
            skipped_targets_map.clear()
            persist_all()
            targets: list[tuple[str, str, str, str]] = []
            if args.search_url.strip() == DEFAULT_SEARCH_URL:
                targets = build_inventory_search_targets(
                    inventory_df,
                    location_slug=FIXED_LOCATION_SLUG,
                )
                if not targets:
                    raise ValueError(
                        "Could not build Make/Model URLs from inventory. "
                        "Ensure inventory has Make and Model columns."
                    )
            else:
                targets = [("", "", "", args.search_url.strip())]

            print(f"Search targets: {len(targets)}")
            emit_progress(
                "mode_started",
                stage="crawl",
                targets_total=len(targets),
                targets_done=0,
                pages_done=0,
                pages_total_effective=len(targets) * max_pages,
                remaining_count=0,
            )
            processed_verify_keys: set[str] = set()
            verify_page = await context.new_page()

            challenge_failures = 0
            for idx, (yr, mk, md, url) in enumerate(targets, 1):
                label = " ".join([x for x in [yr, mk, md] if x]).strip() or url
                print(f"\n[{idx}/{len(targets)}] Crawling: {label}")
                print(f"URL: {url}")
                emit_progress(
                    "target_started",
                    stage="crawl",
                    current_target_label=label,
                    current_target_index=idx,
                )

                async def on_page_cards(cards_part: list[dict], page_num: int):
                    nonlocal now_iso
                    now_iso = datetime.now().isoformat(timespec="seconds")
                    progress_state["pages_done"] = int(progress_state.get("pages_done", 0)) + 1
                    progress_state["cards_collected"] = int(progress_state.get("cards_collected", 0)) + len(cards_part)
                    emit_progress(
                        "page_collected",
                        stage="crawl",
                        current_page=page_num,
                        current_target_index=idx,
                    )
                    for c in cards_part:
                        c = dict(c)
                        c["inventory_year_filter"] = yr
                        c["inventory_make_filter"] = mk
                        c["inventory_model_filter"] = md
                        c["search_url"] = url

                        key = upsert_record(all_cards_map, c, updated_at=now_iso)
                        if not key:
                            continue

                        if not is_suspect(c):
                            continue

                        c["suspect_match_mode"] = match_mode
                        existing = suspected_map.get(key, {})
                        if value_is_empty(existing.get("verification_status", "")):
                            c["verification_status"] = "pending"
                        suspect_key = upsert_record(suspected_map, c, updated_at=now_iso) or key
                        if suspect_key and value_is_empty(existing.get("verification_status", "")):
                            progress_state["suspected_total"] = int(progress_state.get("suspected_total", 0)) + 1
                            emit_progress("suspect_discovered", stage="crawl", suspected_total=progress_state["suspected_total"])

                        if not verify_on_the_go:
                            continue
                        if suspect_key in processed_verify_keys:
                            continue
                        if suspect_key in my_cars_map:
                            continue

                        existing_status = str(suspected_map.get(suspect_key, {}).get("verification_status", "")).strip().lower()
                        if existing_status in {"my_car"}:
                            continue

                        processed_verify_keys.add(suspect_key)
                        print(f"  Verify on-the-go (page {page_num}): {c.get('detail_url', '')}")
                        await verify_and_persist(
                            context,
                            suspected_map.get(suspect_key, c),
                            reason="full-on-the-go",
                            keep_page=verify_page,
                        )
                        progress_state["remaining_count"] = max(
                            0,
                            int(progress_state.get("suspected_total", 0)) - int(progress_state.get("verified_done", 0)),
                        )

                    persist_all()

                cached_rows = get_cached_cards_for_target(yr, mk, md, url) if allow_cache_for_targets else []
                if cached_rows:
                    print(
                        f"[cache] Using cached cards for target ({idx}/{len(targets)}): "
                        f"{len(cached_rows)} rows"
                    )
                    await on_page_cards(cached_rows, 0)
                    meta = {"skipped": False, "cached": True}
                else:
                    _, meta = await collect_all_cards(
                        page,
                        search_url=url,
                        max_pages=max_pages,
                        on_page_cards=on_page_cards,
                    )
                    challenge_failures, needs_cooldown = session_ops.register_challenge_timeout(meta, challenge_failures)
                    if needs_cooldown:
                        cooldown_s = session_ops.compute_cooldown_seconds(
                            int(args.challenge_cooldown_base_seconds),
                            challenge_failures,
                        )
                        print(f"[session] Challenge/timeout suspected. Cooling down {cooldown_s}s before next target.")
                        await page.wait_for_timeout(cooldown_s * 1000)
                if bool(meta.get("skipped")):
                    mark_target_skipped(
                        year=yr,
                        make=mk,
                        model=md,
                        url=url,
                        total_pages=meta.get("total_pages"),
                        reason=str(meta.get("skip_reason") or "too_many_cards"),
                    )
                    emit_progress(
                        "target_skipped",
                        stage="crawl",
                        current_target_label=label,
                        current_target_index=idx,
                    )
                emit_progress(
                    "target_finished",
                    stage="crawl",
                    targets_done=idx,
                    current_target_label=label,
                    current_target_index=idx,
                )

            if not verify_on_the_go:
                pending = []
                for r in suspected_map.values():
                    if not row_in_explicit_scope(r):
                        continue
                    key = row_unique_key(r)
                    status = str(r.get("verification_status", "")).strip().lower()
                    if key in my_cars_map:
                        continue
                    if status in {"my_car"}:
                        continue
                    pending.append(r)
                print(f"Post-crawl suspect verification (on-the-go OFF): {len(pending)}")
                emit_progress("mode_started", stage="verify", remaining_count=len(pending))
                for i, r in enumerate(pending, 1):
                    print(f"Verify suspect {i}/{len(pending)}: {r.get('detail_url', '')}")
                    await verify_and_persist(context, r, reason="full-post-crawl", keep_page=verify_page)
                    emit_progress("verify_queue_progress", stage="verify", remaining_count=max(0, len(pending) - i))
            try:
                await verify_page.close()
            except Exception:
                pass

        persist_all()
        if active_session_state and args.save_storage_state_on_exit:
            try:
                await context.storage_state(path=str(active_session_state))
                print(f"Saved refreshed storage state: {active_session_state}")
            except Exception as e:
                print(f"[warn] Could not save storage state: {e}")

        if active_session_state:
            session_ops.update_session_health_after_run(
                session_health_path=session_health_path,
                session_health=session_health,
                active_session_state=active_session_state,
                mode=mode,
                pages_done=int(progress_state.get("pages_done", 0)),
                base_cooldown_seconds=int(args.challenge_cooldown_base_seconds),
            )
            print(f"Session health updated: {session_health_path}")
        emit_progress("run_completed", stage="finalize", remaining_count=0, progress_percent=100.0)

        print("\n" + "=" * 70)
        print("Completed")
        print("=" * 70)
        print(f"All cards: {all_cards_path}")
        print(f"Suspected cars: {suspected_path}")
        print(f"My cars: {my_cars_path}")
        print(f"Skipped targets: {skipped_targets_path}")
        print(f"Unique all cards: {len(all_cards_map)}")
        print(f"Unique suspected: {len(suspected_map)}")
        print(f"Unique my cars: {len(my_cars_map)}")
        print(f"Skipped targets count: {len(skipped_targets_map)}")

        if not args.no_keep_open:
            if not args.headless and mode == "identified-only":
                try:
                    page_count = sum(len(ctx.pages) for ctx in browser.contexts)
                except Exception:
                    page_count = 0
                if page_count == 0:
                    try:
                        keep_page = await context.new_page()
                        await keep_page.goto("about:blank")
                    except Exception:
                        pass
            print("\nBrowser will stay open. Close browser manually when done.")
            while True:
                try:
                    page_count = sum(len(ctx.pages) for ctx in browser.contexts)
                    if page_count == 0:
                        break
                    await asyncio.sleep(1.0)
                except Exception:
                    break
        else:
            await context.close()
            await browser.close()


def build_parser():
    parser = argparse.ArgumentParser(description="Find your cars from Carsales by exact inventory match.")
    parser.add_argument("--search-url", default=DEFAULT_SEARCH_URL, help="Carsales listing URL")
    parser.add_argument("--inventory-file", default=DEFAULT_INVENTORY, help="Inventory file (csv/xlsx)")
    parser.add_argument(
        "--odometer-col",
        default=None,
        help="Optional explicit inventory odometer column name (example: Odometer)",
    )
    parser.add_argument(
        "--allow-price-only",
        action="store_true",
        help="Allow fallback matching by exact price only when odometer column is missing",
    )
    parser.add_argument("--out-dir", default="find_my_cars_output", help="Output directory")
    parser.add_argument("--max-pages", type=int, default=20, help="Pagination safety limit")
    parser.add_argument(
        "--workflow-mode",
        choices=["full", "identified-only", "not-identified-only"],
        default="full",
        help=(
            "Workflow mode: full crawl, only recheck identified links, "
            "or refresh make/model targets then process unresolved suspects"
        ),
    )
    parser.add_argument(
        "--no-verify-on-the-go",
        action="store_true",
        help="When set, verify suspects after crawl instead of during page collection",
    )
    browser_mode = parser.add_mutually_exclusive_group()
    browser_mode.add_argument("--headless", dest="headless", action="store_true", help="Run browser in headless mode")
    browser_mode.add_argument("--headed", dest="headless", action="store_false", help="Run browser with visible UI")
    parser.set_defaults(headless=_default_headless_mode())
    parser.add_argument("--no-keep-open", action="store_true", help="Auto close browser when finished")
    session_ops.add_session_args(parser)
    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    asyncio.run(run(args))

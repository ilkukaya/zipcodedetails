"""
Script 05 — Generate index JSON files
Reads data/processed/all_zips.csv and produces:
  - data/state_index.json
  - data/city_index.json
  - data/search_index.json
  - data/popular_zips.json
  - public/search_index.json  (copy for the frontend)
"""

import json
import logging
import math
import os
import shutil
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
CSV_PATH = PROJECT_ROOT / "data" / "processed" / "all_zips.csv"
DATA_DIR = PROJECT_ROOT / "data"
PUBLIC_DIR = PROJECT_ROOT / "public"

STATE_INDEX_PATH = DATA_DIR / "state_index.json"
CITY_INDEX_PATH = DATA_DIR / "city_index.json"
SEARCH_INDEX_PATH = DATA_DIR / "search_index.json"
POPULAR_ZIPS_PATH = DATA_DIR / "popular_zips.json"
PUBLIC_SEARCH_INDEX_PATH = PUBLIC_DIR / "search_index.json"

POPULAR_ZIPS_COUNT = 100


def _is_nan(value) -> bool:
    try:
        return math.isnan(value)
    except (TypeError, ValueError):
        return False


def safe_str(val, default=None):
    """Return a stripped string or *default* if the value is NaN/None/blank."""
    if _is_nan(val) or val is None:
        return default
    s = str(val).strip()
    return s if s else default


def safe_int(val, default=0) -> int:
    if _is_nan(val) or val is None:
        return default
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def load_csv(csv_path: Path) -> pd.DataFrame:
    logger.info("Reading CSV: %s", csv_path)
    df = pd.read_csv(
        csv_path,
        dtype={"zip": str, "cbsa_code": str},
        on_bad_lines="warn",
    )
    # Ensure ZIP codes are zero-padded to 5 digits
    df["zip"] = df["zip"].str.strip().str.zfill(5)
    logger.info("Loaded %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# Index builders
# ---------------------------------------------------------------------------

def build_state_index(df: pd.DataFrame) -> dict:
    """
    Build a dict keyed by state abbreviation.
    Each entry:
      name, abbreviation, zip_count, population (sum), zips (list), cities (unique list)
    """
    logger.info("Building state_index …")
    index: dict = {}

    for _, row in df.iterrows():
        state = safe_str(row.get("state"))
        if not state:
            continue

        state_full = safe_str(row.get("state_full"), default=state)
        zip_code = safe_str(row.get("zip"))
        city = safe_str(row.get("city"))
        pop = safe_int(row.get("population"), default=0)

        if state not in index:
            index[state] = {
                "name": state_full,
                "abbreviation": state,
                "zip_count": 0,
                "population": 0,
                "zips": [],
                "cities": [],
                "_city_set": set(),   # removed before serialisation
            }

        entry = index[state]
        entry["zip_count"] += 1
        entry["population"] += pop

        if zip_code:
            entry["zips"].append(zip_code)

        if city and city not in entry["_city_set"]:
            entry["_city_set"].add(city)
            entry["cities"].append(city)

    # Clean up internal helper sets
    for entry in index.values():
        del entry["_city_set"]

    logger.info("state_index built — %d states", len(index))
    return index


def build_city_index(df: pd.DataFrame) -> dict:
    """
    Build a dict keyed by "City_ST".
    Each entry: city, state, state_full, county, population (sum), zips (list)
    """
    logger.info("Building city_index …")
    index: dict = {}

    for _, row in df.iterrows():
        city = safe_str(row.get("city"))
        state = safe_str(row.get("state"))
        if not city or not state:
            continue

        key = f"{city}_{state}"
        state_full = safe_str(row.get("state_full"), default=state)
        county = safe_str(row.get("county"))
        zip_code = safe_str(row.get("zip"))
        pop = safe_int(row.get("population"), default=0)

        if key not in index:
            index[key] = {
                "city": city,
                "state": state,
                "state_full": state_full,
                "county": county,
                "population": 0,
                "zips": [],
            }

        entry = index[key]
        entry["population"] += pop
        if zip_code:
            entry["zips"].append(zip_code)

    logger.info("city_index built — %d city/state combos", len(index))
    return index


def build_search_index(df: pd.DataFrame) -> list:
    """
    Build a flat array of {zip, city, state} dicts for client-side search.
    """
    logger.info("Building search_index …")
    records = []
    for _, row in df.iterrows():
        zip_code = safe_str(row.get("zip"))
        city = safe_str(row.get("city"))
        state = safe_str(row.get("state"))
        if not zip_code:
            continue
        records.append({"zip": zip_code, "city": city, "state": state})
    logger.info("search_index built — %d entries", len(records))
    return records


def build_popular_zips(df: pd.DataFrame, top_n: int = 100) -> list:
    """
    Return the top *top_n* ZIP codes ranked by population descending.
    Each record has: zip, city, state, population.
    """
    logger.info("Building popular_zips (top %d) …", top_n)

    pop_df = df[["zip", "city", "state", "population"]].copy()
    pop_df["population"] = pd.to_numeric(pop_df["population"], errors="coerce").fillna(0).astype(int)
    pop_df = pop_df.sort_values("population", ascending=False).head(top_n)

    records = []
    for _, row in pop_df.iterrows():
        records.append({
            "zip": safe_str(row["zip"]),
            "city": safe_str(row["city"]),
            "state": safe_str(row["state"]),
            "population": int(row["population"]),
        })

    logger.info("popular_zips built — %d entries", len(records))
    return records


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def write_json(path: Path, data, label: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    logger.info("Wrote %s → %s", label, path)


def copy_search_index(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    logger.info("Copied search_index.json → %s", dst)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def generate_indexes(csv_path: Path) -> None:
    df = load_csv(csv_path)

    state_index = build_state_index(df)
    city_index = build_city_index(df)
    search_index = build_search_index(df)
    popular_zips = build_popular_zips(df, top_n=POPULAR_ZIPS_COUNT)

    write_json(STATE_INDEX_PATH, state_index, "state_index.json")
    write_json(CITY_INDEX_PATH, city_index, "city_index.json")
    write_json(SEARCH_INDEX_PATH, search_index, "search_index.json")
    write_json(POPULAR_ZIPS_PATH, popular_zips, "popular_zips.json")
    copy_search_index(SEARCH_INDEX_PATH, PUBLIC_SEARCH_INDEX_PATH)

    logger.info("Script 05 complete.")


if __name__ == "__main__":
    if not CSV_PATH.exists():
        logger.error("Input CSV not found: %s", CSV_PATH)
        sys.exit(1)

    generate_indexes(CSV_PATH)

"""
Script 04 — Generate per-ZIP JSON files
Reads data/processed/all_zips.csv and writes one JSON file per ZIP code to
data/zips/{zip}.json.
"""

import json
import logging
import math
import os
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
# Paths (relative to the project root, two levels up from this script)
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
CSV_PATH = PROJECT_ROOT / "data" / "processed" / "all_zips.csv"
OUTPUT_DIR = PROJECT_ROOT / "data" / "zips"

# Fields that should fall back to 0.0 (not null) when the source value is NaN
ZERO_FALLBACK_FIELDS = {"land_area_sqmi", "water_area_sqmi"}


def _is_nan(value) -> bool:
    """Return True if *value* is a float NaN (handles non-numeric types safely)."""
    try:
        return math.isnan(value)
    except (TypeError, ValueError):
        return False


def parse_surrounding_zips(raw) -> list:
    """Convert a pipe-delimited string of ZIP codes into a list of strings."""
    if _is_nan(raw) or raw is None or str(raw).strip() == "":
        return []
    return [z.strip() for z in str(raw).split("|") if z.strip()]


def build_record(row: pd.Series) -> dict:
    """Build the JSON-serialisable dict for a single ZIP row."""

    # --- Zero-pad the ZIP to 5 characters ------------------------------------
    zip_code = str(row["zip"]).strip().zfill(5)

    # --- Nullable integer fields ---------------------------------------------
    def nullable_int(field: str):
        val = row.get(field)
        if _is_nan(val) or val is None:
            return None
        try:
            return int(val)
        except (TypeError, ValueError):
            return None

    # --- Float fields that fall back to 0.0 ----------------------------------
    def zero_float(field: str) -> float:
        val = row.get(field)
        if _is_nan(val) or val is None:
            return 0.0
        try:
            return float(val)
        except (TypeError, ValueError):
            return 0.0

    # --- Generic float (may be None) -----------------------------------------
    def nullable_float(field: str):
        val = row.get(field)
        if _is_nan(val) or val is None:
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    # --- String helper -------------------------------------------------------
    def str_or_none(field: str):
        val = row.get(field)
        if _is_nan(val) or val is None:
            return None
        return str(val).strip() or None

    # --- Boolean dst ---------------------------------------------------------
    dst_val = row.get("dst")
    if isinstance(dst_val, bool):
        dst = dst_val
    elif _is_nan(dst_val) or dst_val is None:
        dst = None
    else:
        dst = str(dst_val).strip().lower() in ("1", "true", "yes")

    return {
        "zip": zip_code,
        "city": str_or_none("city"),
        "state": str_or_none("state"),
        "state_full": str_or_none("state_full"),
        "county": str_or_none("county"),
        "cbsa": str_or_none("cbsa"),
        "cbsa_code": str_or_none("cbsa_code"),
        "land_area_sqmi": zero_float("land_area_sqmi"),
        "water_area_sqmi": zero_float("water_area_sqmi"),
        "lat": nullable_float("lat"),
        "lng": nullable_float("lng"),
        "timezone": str_or_none("timezone"),
        "dst": dst,
        "zip_type": str_or_none("zip_type"),
        "surrounding_zips": parse_surrounding_zips(row.get("surrounding_zips")),
    }


def generate_zip_jsons(csv_path: Path, output_dir: Path) -> int:
    """Read the CSV and write one JSON file per ZIP. Returns the number written."""
    logger.info("Reading CSV: %s", csv_path)
    df = pd.read_csv(
        csv_path,
        dtype={"zip": str, "cbsa_code": str},
        on_bad_lines="warn",
    )
    total = len(df)
    logger.info("Loaded %d rows", total)

    output_dir.mkdir(parents=True, exist_ok=True)

    written = 0
    for idx, row in df.iterrows():
        record = build_record(row)
        zip_code = record["zip"]
        out_path = output_dir / f"{zip_code}.json"
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(record, fh, indent=2, ensure_ascii=False)
        written += 1

        if written % 5000 == 0:
            logger.info("Progress: %d / %d records written (%.1f%%)",
                        written, total, written / total * 100)

    logger.info("Done. Wrote %d JSON files to %s", written, output_dir)
    return written


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if not CSV_PATH.exists():
        logger.error("Input CSV not found: %s", CSV_PATH)
        sys.exit(1)

    count = generate_zip_jsons(CSV_PATH, OUTPUT_DIR)
    logger.info("Script 04 complete — %d files generated.", count)

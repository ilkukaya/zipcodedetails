"""
Script 06 — Validate generated JSON files
Reads all ZIP JSON files plus state_index.json and city_index.json and
produces a summary report.  Exits with code 1 if any critical fields are
missing from any ZIP file.
"""

import json
import logging
import sys
from collections import defaultdict
from pathlib import Path

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
DATA_DIR = PROJECT_ROOT / "data"
ZIPS_DIR = DATA_DIR / "zips"
STATE_INDEX_PATH = DATA_DIR / "state_index.json"
CITY_INDEX_PATH = DATA_DIR / "city_index.json"

# ---------------------------------------------------------------------------
# Field definitions
# ---------------------------------------------------------------------------
REQUIRED_FIELDS = [
    "zip", "city", "state", "state_full", "county",
    "cbsa", "cbsa_code",
    "population", "median_household_income",
    "land_area_sqmi", "water_area_sqmi",
    "lat", "lng",
    "timezone", "dst", "zip_type", "surrounding_zips",
]

# Exits with code 1 if any of these are missing
CRITICAL_FIELDS = {"zip", "state", "lat", "lng"}

STATE_INDEX_REQUIRED = {"name", "abbreviation", "zip_count", "population", "zips", "cities"}
CITY_INDEX_REQUIRED = {"city", "state", "state_full", "county", "population", "zips"}


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def load_json_file(path: Path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def validate_zip_files(zips_dir: Path):
    """
    Validate every *.json file in *zips_dir*.

    Returns a dict with:
      total_files, missing_fields, null_fields, no_population, no_income,
      critical_missing (set of zip codes with critical fields absent)
    """
    zip_files = sorted(zips_dir.glob("*.json"))
    total = len(zip_files)
    logger.info("Found %d ZIP JSON files in %s", total, zips_dir)

    missing_fields: dict[str, int] = defaultdict(int)   # field → count of files missing it
    null_fields: dict[str, int] = defaultdict(int)       # field → count of files where value is null
    no_population: list[str] = []
    no_income: list[str] = []
    critical_missing_zips: set[str] = set()

    for path in zip_files:
        try:
            record = load_json_file(path)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Could not read %s: %s", path.name, exc)
            continue

        zip_id = record.get("zip", path.stem)

        # Check for missing and null fields
        for field in REQUIRED_FIELDS:
            if field not in record:
                missing_fields[field] += 1
                if field in CRITICAL_FIELDS:
                    critical_missing_zips.add(zip_id)
            elif record[field] is None:
                null_fields[field] += 1
                if field in CRITICAL_FIELDS:
                    critical_missing_zips.add(zip_id)

        # Population
        pop = record.get("population")
        if pop is None or pop == 0:
            no_population.append(zip_id)

        # Income
        income = record.get("median_household_income")
        if income is None:
            no_income.append(zip_id)

    return {
        "total_files": total,
        "missing_fields": dict(missing_fields),
        "null_fields": dict(null_fields),
        "no_population": no_population,
        "no_income": no_income,
        "critical_missing_zips": critical_missing_zips,
    }


def validate_state_index(path: Path) -> dict:
    """Returns {"ok": bool, "issues": list[str]}"""
    issues = []
    if not path.exists():
        return {"ok": False, "issues": [f"File not found: {path}"]}

    try:
        data = load_json_file(path)
    except (json.JSONDecodeError, OSError) as exc:
        return {"ok": False, "issues": [f"Cannot read file: {exc}"]}

    if not isinstance(data, dict):
        return {"ok": False, "issues": ["Top-level structure is not a JSON object"]}

    entry_count = len(data)
    for key, entry in data.items():
        if not isinstance(entry, dict):
            issues.append(f"Entry for '{key}' is not a JSON object")
            continue
        for field in STATE_INDEX_REQUIRED:
            if field not in entry:
                issues.append(f"Entry '{key}' missing field '{field}'")

    return {
        "ok": len(issues) == 0,
        "entry_count": entry_count,
        "issues": issues,
    }


def validate_city_index(path: Path) -> dict:
    """Returns {"ok": bool, "issues": list[str]}"""
    issues = []
    if not path.exists():
        return {"ok": False, "issues": [f"File not found: {path}"]}

    try:
        data = load_json_file(path)
    except (json.JSONDecodeError, OSError) as exc:
        return {"ok": False, "issues": [f"Cannot read file: {exc}"]}

    if not isinstance(data, dict):
        return {"ok": False, "issues": ["Top-level structure is not a JSON object"]}

    entry_count = len(data)
    for key, entry in data.items():
        if not isinstance(entry, dict):
            issues.append(f"Entry for '{key}' is not a JSON object")
            continue
        for field in CITY_INDEX_REQUIRED:
            if field not in entry:
                issues.append(f"Entry '{key}' missing field '{field}'")

    return {
        "ok": len(issues) == 0,
        "entry_count": entry_count,
        "issues": issues,
    }


# ---------------------------------------------------------------------------
# Report printer
# ---------------------------------------------------------------------------

def print_separator(char: str = "-", width: int = 60) -> None:
    print(char * width)


def print_report(zip_results: dict, state_result: dict, city_result: dict) -> bool:
    """Print the summary report. Returns True if validation passed (no critical errors)."""
    print_separator("=")
    print("ZIP CODE DATA VALIDATION REPORT")
    print_separator("=")

    total = zip_results["total_files"]
    print(f"\nTotal ZIP JSON files examined : {total}")

    # --- Missing fields -------------------------------------------------------
    print("\n--- Missing fields (field absent from record) ---")
    missing = zip_results["missing_fields"]
    if not missing:
        print("  (none)")
    else:
        for field, count in sorted(missing.items(), key=lambda x: -x[1]):
            pct = count / total * 100 if total else 0
            flag = "  [CRITICAL]" if field in CRITICAL_FIELDS else ""
            print(f"  {field:<30} {count:>6} ({pct:.1f}%){flag}")

    # --- Null fields ----------------------------------------------------------
    print("\n--- Null values per field ---")
    nulls = zip_results["null_fields"]
    if not nulls:
        print("  (none)")
    else:
        for field, count in sorted(nulls.items(), key=lambda x: -x[1]):
            pct = count / total * 100 if total else 0
            flag = "  [CRITICAL]" if field in CRITICAL_FIELDS else ""
            print(f"  {field:<30} {count:>6} ({pct:.1f}%){flag}")

    # --- No population / no income -------------------------------------------
    no_pop = zip_results["no_population"]
    no_inc = zip_results["no_income"]
    print(f"\n--- ZIP codes with no population data : {len(no_pop)} ---")
    if no_pop:
        preview = ", ".join(no_pop[:20])
        suffix = f"  … and {len(no_pop) - 20} more" if len(no_pop) > 20 else ""
        print(f"  {preview}{suffix}")

    print(f"\n--- ZIP codes with no income data    : {len(no_inc)} ---")
    if no_inc:
        preview = ", ".join(no_inc[:20])
        suffix = f"  … and {len(no_inc) - 20} more" if len(no_inc) > 20 else ""
        print(f"  {preview}{suffix}")

    # --- state_index ----------------------------------------------------------
    print_separator()
    print("state_index.json validation")
    print_separator()
    si_ok = state_result.get("ok", False)
    print(f"  Status      : {'PASS' if si_ok else 'FAIL'}")
    if "entry_count" in state_result:
        print(f"  Entries     : {state_result['entry_count']}")
    if state_result["issues"]:
        for issue in state_result["issues"][:10]:
            print(f"  [ISSUE] {issue}")
        if len(state_result["issues"]) > 10:
            print(f"  … and {len(state_result['issues']) - 10} more issues")

    # --- city_index -----------------------------------------------------------
    print_separator()
    print("city_index.json validation")
    print_separator()
    ci_ok = city_result.get("ok", False)
    print(f"  Status      : {'PASS' if ci_ok else 'FAIL'}")
    if "entry_count" in city_result:
        print(f"  Entries     : {city_result['entry_count']}")
    if city_result["issues"]:
        for issue in city_result["issues"][:10]:
            print(f"  [ISSUE] {issue}")
        if len(city_result["issues"]) > 10:
            print(f"  … and {len(city_result['issues']) - 10} more issues")

    # --- Critical summary -----------------------------------------------------
    print_separator("=")
    critical_zips = zip_results["critical_missing_zips"]
    if critical_zips:
        print(f"CRITICAL ERRORS — {len(critical_zips)} ZIP(s) missing critical fields:")
        preview = ", ".join(sorted(critical_zips)[:20])
        suffix = f"  … and {len(critical_zips) - 20} more" if len(critical_zips) > 20 else ""
        print(f"  {preview}{suffix}")
        print("OVERALL: FAIL")
        print_separator("=")
        return False

    print("OVERALL: PASS — no critical fields missing.")
    print_separator("=")
    return True


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_validation() -> bool:
    if not ZIPS_DIR.exists():
        logger.error("ZIP output directory not found: %s", ZIPS_DIR)
        return False

    zip_results = validate_zip_files(ZIPS_DIR)
    state_result = validate_state_index(STATE_INDEX_PATH)
    city_result = validate_city_index(CITY_INDEX_PATH)

    passed = print_report(zip_results, state_result, city_result)
    return passed


if __name__ == "__main__":
    success = run_validation()
    sys.exit(0 if success else 1)

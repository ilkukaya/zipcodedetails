"""
03_merge_and_enrich.py

Merges the three raw data files produced by scripts 01 and 02, enriches each
record with the 5 nearest ZIPs (by Haversine distance), fills missing values
with sensible defaults, and writes the final dataset to:

  data/processed/all_zips.csv

Columns in output
-----------------
zip, city, state, county_name, county_fips, timezone, cbsa_code, cbsa_name,
latitude, longitude, land_area_sqmi, water_area_sqmi,
population, median_household_income,
surrounding_zips   ← pipe-separated list of 5 nearest ZIP codes
"""

import logging
import math
from pathlib import Path

import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"

GAZETTEER_CSV = RAW_DIR / "gazetteer.csv"
ACS_CSV = RAW_DIR / "acs_data.csv"
ZIP_MAPPING_CSV = RAW_DIR / "zip_mapping.csv"
OUTPUT_CSV = PROCESSED_DIR / "all_zips.csv"

# Number of nearest neighbours to compute
N_SURROUNDING = 5

# Columns we expect / want in the final output (in order)
FINAL_COLUMNS = [
    "zip",
    "city",
    "state",
    "county_name",
    "county_fips",
    "timezone",
    "cbsa_code",
    "cbsa_name",
    "latitude",
    "longitude",
    "land_area_sqmi",
    "water_area_sqmi",
    "population",
    "median_household_income",
    "surrounding_zips",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Haversine distance
# ---------------------------------------------------------------------------

_EARTH_RADIUS_MILES = 3958.8  # mean radius


def haversine_miles(
    lat1: float, lon1: float,
    lat2: np.ndarray, lon2: np.ndarray,
) -> np.ndarray:
    """
    Vectorised Haversine distance from a single point (lat1, lon1) to an
    array of points (lat2, lon2).  Returns distances in miles.
    """
    r = _EARTH_RADIUS_MILES
    phi1 = math.radians(lat1)
    phi2 = np.radians(lat2)
    dphi = phi2 - phi1
    dlambda = np.radians(lon2 - lon1)

    a = (
        np.sin(dphi / 2) ** 2
        + math.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
    )
    return 2 * r * np.arcsin(np.sqrt(a))


# ---------------------------------------------------------------------------
# Surrounding ZIPs
# ---------------------------------------------------------------------------

def compute_surrounding_zips(df: pd.DataFrame, n: int = N_SURROUNDING) -> pd.Series:
    """
    For every row in *df* that has valid latitude/longitude, find the *n*
    nearest other ZIPs by Haversine distance and return them as a
    pipe-separated string.

    Rows without coordinates get an empty string.
    """
    log.info("Computing %d nearest neighbours for %d ZIPs …", n, len(df))

    has_coords = df["latitude"].notna() & df["longitude"].notna()
    coords_df = df.loc[has_coords, ["zip", "latitude", "longitude"]].reset_index(drop=True)

    zip_codes = coords_df["zip"].to_numpy()
    lats = coords_df["latitude"].to_numpy(dtype=float)
    lons = coords_df["longitude"].to_numpy(dtype=float)

    surrounding: dict[str, str] = {}

    total = len(coords_df)
    for i in range(total):
        if i % 5000 == 0:
            log.info("  Nearest-neighbour progress: %d / %d", i, total)

        lat_i, lon_i = lats[i], lons[i]
        # Compute distances from point i to all others
        distances = haversine_miles(lat_i, lon_i, lats, lons)
        # Exclude self (distance == 0)
        distances[i] = math.inf

        # Partial-sort: get indices of n smallest distances
        if total > n:
            nearest_indices = np.argpartition(distances, n)[:n]
            # Sort the top-n by actual distance
            nearest_indices = nearest_indices[np.argsort(distances[nearest_indices])]
        else:
            nearest_indices = np.argsort(distances)[:n]

        surrounding[zip_codes[i]] = "|".join(zip_codes[nearest_indices])

    # Map back to original index
    result = df["zip"].map(surrounding).fillna("")
    log.info("Surrounding ZIPs computed.  Filled: %d / %d", result.ne("").sum(), len(df))
    return result


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        log.warning("%s not found at %s — returning empty DataFrame.", label, path)
        return pd.DataFrame()
    log.info("Loading %s from %s …", label, path)
    df = pd.read_csv(path, dtype=str)
    log.info("  %d rows, %d columns", len(df), len(df.columns))
    return df


# ---------------------------------------------------------------------------
# Merge logic
# ---------------------------------------------------------------------------

def merge_all(
    gaz: pd.DataFrame,
    acs: pd.DataFrame,
    mapping: pd.DataFrame,
) -> pd.DataFrame:
    """
    Left-join on 'zip'.  Gazetteer is the canonical source of ZIPs;
    ACS and mapping data are supplemental.
    """

    def normalise_zip(df: pd.DataFrame) -> pd.DataFrame:
        if "zip" in df.columns:
            df["zip"] = df["zip"].astype(str).str.strip().str.zfill(5)
        return df

    gaz = normalise_zip(gaz.copy())
    acs = normalise_zip(acs.copy())
    mapping = normalise_zip(mapping.copy())

    log.info("Merging Gazetteer ← ZIP mapping …")
    if not mapping.empty:
        # Drop columns already in gazetteer except 'zip'
        mapping_cols = [
            c for c in mapping.columns
            if c == "zip" or c not in gaz.columns
        ]
        merged = gaz.merge(mapping[mapping_cols], on="zip", how="left")
    else:
        log.warning("ZIP mapping is empty; skipping merge.")
        merged = gaz.copy()

    log.info("Merging ← ACS data …")
    if not acs.empty:
        # Keep only useful ACS columns
        acs_keep = ["zip"]
        for col in ["population", "median_household_income", "zcta_name"]:
            if col in acs.columns:
                acs_keep.append(col)
        # Don't overwrite existing columns
        acs_keep = [c for c in acs_keep if c == "zip" or c not in merged.columns]
        merged = merged.merge(acs[acs_keep], on="zip", how="left")
    else:
        log.warning("ACS data is empty; skipping merge.")

    log.info("Merged total rows: %d", len(merged))
    return merged


# ---------------------------------------------------------------------------
# Fill missing values
# ---------------------------------------------------------------------------

def fill_defaults(df: pd.DataFrame) -> pd.DataFrame:
    """Apply sensible defaults for missing values."""

    # Numeric columns: convert first, then fill with 0 / -1
    for col in ["latitude", "longitude", "land_area_sqmi", "water_area_sqmi"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["population", "median_household_income"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            # -1 indicates data not available (distinguishable from 0)
            df[col] = df[col].fillna(-1).astype(int)

    for col in ["land_area_sqmi", "water_area_sqmi"]:
        if col in df.columns:
            df[col] = df[col].fillna(0.0)

    # String columns: empty string
    str_defaults = ["city", "state", "county_name", "county_fips",
                    "timezone", "cbsa_code", "cbsa_name", "surrounding_zips"]
    for col in str_defaults:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    return df


# ---------------------------------------------------------------------------
# Logging statistics
# ---------------------------------------------------------------------------

def log_statistics(df: pd.DataFrame, label: str = "Final dataset") -> None:
    log.info("=== %s statistics ===", label)
    log.info("  Total records      : %d", len(df))
    for col in FINAL_COLUMNS:
        if col not in df.columns:
            log.info("  %-28s : COLUMN MISSING", col)
            continue
        series = df[col]
        if pd.api.types.is_numeric_dtype(series):
            missing = (series.isna() | series.eq(-1)).sum()
            log.info("  %-28s : %d missing / -1", col, missing)
        else:
            missing = series.eq("").sum() + series.isna().sum()
            log.info("  %-28s : %d blank/null", col, missing)
    log.info("==============================")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    log.info("Output directory: %s", PROCESSED_DIR)

    # --- Load ---
    log.info("=== Step 1/4: Load raw data files ===")
    gaz = load_csv(GAZETTEER_CSV, "Gazetteer")
    acs = load_csv(ACS_CSV, "ACS data")
    mapping = load_csv(ZIP_MAPPING_CSV, "ZIP mapping")

    if gaz.empty:
        raise RuntimeError(
            "Gazetteer data is missing.  Run 01_download_census.py first."
        )

    # --- Merge ---
    log.info("=== Step 2/4: Merge datasets ===")
    merged = merge_all(gaz, acs, mapping)

    # --- Surrounding ZIPs ---
    log.info("=== Step 3/4: Compute surrounding ZIPs ===")
    # Convert lat/lon to float before passing
    for col in ["latitude", "longitude"]:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce")

    merged["surrounding_zips"] = compute_surrounding_zips(merged, n=N_SURROUNDING)

    # --- Fill defaults ---
    log.info("=== Step 4/4: Fill defaults and write output ===")
    merged = fill_defaults(merged)

    # Reorder to final column list (keep any extra columns at the end)
    ordered = [c for c in FINAL_COLUMNS if c in merged.columns]
    extras = [c for c in merged.columns if c not in ordered]
    merged = merged[ordered + extras]

    # Log stats before writing
    log_statistics(merged)

    merged.to_csv(OUTPUT_CSV, index=False)
    log.info("Saved enriched ZIP data → %s  (%d rows, %d columns)",
             OUTPUT_CSV, len(merged), len(merged.columns))
    log.info("=== Pipeline complete ===")


if __name__ == "__main__":
    main()

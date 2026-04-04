"""
03_merge_and_enrich.py

Merges the three raw data files produced by scripts 01 and 02, enriches each
record with the 5 nearest ZIPs (by Haversine distance), fills missing values
with sensible defaults, and writes the final dataset to:

  data/processed/all_zips.csv

Columns in output (matching what script 04 expects)
----------------------------------------------------
zip, city, state, state_full, county, cbsa, cbsa_code,
lat, lng, land_area_sqmi, water_area_sqmi,
population, median_household_income,
timezone, dst, zip_type, surrounding_zips
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

N_SURROUNDING = 5

FINAL_COLUMNS = [
    "zip",
    "city",
    "state",
    "state_full",
    "county",
    "cbsa",
    "cbsa_code",
    "population",
    "median_household_income",
    "land_area_sqmi",
    "water_area_sqmi",
    "lat",
    "lng",
    "timezone",
    "dst",
    "zip_type",
    "surrounding_zips",
]

# State full names (fallback if script 02 didn't populate)
STATE_FULL_NAMES: dict[str, str] = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
    "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska",
    "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
    "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "AS": "American Samoa", "GU": "Guam", "MP": "Northern Mariana Islands",
    "PR": "Puerto Rico", "VI": "U.S. Virgin Islands",
}

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

_EARTH_RADIUS_MILES = 3958.8


def haversine_miles(
    lat1: float, lon1: float,
    lat2: np.ndarray, lon2: np.ndarray,
) -> np.ndarray:
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
    log.info("Computing %d nearest neighbours for %d ZIPs …", n, len(df))

    has_coords = df["lat"].notna() & df["lng"].notna()
    coords_df = df.loc[has_coords, ["zip", "lat", "lng"]].reset_index(drop=True)

    zip_codes = coords_df["zip"].to_numpy()
    lats = coords_df["lat"].to_numpy(dtype=float)
    lons = coords_df["lng"].to_numpy(dtype=float)

    surrounding: dict[str, str] = {}
    total = len(coords_df)

    for i in range(total):
        if i % 5000 == 0:
            log.info("  Nearest-neighbour progress: %d / %d", i, total)

        distances = haversine_miles(lats[i], lons[i], lats, lons)
        distances[i] = math.inf

        if total > n:
            nearest_indices = np.argpartition(distances, n)[:n]
            nearest_indices = nearest_indices[np.argsort(distances[nearest_indices])]
        else:
            nearest_indices = np.argsort(distances)[:n]

        surrounding[zip_codes[i]] = "|".join(zip_codes[nearest_indices])

    result = df["zip"].map(surrounding).fillna("")
    log.info("Surrounding ZIPs computed. Filled: %d / %d", result.ne("").sum(), len(df))
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
        acs_keep = ["zip"]
        for col in ["population", "median_household_income"]:
            if col in acs.columns:
                acs_keep.append(col)
        acs_keep = [c for c in acs_keep if c == "zip" or c not in merged.columns]
        merged = merged.merge(acs[acs_keep], on="zip", how="left")
    else:
        log.warning("ACS data is empty; skipping merge.")

    log.info("Merged total rows: %d", len(merged))
    return merged


# ---------------------------------------------------------------------------
# Column renaming and enrichment
# ---------------------------------------------------------------------------

def rename_and_enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns from raw names to the names script 04 expects."""

    # Rename latitude/longitude → lat/lng
    rename_map = {}
    if "latitude" in df.columns and "lat" not in df.columns:
        rename_map["latitude"] = "lat"
    if "longitude" in df.columns and "lng" not in df.columns:
        rename_map["longitude"] = "lng"
    # county_name → county (if old script 02 was used)
    if "county_name" in df.columns and "county" not in df.columns:
        rename_map["county_name"] = "county"
    # cbsa_name → cbsa
    if "cbsa_name" in df.columns and "cbsa" not in df.columns:
        rename_map["cbsa_name"] = "cbsa"

    if rename_map:
        log.info("Renaming columns: %s", rename_map)
        df = df.rename(columns=rename_map)

    # Ensure state_full exists
    if "state_full" not in df.columns:
        log.info("Adding state_full column from state abbreviation …")
        df["state_full"] = df["state"].map(STATE_FULL_NAMES).fillna("")

    # Fill missing city from county name (strip "County" / "Parish" suffix)
    if "city" in df.columns and "county" in df.columns:
        empty_city = df["city"].fillna("").eq("")
        if empty_city.any():
            county_as_city = (
                df.loc[empty_city, "county"]
                .fillna("")
                .str.replace(
                    r'\s+(County|Parish|Borough|Census Area|Municipality|city|Municipio)$',
                    '', regex=True
                )
                .str.strip()
            )
            df.loc[empty_city, "city"] = county_as_city
            log.info("Filled %d missing city values from county name.", empty_city.sum())

    # Ensure dst exists
    if "dst" not in df.columns:
        log.info("Adding dst column from timezone …")
        NO_DST = {
            "America/Phoenix", "Pacific/Honolulu", "America/St_Thomas",
            "America/Puerto_Rico", "Pacific/Guam", "Pacific/Pago_Pago",
        }
        if "timezone" in df.columns:
            df["dst"] = df["timezone"].apply(
                lambda tz: str(tz) not in NO_DST if pd.notna(tz) and str(tz) else False
            )
        else:
            df["dst"] = True

    # Ensure zip_type exists
    if "zip_type" not in df.columns:
        log.info("Adding zip_type column (default STANDARD) …")
        df["zip_type"] = "STANDARD"

    # Ensure cbsa and cbsa_code exist
    if "cbsa" not in df.columns:
        df["cbsa"] = ""
    if "cbsa_code" not in df.columns:
        df["cbsa_code"] = ""

    # Ensure county exists
    if "county" not in df.columns:
        df["county"] = ""

    return df


# ---------------------------------------------------------------------------
# Fill missing values
# ---------------------------------------------------------------------------

def fill_defaults(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["lat", "lng", "land_area_sqmi", "water_area_sqmi"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["population", "median_household_income"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].fillna(-1).astype(int)

    for col in ["land_area_sqmi", "water_area_sqmi"]:
        if col in df.columns:
            df[col] = df[col].fillna(0.0)

    str_defaults = ["city", "state", "state_full", "county",
                    "timezone", "cbsa", "cbsa_code", "zip_type",
                    "surrounding_zips"]
    for col in str_defaults:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    # Ensure dst is boolean
    if "dst" in df.columns:
        df["dst"] = df["dst"].apply(
            lambda v: bool(v) if isinstance(v, bool) else str(v).lower() in ("true", "1", "yes")
        )

    return df


# ---------------------------------------------------------------------------
# Statistics
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
        elif pd.api.types.is_bool_dtype(series):
            log.info("  %-28s : %d True, %d False", col, series.sum(), (~series).sum())
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
    log.info("=== Step 1/5: Load raw data files ===")
    gaz = load_csv(GAZETTEER_CSV, "Gazetteer")
    acs = load_csv(ACS_CSV, "ACS data")
    mapping = load_csv(ZIP_MAPPING_CSV, "ZIP mapping")

    if gaz.empty:
        raise RuntimeError("Gazetteer data is missing. Run 01_download_census.py first.")

    # --- Merge ---
    log.info("=== Step 2/5: Merge datasets ===")
    merged = merge_all(gaz, acs, mapping)

    # --- Rename and enrich ---
    log.info("=== Step 3/5: Rename columns and enrich ===")
    merged = rename_and_enrich(merged)

    # --- Surrounding ZIPs ---
    log.info("=== Step 4/5: Compute surrounding ZIPs ===")
    for col in ["lat", "lng"]:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce")

    merged["surrounding_zips"] = compute_surrounding_zips(merged, n=N_SURROUNDING)

    # --- Fill defaults ---
    log.info("=== Step 5/5: Fill defaults and write output ===")
    merged = fill_defaults(merged)

    # Reorder to final column list
    ordered = [c for c in FINAL_COLUMNS if c in merged.columns]
    extras = [c for c in merged.columns if c not in ordered]
    merged = merged[ordered + extras]

    log_statistics(merged)

    merged.to_csv(OUTPUT_CSV, index=False)
    log.info("Saved enriched ZIP data → %s  (%d rows, %d columns)",
             OUTPUT_CSV, len(merged), len(merged.columns))
    log.info("=== Pipeline complete ===")


if __name__ == "__main__":
    main()

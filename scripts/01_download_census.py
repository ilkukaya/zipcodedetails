"""
01_download_census.py

Downloads US Census Bureau data needed for the ZIP code reference website:
  1. Census Gazetteer 2023 ZCTA file (geography: land area, water area, lat, lon)
  2. ACS 5-year estimates 2022 (population, median household income)

Outputs
-------
data/raw/gazetteer.csv
data/raw/acs_data.csv
"""

import io
import logging
import os
import time
import zipfile
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CENSUS_API_KEY = "bce2f6b976e5e03781def23918ecc67b34498ee7"

GAZETTEER_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
    "2023_Gazetteer/2023_Gaz_zcta_national.zip"
)

ACS_URL = (
    "https://api.census.gov/data/2022/acs/acs5"
    "?get=NAME,B01003_001E,B19013_001E"
    "&for=zip+code+tabulation+area:*"
    f"&key={CENSUS_API_KEY}"
)

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
GAZETTEER_OUT = RAW_DIR / "gazetteer.csv"
ACS_OUT = RAW_DIR / "acs_data.csv"

# Retry configuration
MAX_RETRIES = 5
BACKOFF_FACTOR = 1.5      # seconds between retries: 1.5, 3, 6, 12 …
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
REQUEST_TIMEOUT = 120     # seconds

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
# Helpers
# ---------------------------------------------------------------------------

def build_session() -> requests.Session:
    """Return a requests Session with automatic retry logic."""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=RETRY_STATUS_CODES,
        allowed_methods={"GET"},
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def download_bytes(session: requests.Session, url: str, label: str) -> bytes:
    """Download *url* and return raw bytes; raise on failure."""
    log.info("Downloading %s from %s", label, url)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT, stream=True)
            response.raise_for_status()
            chunks = []
            total = 0
            for chunk in response.iter_content(chunk_size=1024 * 256):
                if chunk:
                    chunks.append(chunk)
                    total += len(chunk)
            log.info("  Downloaded %.2f MB for %s", total / 1_048_576, label)
            return b"".join(chunks)
        except requests.RequestException as exc:
            log.warning(
                "  Attempt %d/%d failed for %s: %s",
                attempt, MAX_RETRIES, label, exc,
            )
            if attempt < MAX_RETRIES:
                sleep_time = BACKOFF_FACTOR * (2 ** (attempt - 1))
                log.info("  Retrying in %.1f s …", sleep_time)
                time.sleep(sleep_time)
            else:
                raise RuntimeError(
                    f"Failed to download {label} after {MAX_RETRIES} attempts"
                ) from exc


# ---------------------------------------------------------------------------
# Step 1 – Gazetteer
# ---------------------------------------------------------------------------

def download_gazetteer(session: requests.Session) -> pd.DataFrame:
    """Download, unzip, and parse the Census Gazetteer ZCTA file."""
    raw_bytes = download_bytes(session, GAZETTEER_URL, "Census Gazetteer ZIP")

    log.info("Extracting Gazetteer ZIP archive …")
    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
        txt_files = [n for n in zf.namelist() if n.lower().endswith(".txt")]
        if not txt_files:
            raise RuntimeError("No .txt file found inside Gazetteer ZIP.")
        txt_name = txt_files[0]
        log.info("  Reading member: %s", txt_name)
        with zf.open(txt_name) as fh:
            content = fh.read().decode("latin-1")

    df = pd.read_csv(
        io.StringIO(content),
        sep="\t",
        dtype=str,
        skipinitialspace=True,
    )

    # Normalise column names: strip whitespace, uppercase
    df.columns = [c.strip().upper() for c in df.columns]

    log.info("Gazetteer raw columns: %s", list(df.columns))

    # Expected columns after normalisation:
    # GEOID, ALAND, AWATER, ALAND_SQMI, AWATER_SQMI, INTPTLAT, INTPTLONG
    rename_map = {
        "GEOID": "zip",
        "ALAND_SQMI": "land_area_sqmi",
        "AWATER_SQMI": "water_area_sqmi",
        "INTPTLAT": "latitude",
        "INTPTLONG": "longitude",
    }
    # Only rename columns that exist
    rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    # Ensure zip is zero-padded to 5 digits
    if "zip" in df.columns:
        df["zip"] = df["zip"].str.strip().str.zfill(5)

    # Convert numeric columns
    for col in ["latitude", "longitude", "land_area_sqmi", "water_area_sqmi"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    log.info("Gazetteer records: %d", len(df))
    return df


# ---------------------------------------------------------------------------
# Step 2 – ACS 5-year estimates
# ---------------------------------------------------------------------------

def download_acs(session: requests.Session) -> pd.DataFrame:
    """Download ACS 5-year estimates for all ZCTAs."""
    raw_bytes = download_bytes(session, ACS_URL, "ACS 5-year estimates")

    log.info("Parsing ACS JSON response …")
    import json
    data = json.loads(raw_bytes.decode("utf-8"))

    # First row is headers
    headers = data[0]
    rows = data[1:]
    df = pd.DataFrame(rows, columns=headers)

    log.info("ACS raw columns: %s", list(df.columns))

    # Rename to friendly names
    rename_map = {
        "zip code tabulation area": "zip",
        "B01003_001E": "population",
        "B19013_001E": "median_household_income",
        "NAME": "zcta_name",
    }
    rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    if "zip" in df.columns:
        df["zip"] = df["zip"].str.strip().str.zfill(5)

    for col in ["population", "median_household_income"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            # Census uses -666666666 as a sentinel for missing
            df.loc[df[col] < -99999, col] = pd.NA

    log.info("ACS records: %d", len(df))
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    log.info("Output directory: %s", RAW_DIR)

    session = build_session()

    # --- Gazetteer ---
    log.info("=== Step 1/2: Census Gazetteer ===")
    gazetteer_df = download_gazetteer(session)
    gazetteer_df.to_csv(GAZETTEER_OUT, index=False)
    log.info("Saved Gazetteer → %s  (%d rows)", GAZETTEER_OUT, len(gazetteer_df))

    # --- ACS ---
    log.info("=== Step 2/2: ACS 5-year estimates ===")
    acs_df = download_acs(session)
    acs_df.to_csv(ACS_OUT, index=False)
    log.info("Saved ACS data → %s  (%d rows)", ACS_OUT, len(acs_df))

    log.info("=== Download complete ===")


if __name__ == "__main__":
    main()

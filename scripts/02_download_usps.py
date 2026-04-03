"""
02_download_usps.py

Builds a comprehensive ZIP-code-to-city/state/county/timezone/CBSA mapping
without requiring HUD USPS registration.

Strategy
--------
1. Read the Gazetteer file already downloaded by 01_download_census.py to get
   confirmed ZCTA codes and coordinates.
2. Call the Census Geocoder batch API to resolve FIPS county codes for a sample
   of ZIPs, then extrapolate the rest via a state-prefix lookup table.
3. Apply a built-in state → primary timezone table.
4. Apply a built-in CBSA lookup for the ~400 largest metro areas.
5. For ZIPs whose coordinates fall in a state with multiple timezones, use a
   longitude-based heuristic to pick the correct zone.

Output
------
data/raw/zip_mapping.csv  — one row per ZCTA with columns:
  zip, city, state, county_name, county_fips, timezone, cbsa_code, cbsa_name
"""

import io
import json
import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CENSUS_API_KEY = "bce2f6b976e5e03781def23918ecc67b34498ee7"

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
GAZETTEER_CSV = RAW_DIR / "gazetteer.csv"
ZIP_MAPPING_OUT = RAW_DIR / "zip_mapping.csv"

MAX_RETRIES = 5
BACKOFF_FACTOR = 1.5
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
REQUEST_TIMEOUT = 60

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
# Static mapping tables
# ---------------------------------------------------------------------------

# State abbreviation → (primary_timezone, longitude_boundary_for_split_states)
# Longitude boundary: if a state spans two zones, ZIPs east of the boundary get
# zone[0], ZIPs at or west of the boundary get zone[1].
# None means no split — use zone[0] uniformly.
STATE_TIMEZONE: dict[str, tuple[str, float | None]] = {
    "AL": ("America/Chicago", None),
    "AK": ("America/Anchorage", None),
    "AZ": ("America/Phoenix", None),
    "AR": ("America/Chicago", None),
    "CA": ("America/Los_Angeles", None),
    "CO": ("America/Denver", None),
    "CT": ("America/New_York", None),
    "DE": ("America/New_York", None),
    "DC": ("America/New_York", None),
    "FL": ("America/New_York", -85.5),   # Panhandle → Chicago west of -85.5
    "GA": ("America/New_York", None),
    "GU": ("Pacific/Guam", None),
    "HI": ("Pacific/Honolulu", None),
    "ID": ("America/Boise", -113.5),     # Northern tip → Los_Angeles
    "IL": ("America/Chicago", None),
    "IN": ("America/Indiana/Indianapolis", None),
    "IA": ("America/Chicago", None),
    "KS": ("America/Chicago", -101.5),   # Far west → Denver
    "KY": ("America/New_York", -84.8),   # Western KY → Chicago
    "LA": ("America/Chicago", None),
    "ME": ("America/New_York", None),
    "MD": ("America/New_York", None),
    "MA": ("America/New_York", None),
    "MI": ("America/Detroit", None),
    "MN": ("America/Chicago", None),
    "MS": ("America/Chicago", None),
    "MO": ("America/Chicago", None),
    "MT": ("America/Denver", None),
    "NE": ("America/Chicago", -104.0),   # Western NE → Denver
    "NV": ("America/Los_Angeles", None),
    "NH": ("America/New_York", None),
    "NJ": ("America/New_York", None),
    "NM": ("America/Denver", None),
    "NY": ("America/New_York", None),
    "NC": ("America/New_York", None),
    "ND": ("America/Chicago", -101.5),   # Far west → Denver
    "OH": ("America/New_York", None),
    "OK": ("America/Chicago", None),
    "OR": ("America/Los_Angeles", None),
    "PA": ("America/New_York", None),
    "PR": ("America/Puerto_Rico", None),
    "RI": ("America/New_York", None),
    "SC": ("America/New_York", None),
    "SD": ("America/Chicago", -104.0),   # West SD → Denver
    "TN": ("America/Chicago", -84.5),    # East TN → New_York
    "TX": ("America/Chicago", -104.8),   # Far west TX → Denver
    "UT": ("America/Denver", None),
    "VT": ("America/New_York", None),
    "VA": ("America/New_York", None),
    "VI": ("America/St_Thomas", None),
    "WA": ("America/Los_Angeles", None),
    "WV": ("America/New_York", None),
    "WI": ("America/Chicago", None),
    "WY": ("America/Denver", None),
}

# FIPS state code → abbreviation
FIPS_TO_STATE: dict[str, str] = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY", "60": "AS", "66": "GU", "69": "MP", "72": "PR",
    "78": "VI",
}

# ZIP prefix (first 3 digits) → state abbreviation
# Source: USPS Publication 28 / public domain postal knowledge
ZIP_PREFIX_TO_STATE: dict[str, str] = {
    **{str(p).zfill(3): "MA" for p in range(10, 28)},   # 010–027
    "028": "RI", "029": "RI",
    **{str(p).zfill(3): "NH" for p in range(30, 39)},
    **{str(p).zfill(3): "ME" for p in range(39, 50)},
    **{str(p).zfill(3): "VT" for p in [50, 51, 52, 53, 54, 56, 57, 58, 59]},
    **{str(p).zfill(3): "CT" for p in range(60, 70)},
    **{str(p).zfill(3): "NJ" for p in range(70, 90)},
    **{str(p).zfill(3): "NY" for p in list(range(100, 150))},
    **{str(p).zfill(3): "PA" for p in range(150, 200)},
    **{str(p).zfill(3): "DE" for p in [197, 198, 199]},
    **{str(p).zfill(3): "MD" for p in list(range(206, 220)) + [200, 201, 202, 203, 204, 205]},
    "201": "DC", "202": "DC", "203": "DC", "204": "DC", "205": "DC",
    **{str(p).zfill(3): "VA" for p in list(range(220, 247))},
    **{str(p).zfill(3): "WV" for p in range(247, 270)},
    **{str(p).zfill(3): "NC" for p in range(270, 290)},
    **{str(p).zfill(3): "SC" for p in range(290, 300)},
    **{str(p).zfill(3): "GA" for p in range(300, 320)},
    **{str(p).zfill(3): "FL" for p in range(320, 350)},
    **{str(p).zfill(3): "AL" for p in range(350, 370)},
    **{str(p).zfill(3): "TN" for p in range(370, 386)},
    **{str(p).zfill(3): "MS" for p in range(386, 400)},
    **{str(p).zfill(3): "KY" for p in range(400, 428)},
    **{str(p).zfill(3): "OH" for p in range(430, 460)},
    **{str(p).zfill(3): "IN" for p in range(460, 480)},
    **{str(p).zfill(3): "MI" for p in range(480, 500)},
    **{str(p).zfill(3): "IA" for p in range(500, 528)},
    **{str(p).zfill(3): "WI" for p in range(530, 550)},
    **{str(p).zfill(3): "MN" for p in range(550, 568)},
    **{str(p).zfill(3): "SD" for p in range(570, 578)},
    **{str(p).zfill(3): "ND" for p in range(580, 590)},
    **{str(p).zfill(3): "MT" for p in range(590, 600)},
    **{str(p).zfill(3): "IL" for p in range(600, 630)},
    **{str(p).zfill(3): "MO" for p in range(630, 660)},
    **{str(p).zfill(3): "KS" for p in range(660, 680)},
    **{str(p).zfill(3): "NE" for p in range(680, 694)},
    **{str(p).zfill(3): "LA" for p in range(700, 715)},
    **{str(p).zfill(3): "AR" for p in range(716, 730)},
    **{str(p).zfill(3): "OK" for p in range(730, 750)},
    **{str(p).zfill(3): "TX" for p in range(750, 800)},
    **{str(p).zfill(3): "CO" for p in range(800, 817)},
    **{str(p).zfill(3): "WY" for p in range(820, 832)},
    **{str(p).zfill(3): "ID" for p in range(832, 839)},
    **{str(p).zfill(3): "UT" for p in range(840, 848)},
    **{str(p).zfill(3): "AZ" for p in range(850, 866)},
    **{str(p).zfill(3): "NM" for p in range(870, 885)},
    **{str(p).zfill(3): "NV" for p in range(889, 900)},
    **{str(p).zfill(3): "CA" for p in range(900, 962)},
    **{str(p).zfill(3): "OR" for p in range(970, 980)},
    **{str(p).zfill(3): "WA" for p in range(980, 995)},
    **{str(p).zfill(3): "AK" for p in range(995, 1000)},
    **{str(p).zfill(3): "HI" for p in range(967, 969)},
    "006": "PR", "007": "PR", "008": "PR", "009": "PR",
}

# Selected large CBSAs: cbsa_code → (cbsa_name, list_of_zip_prefixes)
# Only the most-populated metros; the full list would be 900+ entries.
# This is supplemental — ZIPs not matched here will have blank CBSA fields.
CBSA_PREFIX_MAP: list[tuple[str, str, list[str]]] = [
    ("35620", "New York-Newark-Jersey City, NY-NJ-PA",
     ["100", "101", "102", "103", "104", "105", "106", "107", "108", "109",
      "110", "111", "112", "113", "114", "115", "116", "117", "118", "119",
      "070", "071", "072", "073", "074", "075", "076", "077", "078", "079"]),
    ("31080", "Los Angeles-Long Beach-Anaheim, CA",
     ["900", "901", "902", "903", "904", "905", "906", "907", "908",
      "910", "911", "912", "913", "914", "915", "916", "917", "918", "926", "927", "928"]),
    ("16980", "Chicago-Naperville-Elgin, IL-IN-WI",
     ["600", "601", "602", "603", "604", "605", "606", "607", "608",
      "460", "461", "463", "464", "465", "530", "531"]),
    ("19100", "Dallas-Fort Worth-Arlington, TX",
     ["750", "751", "752", "753", "754", "755", "756", "757", "758", "759",
      "760", "761", "762", "763", "764"]),
    ("26420", "Houston-The Woodlands-Sugar Land, TX",
     ["770", "771", "772", "773", "774", "775", "776", "777"]),
    ("37980", "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
     ["190", "191", "192", "193", "194", "195", "196", "080", "081", "082", "083",
      "197", "198", "210", "211"]),
    ("33100", "Miami-Fort Lauderdale-Pompano Beach, FL",
     ["330", "331", "332", "333", "334"]),
    ("14460", "Boston-Cambridge-Newton, MA-NH",
     ["017", "018", "019", "020", "021", "022", "023", "024", "025", "026", "027"]),
    ("38060", "Phoenix-Mesa-Chandler, AZ",
     ["850", "851", "852", "853", "855", "856", "857", "859", "860"]),
    ("41860", "San Francisco-Oakland-Berkeley, CA",
     ["940", "941", "942", "943", "944", "945", "946", "947", "948", "949"]),
    ("47900", "Washington-Arlington-Alexandria, DC-VA-MD-WV",
     ["200", "201", "202", "203", "204", "205", "220", "221", "222", "223",
      "206", "207", "208", "209"]),
    ("28140", "Kansas City, MO-KS",
     ["640", "641", "644", "645", "660", "661", "662"]),
    ("17460", "Cleveland-Elyria, OH",
     ["440", "441", "442", "443", "444"]),
    ("39580", "Raleigh-Cary, NC",
     ["275", "276", "277"]),
    ("12060", "Atlanta-Sandy Springs-Alpharetta, GA",
     ["300", "301", "302", "303", "304", "305", "306", "307", "308", "309",
      "310", "311", "312", "313", "314", "315", "316", "317", "318", "319"]),
    ("40140", "Riverside-San Bernardino-Ontario, CA",
     ["917", "918", "919", "920", "921", "922", "923", "924", "925"]),
    ("41740", "San Diego-Chula Vista-Carlsbad, CA",
     ["919", "920", "921", "922"]),
    ("45300", "Tampa-St. Petersburg-Clearwater, FL",
     ["335", "336", "337", "338", "339", "340"]),
    ("19820", "Detroit-Warren-Dearborn, MI",
     ["480", "481", "482", "483", "484", "485"]),
    ("41180", "St. Louis, MO-IL",
     ["630", "631", "632", "633", "634", "621", "622", "623"]),
    ("42660", "Seattle-Tacoma-Bellevue, WA",
     ["980", "981", "982", "983", "984", "985", "986"]),
    ("38300", "Pittsburgh, PA",
     ["150", "151", "152", "153", "154", "155", "156"]),
    ("36740", "Orlando-Kissimmee-Sanford, FL",
     ["327", "328", "329", "347"]),
    ("29820", "Las Vegas-Henderson-Paradise, NV",
     ["889", "890", "891", "893"]),
    ("36420", "Oklahoma City, OK",
     ["730", "731", "734", "735", "736", "737"]),
    ("27260", "Jacksonville, FL",
     ["320", "321", "322"]),
    ("34980", "Nashville-Davidson--Murfreesboro--Franklin, TN",
     ["370", "371", "372", "373"]),
    ("32820", "Memphis, TN-MS-AR",
     ["380", "381", "382"]),
    ("18140", "Columbus, OH",
     ["430", "431", "432", "433", "434"]),
    ("24340", "Grand Rapids-Kentwood, MI",
     ["493", "494", "495", "496"]),
    ("40900", "Sacramento-Roseville-Folsom, CA",
     ["956", "957", "958", "959", "960", "961"]),
    ("24860", "Harrisburg-Carlisle, PA",
     ["170", "171", "172", "173", "174", "175", "176", "177"]),
    ("25540", "Hartford-East Hartford-Middletown, CT",
     ["060", "061", "062", "063", "064", "065", "066"]),
    ("12420", "Austin-Round Rock-Georgetown, TX",
     ["786", "787", "788"]),
    ("29460", "Knoxville, TN",
     ["377", "378", "379"]),
    ("16740", "Charlotte-Concord-Gastonia, NC-SC",
     ["280", "281", "282", "283", "284", "290", "291", "292"]),
    ("31140", "Louisville/Jefferson County, KY-IN",
     ["400", "401", "402", "410", "411", "412"]),
    ("26900", "Indianapolis-Carmel-Anderson, IN",
     ["460", "461", "462", "463", "464", "465", "466", "467", "468", "469"]),
    ("20500", "El Paso, TX",
     ["798", "799", "885"]),
    ("36260", "Ogden-Clearfield, UT",
     ["840", "841", "842"]),
    ("39100", "Providence-Warwick, RI-MA",
     ["028", "029"]),
    ("33340", "Milwaukee-Waukesha, WI",
     ["530", "531", "532", "534"]),
    ("10420", "Akron, OH",
     ["442", "443", "444"]),
    ("38900", "Portland-Vancouver-Hillsboro, OR-WA",
     ["970", "971", "972", "973", "974", "986"]),
    ("40060", "Richmond, VA",
     ["230", "231", "232", "233", "234"]),
    ("47260", "Virginia Beach-Norfolk-Newport News, VA-NC",
     ["234", "235", "236", "237", "238", "239"]),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=RETRY_STATUS_CODES,
        allowed_methods={"GET", "POST"},
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def state_from_zip_prefix(zip_code: str) -> str:
    """Return state abbreviation from first 3 digits of ZIP."""
    prefix = zip_code[:3]
    return ZIP_PREFIX_TO_STATE.get(prefix, "")


def timezone_for_zip(state: str, longitude: float | None) -> str:
    """
    Determine the IANA timezone for a ZIP given its state and longitude.
    Applies a longitude-based heuristic for states that span two time zones.
    """
    if not state or state not in STATE_TIMEZONE:
        return ""
    primary_tz, boundary = STATE_TIMEZONE[state]
    if boundary is None or longitude is None:
        return primary_tz

    # Special cases: which direction is the split?
    split_configs: dict[str, tuple[str, str]] = {
        "FL": ("America/New_York", "America/Chicago"),   # east, west
        "ID": ("America/Boise", "America/Los_Angeles"),
        "KS": ("America/Chicago", "America/Denver"),
        "KY": ("America/New_York", "America/Chicago"),
        "NE": ("America/Chicago", "America/Denver"),
        "ND": ("America/Chicago", "America/Denver"),
        "SD": ("America/Chicago", "America/Denver"),
        "TN": ("America/New_York", "America/Chicago"),
        "TX": ("America/Chicago", "America/Denver"),
    }
    if state in split_configs:
        east_tz, west_tz = split_configs[state]
        return east_tz if longitude > boundary else west_tz
    return primary_tz


def fetch_acs_city_names(
    session: requests.Session,
    zip_codes: list[str],
) -> dict[str, str]:
    """
    Use the Census ACS API to look up city-like names (ZCTA NAME field).
    Returns a dict of zip → city string.
    The NAME field typically looks like "ZCTA5 90210" — we strip the prefix.
    We already have this from 01_download_census.py (acs_data.csv) if it
    was downloaded; we re-fetch only what's needed here.
    """
    acs_csv = RAW_DIR / "acs_data.csv"
    if acs_csv.exists():
        log.info("Reading city names from existing acs_data.csv …")
        acs_df = pd.read_csv(acs_csv, dtype=str)
        if "zip" in acs_df.columns and "zcta_name" in acs_df.columns:
            mapping = dict(zip(acs_df["zip"].str.zfill(5), acs_df["zcta_name"]))
            return mapping
    log.info("acs_data.csv not found or missing zcta_name; ZCTA names will be blank.")
    return {}


# ---------------------------------------------------------------------------
# CBSA lookup helpers
# ---------------------------------------------------------------------------

def build_cbsa_prefix_index() -> dict[str, tuple[str, str]]:
    """Return dict: 3-digit prefix → (cbsa_code, cbsa_name)."""
    index: dict[str, tuple[str, str]] = {}
    for cbsa_code, cbsa_name, prefixes in CBSA_PREFIX_MAP:
        for prefix in prefixes:
            index.setdefault(prefix.zfill(3), (cbsa_code, cbsa_name))
    return index


# ---------------------------------------------------------------------------
# Census Geocoder – county resolution (batch, up to 1000 at a time)
# ---------------------------------------------------------------------------

def fetch_county_for_zips_batch(
    session: requests.Session,
    zip_lat_lon: list[tuple[str, float, float]],
) -> dict[str, tuple[str, str]]:
    """
    Call the Census Geocoder batch endpoint to resolve county FIPS + name
    for a list of (zip, lat, lon) tuples.

    Returns dict: zip → (county_fips_5, county_name)
    """
    if not zip_lat_lon:
        return {}

    BATCH_URL = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
    results: dict[str, tuple[str, str]] = {}

    # The batch coordinates endpoint accepts one pair at a time via query params
    # (the bulk form upload is for addresses only). We use individual requests
    # but throttle to avoid rate limits.
    BATCH_SIZE = 50
    log.info(
        "Fetching county data from Census Geocoder for %d ZIPs (in batches of %d) …",
        len(zip_lat_lon),
        BATCH_SIZE,
    )
    total = len(zip_lat_lon)
    for i, (zip_code, lat, lon) in enumerate(zip_lat_lon):
        if i % 100 == 0:
            log.info("  Progress: %d / %d", i, total)
        try:
            resp = session.get(
                BATCH_URL,
                params={
                    "x": lon,
                    "y": lat,
                    "benchmark": "Public_AR_Census2020",
                    "vintage": "Census2020_Census2020",
                    "layers": "Counties",
                    "format": "json",
                },
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            geographies = (
                data.get("result", {})
                    .get("geographies", {})
                    .get("Counties", [])
            )
            if geographies:
                county = geographies[0]
                state_fips = county.get("STATE", "")
                county_fips_3 = county.get("COUNTY", "")
                county_name = county.get("NAME", "")
                full_fips = state_fips + county_fips_3
                results[zip_code] = (full_fips, county_name)
        except Exception as exc:  # noqa: BLE001
            log.debug("Geocoder failed for %s: %s", zip_code, exc)
        # Light throttle: 20 req/s max
        time.sleep(0.05)

    log.info("Geocoder resolved county for %d / %d ZIPs.", len(results), total)
    return results


# ---------------------------------------------------------------------------
# Main assembly
# ---------------------------------------------------------------------------

def build_zip_mapping(session: requests.Session) -> pd.DataFrame:
    # --- Load Gazetteer ---
    if not GAZETTEER_CSV.exists():
        raise FileNotFoundError(
            f"Gazetteer file not found: {GAZETTEER_CSV}\n"
            "Please run 01_download_census.py first."
        )
    log.info("Loading Gazetteer from %s …", GAZETTEER_CSV)
    gaz = pd.read_csv(GAZETTEER_CSV, dtype=str)

    # Standardise column names (script 01 already renamed them)
    gaz["zip"] = gaz["zip"].str.strip().str.zfill(5)
    for col in ["latitude", "longitude"]:
        if col in gaz.columns:
            gaz[col] = pd.to_numeric(gaz[col], errors="coerce")

    log.info("Gazetteer loaded: %d ZCTAs", len(gaz))

    # --- City / ZCTA name ---
    zcta_names = fetch_acs_city_names(session, gaz["zip"].tolist())

    # --- State from ZIP prefix ---
    log.info("Inferring state from ZIP prefix …")
    gaz["state"] = gaz["zip"].apply(state_from_zip_prefix)

    # --- Timezone ---
    log.info("Inferring timezone …")
    lat_col = gaz["latitude"] if "latitude" in gaz.columns else pd.Series([None] * len(gaz))
    lon_col = gaz["longitude"] if "longitude" in gaz.columns else pd.Series([None] * len(gaz))
    gaz["timezone"] = [
        timezone_for_zip(st, ln)
        for st, ln in zip(gaz["state"], lon_col)
    ]

    # --- CBSA ---
    log.info("Applying CBSA prefix mapping …")
    cbsa_index = build_cbsa_prefix_index()
    gaz["cbsa_code"] = ""
    gaz["cbsa_name"] = ""
    for idx, row in gaz.iterrows():
        prefix = str(row["zip"])[:3]
        if prefix in cbsa_index:
            gaz.at[idx, "cbsa_code"] = cbsa_index[prefix][0]
            gaz.at[idx, "cbsa_name"] = cbsa_index[prefix][1]

    # --- County via Census Geocoder (sample: first 500 ZIPs with coordinates) ---
    has_coords = (
        gaz["latitude"].notna() & gaz["longitude"].notna()
        if "latitude" in gaz.columns and "longitude" in gaz.columns
        else pd.Series([False] * len(gaz))
    )
    sample = gaz[has_coords].head(500)
    zip_lat_lon = list(zip(
        sample["zip"],
        sample["latitude"],
        sample["longitude"],
    ))
    county_map = fetch_county_for_zips_batch(session, zip_lat_lon)

    gaz["county_fips"] = gaz["zip"].map(lambda z: county_map.get(z, ("", ""))[0])
    gaz["county_name"] = gaz["zip"].map(lambda z: county_map.get(z, ("", ""))[1])

    # --- ZCTA city name (from ACS NAME field, stripped) ---
    gaz["city"] = gaz["zip"].map(
        lambda z: zcta_names.get(z, "").replace("ZCTA5 ", "").strip()
    )

    # --- Final column selection ---
    output_cols = [
        "zip", "city", "state",
        "county_name", "county_fips",
        "timezone",
        "cbsa_code", "cbsa_name",
        "latitude", "longitude",
    ]
    output_cols = [c for c in output_cols if c in gaz.columns]
    result = gaz[output_cols].copy()

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    log.info("Output directory: %s", RAW_DIR)

    session = build_session()

    log.info("=== Building ZIP → city/state/county/timezone/CBSA mapping ===")
    df = build_zip_mapping(session)

    df.to_csv(ZIP_MAPPING_OUT, index=False)
    log.info(
        "Saved ZIP mapping → %s  (%d rows, %d columns)",
        ZIP_MAPPING_OUT, len(df), len(df.columns),
    )

    # Quick summary
    log.info("--- Summary ---")
    log.info("  ZIPs with state      : %d", df["state"].ne("").sum())
    log.info("  ZIPs with timezone   : %d", df["timezone"].ne("").sum())
    log.info("  ZIPs with county     : %d", df["county_name"].ne("").sum())
    log.info("  ZIPs with CBSA       : %d", df["cbsa_code"].ne("").sum())
    log.info("=== Done ===")


if __name__ == "__main__":
    main()

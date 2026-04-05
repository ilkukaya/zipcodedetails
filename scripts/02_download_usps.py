"""
02_download_usps.py

Builds a comprehensive ZIP-code-to-city/state/county/timezone/CBSA mapping
using Census Bureau relationship files (fast bulk downloads).

Strategy
--------
1. Read the Gazetteer file already downloaded by 01_download_census.py.
2. Download Census ZCTA-to-County relationship file for county names.
3. Download Census ZCTA-to-Place relationship file for city names.
4. Apply a built-in state-prefix → abbreviation lookup table.
5. Apply a built-in state → timezone table (with longitude heuristic for
   split-timezone states).
6. Apply a built-in CBSA prefix lookup for major metro areas.
7. Add state full names, DST flag, and zip_type.

Output
------
data/raw/zip_mapping.csv  — one row per ZCTA with columns:
  zip, city, state, state_full, county, timezone, dst, zip_type,
  cbsa_code, cbsa_name, latitude, longitude
"""

import io
import json
import logging
import os
import re
import time
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
GAZETTEER_CSV = RAW_DIR / "gazetteer.csv"
ZIP_MAPPING_OUT = RAW_DIR / "zip_mapping.csv"

# Census 2020 relationship files
ZCTA_COUNTY_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/"
    "zcta520/tab20_zcta520_county20_natl.txt"
)
ZCTA_PLACE_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/"
    "zcta520/tab20_zcta520_place20_natl.txt"
)

MAX_RETRIES = 5
BACKOFF_FACTOR = 1.5
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
REQUEST_TIMEOUT = 120

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
# State full names
# ---------------------------------------------------------------------------

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
# State timezone mapping
# ---------------------------------------------------------------------------

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
    "FL": ("America/New_York", -85.5),
    "GA": ("America/New_York", None),
    "GU": ("Pacific/Guam", None),
    "HI": ("Pacific/Honolulu", None),
    "ID": ("America/Boise", -113.5),
    "IL": ("America/Chicago", None),
    "IN": ("America/Indiana/Indianapolis", None),
    "IA": ("America/Chicago", None),
    "KS": ("America/Chicago", -101.5),
    "KY": ("America/New_York", -84.8),
    "LA": ("America/Chicago", None),
    "ME": ("America/New_York", None),
    "MD": ("America/New_York", None),
    "MA": ("America/New_York", None),
    "MI": ("America/Detroit", None),
    "MN": ("America/Chicago", None),
    "MS": ("America/Chicago", None),
    "MO": ("America/Chicago", None),
    "MT": ("America/Denver", None),
    "NE": ("America/Chicago", -104.0),
    "NV": ("America/Los_Angeles", None),
    "NH": ("America/New_York", None),
    "NJ": ("America/New_York", None),
    "NM": ("America/Denver", None),
    "NY": ("America/New_York", None),
    "NC": ("America/New_York", None),
    "ND": ("America/Chicago", -101.5),
    "OH": ("America/New_York", None),
    "OK": ("America/Chicago", None),
    "OR": ("America/Los_Angeles", None),
    "PA": ("America/New_York", None),
    "PR": ("America/Puerto_Rico", None),
    "RI": ("America/New_York", None),
    "SC": ("America/New_York", None),
    "SD": ("America/Chicago", -104.0),
    "TN": ("America/Chicago", -84.5),
    "TX": ("America/Chicago", -104.8),
    "UT": ("America/Denver", None),
    "VT": ("America/New_York", None),
    "VA": ("America/New_York", None),
    "VI": ("America/St_Thomas", None),
    "WA": ("America/Los_Angeles", None),
    "WV": ("America/New_York", None),
    "WI": ("America/Chicago", None),
    "WY": ("America/Denver", None),
}

# Timezones that do NOT observe DST
NO_DST_TIMEZONES = {
    "America/Phoenix", "Pacific/Honolulu", "America/St_Thomas",
    "America/Puerto_Rico", "Pacific/Guam", "Pacific/Pago_Pago",
}

# ---------------------------------------------------------------------------
# ZIP prefix → state abbreviation
# ---------------------------------------------------------------------------

ZIP_PREFIX_TO_STATE: dict[str, str] = {}

def _add_range(start, end, state):
    for p in range(start, end):
        ZIP_PREFIX_TO_STATE[str(p).zfill(3)] = state

# Northeast
_add_range(5, 5, "NY")  # 005 = NY (IRS)
ZIP_PREFIX_TO_STATE["005"] = "NY"
ZIP_PREFIX_TO_STATE["006"] = "PR"
ZIP_PREFIX_TO_STATE["007"] = "PR"
ZIP_PREFIX_TO_STATE["008"] = "PR"
ZIP_PREFIX_TO_STATE["009"] = "PR"
_add_range(10, 28, "MA")
ZIP_PREFIX_TO_STATE["028"] = "RI"
ZIP_PREFIX_TO_STATE["029"] = "RI"
_add_range(30, 39, "NH")
_add_range(39, 50, "ME")
for p in [50, 51, 52, 53, 54, 56, 57, 58, 59]:
    ZIP_PREFIX_TO_STATE[str(p).zfill(3)] = "VT"
ZIP_PREFIX_TO_STATE["055"] = "MA"
_add_range(60, 70, "CT")
_add_range(70, 90, "NJ")
_add_range(90, 99, "AE")  # Military APO/FPO
_add_range(100, 150, "NY")
_add_range(150, 197, "PA")
ZIP_PREFIX_TO_STATE["197"] = "DE"
ZIP_PREFIX_TO_STATE["198"] = "DE"
ZIP_PREFIX_TO_STATE["199"] = "DE"
# DC & MD
ZIP_PREFIX_TO_STATE["200"] = "DC"
ZIP_PREFIX_TO_STATE["201"] = "VA"  # Northern VA (some overlap)
ZIP_PREFIX_TO_STATE["202"] = "DC"
ZIP_PREFIX_TO_STATE["203"] = "DC"
ZIP_PREFIX_TO_STATE["204"] = "DC"
ZIP_PREFIX_TO_STATE["205"] = "DC"
_add_range(206, 220, "MD")
_add_range(220, 247, "VA")
_add_range(247, 270, "WV")
_add_range(270, 290, "NC")
_add_range(290, 300, "SC")
_add_range(300, 320, "GA")
_add_range(320, 350, "FL")
_add_range(350, 370, "AL")
_add_range(370, 386, "TN")
_add_range(386, 398, "MS")
ZIP_PREFIX_TO_STATE["398"] = "GA"
ZIP_PREFIX_TO_STATE["399"] = "GA"
_add_range(400, 428, "KY")
_add_range(430, 460, "OH")
_add_range(460, 480, "IN")
_add_range(480, 500, "MI")
_add_range(500, 529, "IA")
_add_range(530, 550, "WI")
_add_range(550, 568, "MN")
_add_range(570, 578, "SD")
_add_range(580, 589, "ND")
_add_range(590, 600, "MT")
_add_range(600, 630, "IL")
_add_range(630, 659, "MO")
ZIP_PREFIX_TO_STATE["659"] = "KS"
_add_range(660, 680, "KS")
_add_range(680, 694, "NE")
_add_range(700, 715, "LA")
ZIP_PREFIX_TO_STATE["715"] = "LA"
_add_range(716, 730, "AR")
_add_range(730, 750, "OK")
_add_range(750, 800, "TX")
_add_range(800, 817, "CO")
_add_range(820, 831, "WY")
ZIP_PREFIX_TO_STATE["831"] = "WY"
_add_range(832, 839, "ID")
_add_range(840, 848, "UT")
_add_range(850, 866, "AZ")
_add_range(870, 885, "NM")
ZIP_PREFIX_TO_STATE["885"] = "TX"  # El Paso area
_add_range(889, 899, "NV")
ZIP_PREFIX_TO_STATE["899"] = "NV"
_add_range(900, 962, "CA")
_add_range(962, 967, "AP")  # Military Pacific
_add_range(967, 969, "HI")
_add_range(970, 979, "OR")
ZIP_PREFIX_TO_STATE["979"] = "OR"
_add_range(980, 995, "WA")
_add_range(995, 1000, "AK")

# Fill known gaps
ZIP_PREFIX_TO_STATE.setdefault("001", "NY")   # Unique NY
ZIP_PREFIX_TO_STATE.setdefault("002", "NY")
ZIP_PREFIX_TO_STATE.setdefault("003", "NY")
ZIP_PREFIX_TO_STATE.setdefault("004", "NY")
_add_range(428, 430, "KY")   # Gap between KY and OH
_add_range(568, 570, "MN")   # Gap between MN and SD
_add_range(578, 580, "SD")   # Gap between SD and ND
_add_range(694, 700, "NE")   # Gap between NE and LA
_add_range(817, 820, "CO")   # Gap between CO and WY
_add_range(848, 850, "UT")   # Gap between UT and AZ
_add_range(865, 870, "AZ")   # Gap between AZ and NM
_add_range(885, 889, "NM")   # Gap between NM and NV (except 885=TX already set)
ZIP_PREFIX_TO_STATE.setdefault("962", "GU")   # Guam / AP military
ZIP_PREFIX_TO_STATE.setdefault("963", "GU")
ZIP_PREFIX_TO_STATE.setdefault("964", "GU")
ZIP_PREFIX_TO_STATE.setdefault("965", "MP")   # Northern Mariana Islands
ZIP_PREFIX_TO_STATE.setdefault("966", "MP")
ZIP_PREFIX_TO_STATE.setdefault("969", "GU")

# ---------------------------------------------------------------------------
# CBSA prefix map (major metros)
# ---------------------------------------------------------------------------

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
     ["923", "924", "925"]),
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
    ("12420", "Austin-Round Rock-Georgetown, TX",
     ["786", "787", "788"]),
    ("16740", "Charlotte-Concord-Gastonia, NC-SC",
     ["280", "281", "282", "283", "284", "290", "291", "292"]),
    ("31140", "Louisville/Jefferson County, KY-IN",
     ["400", "401", "402", "410", "411", "412"]),
    ("26900", "Indianapolis-Carmel-Anderson, IN",
     ["460", "461", "462", "463", "464", "465", "466", "467", "468", "469"]),
    ("41940", "San Jose-Sunnyvale-Santa Clara, CA",
     ["950", "951"]),
    ("33460", "Minneapolis-St. Paul-Bloomington, MN-WI",
     ["550", "551", "553", "554", "555", "556", "557"]),
    ("19740", "Denver-Aurora-Lakewood, CO",
     ["800", "801", "802", "803", "804", "805"]),
    ("38900", "Portland-Vancouver-Hillsboro, OR-WA",
     ["970", "971", "972", "973", "974", "986"]),
    ("40060", "Richmond, VA",
     ["230", "231", "232", "233", "234"]),
    ("47260", "Virginia Beach-Norfolk-Newport News, VA-NC",
     ["234", "235", "236", "237", "238", "239"]),
    ("41620", "Salt Lake City, UT",
     ["840", "841", "842", "843", "844", "845"]),
    ("13820", "Birmingham-Hoover, AL",
     ["350", "351", "352"]),
    ("10580", "Albany-Schenectady-Troy, NY",
     ["120", "121", "122", "123"]),
    ("15380", "Buffalo-Cheektowaga, NY",
     ["140", "141", "142", "143"]),
    ("40380", "Rochester, NY",
     ["144", "145", "146", "147", "148", "149"]),
    ("44060", "Syracuse, NY",
     ["130", "131", "132"]),
    ("36540", "Omaha-Council Bluffs, NE-IA",
     ["680", "681", "682", "683", "684"]),
    ("46060", "Tucson, AZ",
     ["856", "857"]),
    ("21340", "El Paso, TX",
     ["798", "799"]),
    ("10740", "Albuquerque, NM",
     ["870", "871", "872", "873"]),
    ("25540", "Hartford-East Hartford-Middletown, CT",
     ["060", "061", "062", "063", "064", "065", "066"]),
    ("39100", "Providence-Warwick, RI-MA",
     ["028", "029"]),
    ("33340", "Milwaukee-Waukesha, WI",
     ["530", "531", "532", "534"]),
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
        allowed_methods={"GET"},
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def download_text(session: requests.Session, url: str, label: str) -> str:
    """Download a URL and return the text content."""
    log.info("Downloading %s from %s", label, url)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            log.info("  Downloaded %.2f MB for %s", len(resp.content) / 1_048_576, label)
            return resp.text
        except requests.RequestException as exc:
            log.warning("  Attempt %d/%d failed for %s: %s", attempt, MAX_RETRIES, label, exc)
            if attempt < MAX_RETRIES:
                sleep_time = BACKOFF_FACTOR * (2 ** (attempt - 1))
                log.info("  Retrying in %.1f s …", sleep_time)
                time.sleep(sleep_time)
            else:
                raise RuntimeError(f"Failed to download {label} after {MAX_RETRIES} attempts") from exc


def state_from_zip_prefix(zip_code: str) -> str:
    prefix = zip_code[:3]
    return ZIP_PREFIX_TO_STATE.get(prefix, "")


def timezone_for_zip(state: str, longitude: float | None) -> str:
    if not state or state not in STATE_TIMEZONE:
        return ""
    primary_tz, boundary = STATE_TIMEZONE[state]
    if boundary is None or longitude is None:
        return primary_tz

    split_configs: dict[str, tuple[str, str]] = {
        "FL": ("America/New_York", "America/Chicago"),
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


def dst_for_timezone(tz: str) -> bool:
    """Return True if the timezone observes Daylight Saving Time."""
    if not tz:
        return False
    return tz not in NO_DST_TIMEZONES


def build_cbsa_prefix_index() -> dict[str, tuple[str, str]]:
    index: dict[str, tuple[str, str]] = {}
    for cbsa_code, cbsa_name, prefixes in CBSA_PREFIX_MAP:
        for prefix in prefixes:
            index.setdefault(prefix.zfill(3), (cbsa_code, cbsa_name))
    return index


def clean_place_name(name: str) -> str:
    """Clean Census place name: 'New York city' → 'New York'."""
    if not name:
        return ""
    # Remove suffixes like 'city', 'town', 'village', 'CDP', 'borough', etc.
    name = re.sub(
        r'\s+(city|town|village|CDP|borough|municipality|plantation|comunidad|zona urbana)$',
        '', name, flags=re.IGNORECASE
    )
    return name.strip()


def clean_county_name(name: str) -> str:
    """Clean Census county name to standard format: 'Kings County' stays, etc."""
    if not name:
        return ""
    return name.strip()


# ---------------------------------------------------------------------------
# Download and parse Census relationship files
# ---------------------------------------------------------------------------

def download_zcta_county_mapping(session: requests.Session) -> dict[str, str]:
    """
    Download the ZCTA-to-County relationship file.
    Returns dict: ZCTA (zip) → county name (the county with largest area overlap).
    """
    text = download_text(session, ZCTA_COUNTY_URL, "ZCTA-County relationship")
    df = pd.read_csv(io.StringIO(text), sep="|", dtype=str)
    log.info("ZCTA-County file columns: %s", list(df.columns))
    log.info("ZCTA-County file rows: %d", len(df))

    # Columns: GEOID_ZCTA5_20, GEOID_COUNTY_20, NAMELSAD_ZCTA5_20,
    #          NAMELSAD_COUNTY_20, AREALAND_PART, etc.
    zcta_col = [c for c in df.columns if "GEOID_ZCTA" in c.upper()][0]
    county_name_col = [c for c in df.columns if "NAMELSAD_COUNTY" in c.upper()][0]
    area_col = [c for c in df.columns if "AREALAND_PART" in c.upper() and "PCT" not in c.upper()]

    if area_col:
        area_col = area_col[0]
        df[area_col] = pd.to_numeric(df[area_col], errors="coerce").fillna(0)
    else:
        # If no area column, just take first occurrence
        area_col = None

    df[zcta_col] = df[zcta_col].str.strip().str.zfill(5)

    # For each ZCTA, pick the county with the largest overlap area
    result: dict[str, str] = {}
    if area_col:
        idx = df.groupby(zcta_col)[area_col].idxmax()
        for i in idx:
            row = df.loc[i]
            zcta = row[zcta_col]
            county = clean_county_name(str(row[county_name_col]))
            result[zcta] = county
    else:
        for _, row in df.drop_duplicates(subset=[zcta_col], keep="first").iterrows():
            zcta = row[zcta_col]
            county = clean_county_name(str(row[county_name_col]))
            result[zcta] = county

    log.info("County mapping built: %d ZCTAs", len(result))
    return result


def download_zcta_place_mapping(session: requests.Session) -> dict[str, str]:
    """
    Download the ZCTA-to-Place relationship file.
    Returns dict: ZCTA (zip) → city name (the place with largest area overlap).
    """
    text = download_text(session, ZCTA_PLACE_URL, "ZCTA-Place relationship")
    df = pd.read_csv(io.StringIO(text), sep="|", dtype=str)
    log.info("ZCTA-Place file columns: %s", list(df.columns))
    log.info("ZCTA-Place file rows: %d", len(df))

    zcta_col = [c for c in df.columns if "GEOID_ZCTA" in c.upper()][0]
    place_name_col = [c for c in df.columns if "NAMELSAD_PLACE" in c.upper()][0]
    area_col = [c for c in df.columns if "AREALAND_PART" in c.upper() and "PCT" not in c.upper()]

    if area_col:
        area_col = area_col[0]
        df[area_col] = pd.to_numeric(df[area_col], errors="coerce").fillna(0)
    else:
        area_col = None

    df[zcta_col] = df[zcta_col].str.strip().str.zfill(5)

    result: dict[str, str] = {}
    if area_col:
        idx = df.groupby(zcta_col)[area_col].idxmax()
        for i in idx:
            row = df.loc[i]
            zcta = row[zcta_col]
            city = clean_place_name(str(row[place_name_col]))
            result[zcta] = city
    else:
        for _, row in df.drop_duplicates(subset=[zcta_col], keep="first").iterrows():
            zcta = row[zcta_col]
            city = clean_place_name(str(row[place_name_col]))
            result[zcta] = city

    log.info("Place (city) mapping built: %d ZCTAs", len(result))
    return result


# ---------------------------------------------------------------------------
# Main assembly
# ---------------------------------------------------------------------------

def load_usps_locale(usps_path: Path) -> dict[str, str]:
    """
    Load the USPS ZIP_Locale_Detail file and return a dict: ZIP → locale name.
    Uses the ZIP_DETAIL sheet, taking the first locale name per ZIP.
    """
    if not usps_path.exists():
        log.warning("USPS locale file not found at %s — skipping.", usps_path)
        return {}

    log.info("Loading USPS ZIP_Locale_Detail from %s …", usps_path)
    try:
        import xlrd
        wb = xlrd.open_workbook(str(usps_path))
        ws = wb.sheet_by_name("ZIP_DETAIL")

        result: dict[str, str] = {}
        for i in range(1, ws.nrows):
            zip_code = str(ws.cell_value(i, 4)).strip().zfill(5)
            locale_name = str(ws.cell_value(i, 5)).strip()
            if zip_code and locale_name and zip_code not in result:
                # Title-case the locale name (USPS uses uppercase)
                result[zip_code] = locale_name.title()

        log.info("USPS locale mapping loaded: %d ZIPs", len(result))
        return result
    except Exception as exc:
        log.warning("Failed to read USPS locale file: %s", exc)
        return {}


def build_zip_mapping(session: requests.Session) -> pd.DataFrame:
    # --- Load Gazetteer ---
    if not GAZETTEER_CSV.exists():
        raise FileNotFoundError(
            f"Gazetteer file not found: {GAZETTEER_CSV}\n"
            "Please run 01_download_census.py first."
        )
    log.info("Loading Gazetteer from %s …", GAZETTEER_CSV)
    gaz = pd.read_csv(GAZETTEER_CSV, dtype=str)
    gaz["zip"] = gaz["zip"].str.strip().str.zfill(5)
    for col in ["latitude", "longitude"]:
        if col in gaz.columns:
            gaz[col] = pd.to_numeric(gaz[col], errors="coerce")
    log.info("Gazetteer loaded: %d ZCTAs", len(gaz))

    # --- Download city/county from Census relationship files ---
    log.info("=== Downloading Census relationship files ===")
    county_map = download_zcta_county_mapping(session)
    city_map = download_zcta_place_mapping(session)

    # --- Load USPS locale data as supplementary city source ---
    usps_path = Path(__file__).resolve().parent.parent / "data" / "ZIP_Locale_Detail.xls"
    usps_locale_map = load_usps_locale(usps_path)

    # --- Apply city names (prefer USPS locale, fall back to Census place) ---
    def get_city(zip_code: str) -> str:
        # USPS locale name is the official postal name for the ZIP
        if zip_code in usps_locale_map:
            return usps_locale_map[zip_code]
        # Fall back to Census place name
        if zip_code in city_map:
            return city_map[zip_code]
        return ""

    gaz["city"] = gaz["zip"].apply(get_city)

    # --- State from ZIP prefix ---
    log.info("Inferring state from ZIP prefix …")
    gaz["state"] = gaz["zip"].apply(state_from_zip_prefix)

    # --- State full name ---
    gaz["state_full"] = gaz["state"].map(STATE_FULL_NAMES).fillna("")

    # --- County ---
    gaz["county"] = gaz["zip"].map(county_map).fillna("")

    # --- Timezone ---
    log.info("Inferring timezone …")
    lon_col = gaz["longitude"] if "longitude" in gaz.columns else pd.Series([None] * len(gaz))
    gaz["timezone"] = [
        timezone_for_zip(st, ln)
        for st, ln in zip(gaz["state"], lon_col)
    ]

    # --- DST ---
    gaz["dst"] = gaz["timezone"].apply(dst_for_timezone)

    # --- ZIP type ---
    gaz["zip_type"] = "STANDARD"

    # --- CBSA ---
    log.info("Applying CBSA prefix mapping …")
    cbsa_index = build_cbsa_prefix_index()
    cbsa_codes = []
    cbsa_names = []
    for _, row in gaz.iterrows():
        prefix = str(row["zip"])[:3]
        if prefix in cbsa_index:
            cbsa_codes.append(cbsa_index[prefix][0])
            cbsa_names.append(cbsa_index[prefix][1])
        else:
            cbsa_codes.append("")
            cbsa_names.append("")
    gaz["cbsa_code"] = cbsa_codes
    gaz["cbsa_name"] = cbsa_names

    # --- Final column selection ---
    output_cols = [
        "zip", "city", "state", "state_full", "county",
        "timezone", "dst", "zip_type",
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
    log.info("  ZIPs with city       : %d", df["city"].ne("").sum())
    log.info("  ZIPs with state      : %d", df["state"].ne("").sum())
    log.info("  ZIPs with county     : %d", df["county"].ne("").sum())
    log.info("  ZIPs with timezone   : %d", df["timezone"].ne("").sum())
    log.info("  ZIPs with CBSA       : %d", df["cbsa_code"].ne("").sum())
    log.info("=== Done ===")


if __name__ == "__main__":
    main()

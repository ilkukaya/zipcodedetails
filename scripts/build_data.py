#!/usr/bin/env python3
"""
build_data.py

Single data pipeline for zipcodedetails.com.
Input:  data/raw/pseo_zipcodes_full.json
Output: data/zips/<zip>.json (39,398 files)
        data/state_index.json
        data/city_index.json
        data/county_index.json
        data/popular_zips.json
        public/search_index.json
        public/_redirects        (Netlify city-slug redirects)
"""

import json
import math
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA = PROJECT_ROOT / "data" / "raw" / "pseo_zipcodes_full.json"
ZIPS_DIR = PROJECT_ROOT / "data" / "zips"
DATA_DIR = PROJECT_ROOT / "data"
PUBLIC_DIR = PROJECT_ROOT / "public"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
EARTH_RADIUS_MI = 3958.8

STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
    "PR": "Puerto Rico", "VI": "Virgin Islands", "GU": "Guam",
    "AS": "American Samoa", "MP": "Northern Mariana Islands",
}

# Timezones that do NOT observe DST
NO_DST_ZONES = {
    "America/Phoenix",
    "Pacific/Honolulu",
    "America/Puerto_Rico",
    "Pacific/Guam",
    "Pacific/Saipan",
    "Pacific/Pago_Pago",
}


# ---------------------------------------------------------------------------
# Slug helpers
# ---------------------------------------------------------------------------

def slugify(text: str) -> str:
    s = text.lower()
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"-+", "-", s)
    return s.strip("-")


def city_slug(city: str, state: str) -> str:
    return slugify(city) + "-" + state.lower()


def county_slug(county: str, state: str) -> str:
    return slugify(county) + "-" + state.lower()


# ---------------------------------------------------------------------------
# Nearest-neighbor computation
# ---------------------------------------------------------------------------

def compute_nearest_numpy(state_zips: list, n: int = 8) -> dict:
    """Vectorized haversine nearest-neighbor using numpy."""
    N = len(state_zips)
    if N <= 1:
        return {z["zipcode"]: [] for z in state_zips}

    lats = np.array([z["lat"] for z in state_zips], dtype=np.float64)
    lngs = np.array([z["lng"] for z in state_zips], dtype=np.float64)
    lat_r = np.radians(lats)
    lng_r = np.radians(lngs)

    result = {}
    k = min(n, N - 1)

    # Process row by row to avoid huge memory allocation for large states
    for i, z in enumerate(state_zips):
        dlat = lat_r - lat_r[i]
        dlng = lng_r - lng_r[i]
        a = (np.sin(dlat / 2) ** 2
             + math.cos(lat_r[i]) * np.cos(lat_r) * np.sin(dlng / 2) ** 2)
        distances = EARTH_RADIUS_MI * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
        distances[i] = np.inf

        nearest_idx = np.argpartition(distances, k)[:k]
        sorted_order = np.argsort(distances[nearest_idx])
        nearest_idx_sorted = nearest_idx[sorted_order]

        result[z["zipcode"]] = [
            {"zip": state_zips[j]["zipcode"], "distance_mi": round(float(distances[j]), 2)}
            for j in nearest_idx_sorted
        ]

    return result


def haversine(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = EARTH_RADIUS_MI
    d_lat = math.radians(lat2 - lat1)
    d_lng = math.radians(lng2 - lng1)
    a = (math.sin(d_lat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(d_lng / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def compute_nearest_pure(state_zips: list, n: int = 8) -> dict:
    """Pure-Python fallback for nearest-neighbor."""
    result = {}
    k = min(n, len(state_zips) - 1)
    for i, z in enumerate(state_zips):
        dists = []
        for j, w in enumerate(state_zips):
            if i == j:
                continue
            d = haversine(z["lat"], z["lng"], w["lat"], w["lng"])
            dists.append((d, w["zipcode"]))
        dists.sort()
        result[z["zipcode"]] = [
            {"zip": zc, "distance_mi": round(d, 2)} for d, zc in dists[:k]
        ]
    return result


def compute_nearest(state_zips: list, n: int = 8) -> dict:
    if HAS_NUMPY:
        return compute_nearest_numpy(state_zips, n)
    return compute_nearest_pure(state_zips, n)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main() -> None:
    t0 = time.time()

    # --- Load raw data -------------------------------------------------
    if not RAW_DATA.exists():
        print(f"ERROR: {RAW_DATA} not found.", file=sys.stderr)
        print("Place pseo_zipcodes_full.json in data/raw/ and re-run.", file=sys.stderr)
        sys.exit(1)

    print(f"Loading {RAW_DATA} …")
    with open(RAW_DATA, encoding="utf-8") as f:
        raw = json.load(f)

    zips: list[dict] = raw["zipcodes"]
    print(f"  Loaded {len(zips):,} records.")

    # --- Sanity checks -------------------------------------------------
    assert len(zips) > 35_000, f"Expected 35K+ records, got {len(zips)}"
    missing_zip = [z for z in zips if not z.get("zipcode")]
    assert not missing_zip, f"{len(missing_zip)} records missing zipcode"
    bad_leading = [z for z in zips if len(str(z["zipcode"])) != 5]
    assert not bad_leading, f"{len(bad_leading)} ZIPs with wrong length"
    null_lat = [z for z in zips if z.get("lat") is None]
    if null_lat:
        print(f"  WARNING: {len(null_lat)} records with null lat — skipping")

    # --- Ensure directories --------------------------------------------
    ZIPS_DIR.mkdir(parents=True, exist_ok=True)
    PUBLIC_DIR.mkdir(parents=True, exist_ok=True)

    # --- Group by state ------------------------------------------------
    by_state: dict[str, list] = defaultdict(list)
    for z in zips:
        if z.get("lat") is not None:
            by_state[z["state"]].append(z)

    # --- Compute nearest neighbors per state ---------------------------
    print("Computing nearest neighbors …")
    nearest_map: dict[str, list] = {}
    for state, state_zips in sorted(by_state.items()):
        neighbors = compute_nearest(state_zips, n=8)
        nearest_map.update(neighbors)
        print(f"  {state}: {len(state_zips)} ZIPs processed", end="\r")
    print(f"\n  Done in {time.time() - t0:.1f}s")

    # --- Build index structures ----------------------------------------
    state_index: dict[str, dict] = {}
    city_index: dict[str, dict] = {}
    county_index: dict[str, dict] = {}
    popular_candidates: list[dict] = []
    search_index: list[dict] = []

    for z in zips:
        state = z["state"]
        state_full = STATE_NAMES.get(state, state)
        city = z["major_city"]
        county = z["county"] or ""
        zipcode = z["zipcode"]

        # State index
        if state not in state_index:
            state_index[state] = {"name": state_full, "zip_count": 0, "zips": []}
        state_index[state]["zip_count"] += 1
        state_index[state]["zips"].append(zipcode)

        # City index
        cslug = city_slug(city, state)
        if cslug not in city_index:
            city_index[cslug] = {
                "city": city,
                "state": state,
                "state_full": state_full,
                "county": county,
                "zips": [],
            }
        city_index[cslug]["zips"].append(zipcode)

        # County index
        if county:
            coslug = county_slug(county, state)
            if coslug not in county_index:
                county_index[coslug] = {
                    "county": county,
                    "state": state,
                    "state_full": state_full,
                    "zips": [],
                }
            county_index[coslug]["zips"].append(zipcode)

        # Search index
        search_index.append({"zip": zipcode, "city": city, "state": state})

        # Popular candidates (STANDARD + population data)
        if z["zipcode_type"] == "STANDARD" and z.get("population"):
            popular_candidates.append({
                "zip": zipcode,
                "city": city,
                "state": state,
                "state_full": state_full,
                "county": county,
                "population": z["population"],
                "median_household_income": z.get("median_household_income"),
                "median_home_value": z.get("median_home_value"),
                "lat": z["lat"],
                "lng": z["lng"],
            })

    # Finalize county zip_count
    for v in county_index.values():
        v["zip_count"] = len(v["zips"])

    # Sort state zips
    for s in state_index.values():
        s["zips"].sort()

    # Popular ZIPs: top 100 by population
    popular_zips = sorted(popular_candidates, key=lambda x: -(x["population"] or 0))[:100]

    # --- Generate individual ZIP JSON files ----------------------------
    print("Writing ZIP JSON files …")
    written = 0
    for z in zips:
        zipcode = z["zipcode"]
        state = z["state"]
        state_full = STATE_NAMES.get(state, state)
        city = z["major_city"]
        county = z["county"] or ""
        timezone = z.get("timezone") or ""
        dst = timezone not in NO_DST_ZONES if timezone else False

        coslug = county_slug(county, state) if county else ""

        # bounds dict
        bounds = None
        if z.get("bounds_west") is not None:
            bounds = {
                "west": z["bounds_west"],
                "east": z["bounds_east"],
                "north": z["bounds_north"],
                "south": z["bounds_south"],
            }

        record = {
            "zip": zipcode,
            "city": city,
            "state": state,
            "state_full": state_full,
            "county": county,
            "county_slug": coslug,
            "timezone": timezone,
            "dst": dst,
            "lat": z["lat"],
            "lng": z["lng"],
            "zip_type": z.get("zipcode_type", "STANDARD"),
            "land_area_sqmi": z.get("land_area_in_sqmi"),
            "water_area_sqmi": z.get("water_area_in_sqmi"),
            "population": z.get("population"),
            "population_density": z.get("population_density"),
            "housing_units": z.get("housing_units"),
            "occupied_housing_units": z.get("occupied_housing_units"),
            "median_home_value": z.get("median_home_value"),
            "median_household_income": z.get("median_household_income"),
            "bounds": bounds,
            "surrounding_zips": nearest_map.get(zipcode, []),
        }

        (ZIPS_DIR / f"{zipcode}.json").write_text(
            json.dumps(record, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        written += 1
        if written % 5000 == 0:
            print(f"  {written:,} / {len(zips):,}", end="\r")

    print(f"  Wrote {written:,} ZIP files.                  ")

    # --- Write index files --------------------------------------------
    print("Writing index files …")
    (DATA_DIR / "state_index.json").write_text(
        json.dumps(state_index, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    (DATA_DIR / "city_index.json").write_text(
        json.dumps(city_index, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    (DATA_DIR / "county_index.json").write_text(
        json.dumps(county_index, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    (DATA_DIR / "popular_zips.json").write_text(
        json.dumps(popular_zips, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    (PUBLIC_DIR / "search_index.json").write_text(
        json.dumps(search_index, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )

    print(f"  States: {len(state_index)}, Cities: {len(city_index)}, "
          f"Counties: {len(county_index)}")

    # --- Generate _redirects ------------------------------------------
    print("Generating _redirects …")
    redirects_lines = [
        "# City slug redirects (post_office_city → major_city, generated by build_data.py)",
    ]
    city_slugs_set = set(city_index.keys())

    seen_redirects: set[str] = set()
    for z in zips:
        poc = z.get("post_office_city") or ""
        mc = z["major_city"]
        state = z["state"]
        if not poc:
            continue
        city_part = poc.rsplit(",", 1)[0].strip()
        old_slug = city_slug(city_part, state)
        new_slug = city_slug(mc, state)
        if old_slug == new_slug:
            continue
        # Only redirect if old slug has no valid page in new system
        if old_slug in city_slugs_set:
            continue
        if old_slug in seen_redirects:
            continue
        seen_redirects.add(old_slug)
        redirects_lines.append(f"/city/{old_slug} /city/{new_slug} 301")

    (PUBLIC_DIR / "_redirects").write_text(
        "\n".join(redirects_lines) + "\n", encoding="utf-8"
    )
    print(f"  Added {len(seen_redirects)} city slug redirects.")

    # --- Done ---------------------------------------------------------
    elapsed = time.time() - t0
    print(f"\nBuild complete in {elapsed:.1f}s")
    print(f"  {written:,} ZIP files in data/zips/")
    print(f"  {len(state_index)} states, {len(city_index):,} cities, "
          f"{len(county_index):,} counties")


if __name__ == "__main__":
    main()

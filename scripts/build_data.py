#!/usr/bin/env python3
"""
build_data.py

Single data pipeline for ZIPCodeDetails.
Input:  data/raw/pseo_zipcodes_full.json
        (falls back to data/pseo_zipcodes_full.json.gz, then ./pseo_zipcodes_full.rar)
Output: data/zips/<zip>.json        one record per ZIP, enriched with state context
        data/state_index.json       per-state aggregates
        data/city_index.json        per-city aggregates + nearby cities
        data/county_index.json      per-county aggregates
        data/popular_zips.json      most populous ZIPs nationally
        data/rankings.json          national + per-state top lists
        data/meta.json              totals and build date
        public/zip-index.json       compact [zip, city, state, lat, lng, pop] rows for
                                    client-side search and the distance/radius tools
        public/_redirects           Netlify city-slug redirects
"""

import gzip
import json
import math
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from datetime import date
from pathlib import Path
import re

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA = PROJECT_ROOT / "data" / "raw" / "pseo_zipcodes_full.json"
RAW_GZIP = PROJECT_ROOT / "data" / "pseo_zipcodes_full.json.gz"
RAW_ARCHIVE = PROJECT_ROOT / "pseo_zipcodes_full.rar"
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
    "PR": "Puerto Rico", "VI": "U.S. Virgin Islands", "GU": "Guam",
    "AS": "American Samoa", "MP": "Northern Mariana Islands",
    "FM": "Federated States of Micronesia", "MH": "Marshall Islands", "PW": "Palau",
}

# Timezones that do NOT observe DST
NO_DST_ZONES = {
    "America/Phoenix",
    "Pacific/Honolulu",
    "America/Puerto_Rico",
    "America/St_Thomas",
    "Pacific/Guam",
    "Pacific/Saipan",
    "Pacific/Pago_Pago",
    "Pacific/Chuuk",
    "Pacific/Pohnpei",
    "Pacific/Majuro",
    "Pacific/Palau",
}

# Minimum population for income / home-value rankings (keeps tiny ZIPs from
# dominating "richest" lists with noisy survey estimates)
RANK_MIN_POP = 1000


# ---------------------------------------------------------------------------
# Helpers
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


def median(values: list):
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    n = len(vals)
    mid = n // 2
    if n % 2:
        return vals[mid]
    return (vals[mid - 1] + vals[mid]) / 2


def weighted_avg(pairs: list):
    """pairs: list of (value, weight). Ignores None values / zero weights."""
    num = 0.0
    den = 0.0
    for v, w in pairs:
        if v is None or not w:
            continue
        num += v * w
        den += w
    return round(num / den) if den else None


def percentile_rank(sorted_vals: list, v) -> int | None:
    """Share of values strictly below v, as an integer 0–100."""
    if v is None or not sorted_vals:
        return None
    lo, hi = 0, len(sorted_vals)
    while lo < hi:
        mid = (lo + hi) // 2
        if sorted_vals[mid] < v:
            lo = mid + 1
        else:
            hi = mid
    return round(100 * lo / len(sorted_vals))


def haversine(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    d_lat = math.radians(lat2 - lat1)
    d_lng = math.radians(lng2 - lng1)
    a = (math.sin(d_lat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(d_lng / 2) ** 2)
    return EARTH_RADIUS_MI * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def nearest_neighbors(points: list, n: int = 8) -> list:
    """points: list of (key, lat, lng). Returns list (same order) of [(key, dist)]."""
    N = len(points)
    if N <= 1:
        return [[] for _ in points]
    k = min(n, N - 1)
    out = []
    if HAS_NUMPY:
        lat_r = np.radians(np.array([p[1] for p in points], dtype=np.float64))
        lng_r = np.radians(np.array([p[2] for p in points], dtype=np.float64))
        cos_lat = np.cos(lat_r)
        for i in range(N):
            dlat = lat_r - lat_r[i]
            dlng = lng_r - lng_r[i]
            a = np.sin(dlat / 2) ** 2 + cos_lat[i] * cos_lat * np.sin(dlng / 2) ** 2
            d = EARTH_RADIUS_MI * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
            d[i] = np.inf
            idx = np.argpartition(d, k)[:k]
            idx = idx[np.argsort(d[idx])]
            out.append([(points[j][0], round(float(d[j]), 1)) for j in idx])
        return out
    for i, (_, la, ln) in enumerate(points):
        ds = sorted(
            (haversine(la, ln, q[1], q[2]), q[0]) for j, q in enumerate(points) if j != i
        )[:k]
        out.append([(key, round(d, 1)) for d, key in ds])
    return out


def ensure_raw_data() -> None:
    """Extract the bundled RAR archive when the raw JSON is not present."""
    if RAW_DATA.exists() or not RAW_ARCHIVE.exists():
        return
    RAW_DATA.parent.mkdir(parents=True, exist_ok=True)
    for cmd in (
        ["bsdtar", "-xf", str(RAW_ARCHIVE), "-C", str(RAW_DATA.parent)],
        ["unrar", "x", "-o+", str(RAW_ARCHIVE), str(RAW_DATA.parent) + "/"],
        ["7z", "x", "-y", f"-o{RAW_DATA.parent}", str(RAW_ARCHIVE)],
    ):
        if shutil.which(cmd[0]) is None:
            continue
        print(f"Extracting {RAW_ARCHIVE.name} with {cmd[0]} …")
        if subprocess.run(cmd).returncode == 0 and RAW_DATA.exists():
            return
    print("WARNING: could not extract the RAR archive (install bsdtar/unrar/7z).",
          file=sys.stderr)


def write_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")),
                    encoding="utf-8")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main() -> None:
    t0 = time.time()
    if not RAW_DATA.exists() and RAW_GZIP.exists():
        print(f"Using bundled {RAW_GZIP.relative_to(PROJECT_ROOT)}")
        RAW_DATA.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(RAW_GZIP, "rb") as src, open(RAW_DATA, "wb") as dst:
            shutil.copyfileobj(src, dst)
    ensure_raw_data()

    if not RAW_DATA.exists():
        print(f"ERROR: {RAW_DATA} not found.", file=sys.stderr)
        print("Place pseo_zipcodes_full.json in data/raw/ and re-run.", file=sys.stderr)
        sys.exit(1)

    print(f"Loading {RAW_DATA} …")
    with open(RAW_DATA, encoding="utf-8") as f:
        raw = json.load(f)

    zips: list[dict] = [z for z in raw["zipcodes"] if z.get("lat") is not None]
    print(f"  Loaded {len(zips):,} records.")

    assert len(zips) > 35_000, f"Expected 35K+ records, got {len(zips)}"
    assert all(len(str(z["zipcode"])) == 5 for z in zips), "ZIPs with wrong length"
    zips.sort(key=lambda z: z["zipcode"])

    ZIPS_DIR.mkdir(parents=True, exist_ok=True)
    PUBLIC_DIR.mkdir(parents=True, exist_ok=True)
    for old in ZIPS_DIR.glob("*.json"):
        old.unlink()

    by_zip = {z["zipcode"]: z for z in zips}

    # --- Nearest ZIPs (within state) -------------------------------------
    print("Computing nearest ZIPs …")
    by_state: dict[str, list] = defaultdict(list)
    for z in zips:
        by_state[z["state"]].append(z)
    nearest_map: dict[str, list] = {}
    for state, state_zips in sorted(by_state.items()):
        pts = [(z["zipcode"], z["lat"], z["lng"]) for z in state_zips]
        for z, nn in zip(state_zips, nearest_neighbors(pts, 10)):
            nearest_map[z["zipcode"]] = [{"zip": k, "distance_mi": d} for k, d in nn]

    # --- State-level distributions (for percentile context) ---------------
    state_dist: dict[str, dict] = {}
    for state, state_zips in by_state.items():
        pop_sorted = sorted(
            (z for z in state_zips if z.get("population")),
            key=lambda z: -z["population"],
        )
        state_dist[state] = {
            "income": sorted(z["median_household_income"] for z in state_zips
                             if z.get("median_household_income")),
            "home": sorted(z["median_home_value"] for z in state_zips
                           if z.get("median_home_value")),
            "density": sorted(z["population_density"] for z in state_zips
                              if z.get("population_density")),
            "pop_rank": {z["zipcode"]: i + 1 for i, z in enumerate(pop_sorted)},
            "pop_ranked": len(pop_sorted),
        }

    # --- Aggregate structures ---------------------------------------------
    state_index: dict[str, dict] = {}
    city_index: dict[str, dict] = {}
    county_index: dict[str, dict] = {}

    for z in zips:
        state = z["state"]
        state_full = STATE_NAMES.get(state, state)
        city = z["major_city"]
        county = z["county"] or ""
        pop = z.get("population") or 0

        st = state_index.setdefault(state, {
            "name": state_full, "zip_count": 0, "zips": [], "population": 0,
            "housing_units": 0, "land_area_sqmi": 0.0, "water_area_sqmi": 0.0,
            "po_box_count": 0, "timezones": {}, "_city_pop": defaultdict(int),
            "_counties": set(), "_income": [], "_home": [],
        })
        st["zip_count"] += 1
        st["zips"].append(z["zipcode"])
        st["population"] += pop
        st["housing_units"] += z.get("housing_units") or 0
        st["land_area_sqmi"] += z.get("land_area_in_sqmi") or 0
        st["water_area_sqmi"] += z.get("water_area_in_sqmi") or 0
        if z.get("zipcode_type") == "PO BOX":
            st["po_box_count"] += 1
        if z.get("timezone"):
            st["timezones"][z["timezone"]] = st["timezones"].get(z["timezone"], 0) + 1
        st["_city_pop"][city] += pop
        if county:
            st["_counties"].add(county)
        st["_income"].append((z.get("median_household_income"), pop))
        st["_home"].append((z.get("median_home_value"), z.get("occupied_housing_units") or pop))

        cslug = city_slug(city, state)
        ct = city_index.setdefault(cslug, {
            "city": city, "state": state, "state_full": state_full, "county": county,
            "counties": [], "zips": [], "population": 0, "housing_units": 0,
            "land_area_sqmi": 0.0, "_lat": [], "_income": [], "_home": [],
            "timezone": z.get("timezone") or "",
        })
        ct["zips"].append(z["zipcode"])
        if county and county not in ct["counties"]:
            ct["counties"].append(county)
        ct["population"] += pop
        ct["housing_units"] += z.get("housing_units") or 0
        ct["land_area_sqmi"] += z.get("land_area_in_sqmi") or 0
        ct["_lat"].append((z["lat"], z["lng"], max(pop, 1)))
        ct["_income"].append((z.get("median_household_income"), pop))
        ct["_home"].append((z.get("median_home_value"), z.get("occupied_housing_units") or pop))

        if county:
            coslug = county_slug(county, state)
            co = county_index.setdefault(coslug, {
                "county": county, "state": state, "state_full": state_full,
                "zips": [], "population": 0, "housing_units": 0,
                "land_area_sqmi": 0.0, "_cities": defaultdict(int), "_lat": [],
                "_income": [], "_home": [], "timezone": z.get("timezone") or "",
            })
            co["zips"].append(z["zipcode"])
            co["population"] += pop
            co["housing_units"] += z.get("housing_units") or 0
            co["land_area_sqmi"] += z.get("land_area_in_sqmi") or 0
            co["_cities"][city] += pop
            co["_lat"].append((z["lat"], z["lng"], max(pop, 1)))
            co["_income"].append((z.get("median_household_income"), pop))
            co["_home"].append((z.get("median_home_value"), z.get("occupied_housing_units") or pop))

    def centroid(pts):
        w = sum(p[2] for p in pts)
        return (round(sum(p[0] * p[2] for p in pts) / w, 4),
                round(sum(p[1] * p[2] for p in pts) / w, 4))

    # Finalize states
    for abbr, st in state_index.items():
        city_pop = st.pop("_city_pop")
        st["city_count"] = len(city_pop)
        st["county_count"] = len(st.pop("_counties"))
        st["largest_cities"] = [c for c, p in sorted(city_pop.items(), key=lambda kv: -kv[1])[:24]]
        st["avg_household_income"] = weighted_avg(st.pop("_income"))
        st["avg_home_value"] = weighted_avg(st.pop("_home"))
        st["median_zip_income"] = median(state_dist[abbr]["income"])
        st["median_zip_home_value"] = median(state_dist[abbr]["home"])
        st["land_area_sqmi"] = round(st["land_area_sqmi"], 1)
        st["water_area_sqmi"] = round(st["water_area_sqmi"], 1)
        st["zips"].sort()
        st["zip_range"] = [st["zips"][0], st["zips"][-1]]
        st["timezones"] = [tz for tz, _ in sorted(st["timezones"].items(), key=lambda kv: -kv[1])]

    # Finalize cities (+ nearby cities within state)
    city_by_state: dict[str, list] = defaultdict(list)
    for slug, ct in city_index.items():
        lat, lng = centroid(ct.pop("_lat"))
        ct["lat"], ct["lng"] = lat, lng
        ct["avg_household_income"] = weighted_avg(ct.pop("_income"))
        ct["avg_home_value"] = weighted_avg(ct.pop("_home"))
        ct["land_area_sqmi"] = round(ct["land_area_sqmi"], 2)
        ct["zips"].sort()
        city_by_state[ct["state"]].append(slug)
    print("Computing nearby cities …")
    for state, slugs in city_by_state.items():
        pts = [(s, city_index[s]["lat"], city_index[s]["lng"]) for s in slugs]
        for s, nn in zip(slugs, nearest_neighbors(pts, 8)):
            city_index[s]["nearby"] = [
                {"slug": k, "city": city_index[k]["city"], "distance_mi": d} for k, d in nn
            ]

    # Finalize counties
    for co in county_index.values():
        cities = co.pop("_cities")
        co["cities"] = [c for c, _ in sorted(cities.items(), key=lambda kv: (-kv[1], kv[0]))]
        co["lat"], co["lng"] = centroid(co.pop("_lat"))
        co["avg_household_income"] = weighted_avg(co.pop("_income"))
        co["avg_home_value"] = weighted_avg(co.pop("_home"))
        co["land_area_sqmi"] = round(co["land_area_sqmi"], 1)
        co["zip_count"] = len(co["zips"])
        co["zips"].sort()

    # State ranking of counties by population
    for abbr, st in state_index.items():
        counties = [(k, v) for k, v in county_index.items() if v["state"] == abbr]
        counties.sort(key=lambda kv: -kv[1]["population"])
        st["largest_counties"] = [k for k, _ in counties[:12]]

    # --- Per-ZIP records ----------------------------------------------------
    print("Writing ZIP JSON files …")
    for z in zips:
        zipcode = z["zipcode"]
        state = z["state"]
        state_full = STATE_NAMES.get(state, state)
        city = z["major_city"]
        county = z["county"] or ""
        timezone = z.get("timezone") or ""
        dst = timezone not in NO_DST_ZONES if timezone else None
        dist = state_dist[state]
        cslug = city_slug(city, state)

        bounds = None
        if z.get("bounds_west") is not None:
            bounds = {
                "west": z["bounds_west"], "east": z["bounds_east"],
                "north": z["bounds_north"], "south": z["bounds_south"],
            }

        poc = (z.get("post_office_city") or "").rsplit(",", 1)[0].strip()

        record = {
            "zip": zipcode,
            "city": city,
            "city_slug": cslug,
            "post_office_city": poc if poc and poc != city else None,
            "state": state,
            "state_full": state_full,
            "county": county,
            "county_slug": county_slug(county, state) if county else "",
            "timezone": timezone,
            "dst": dst,
            "lat": z["lat"],
            "lng": z["lng"],
            "zip_type": z.get("zipcode_type", "STANDARD"),
            "radius_mi": z.get("radius_in_miles"),
            "land_area_sqmi": z.get("land_area_in_sqmi"),
            "water_area_sqmi": z.get("water_area_in_sqmi"),
            "population": z.get("population"),
            "population_density": z.get("population_density"),
            "housing_units": z.get("housing_units"),
            "occupied_housing_units": z.get("occupied_housing_units"),
            "median_home_value": z.get("median_home_value"),
            "median_household_income": z.get("median_household_income"),
            "bounds": bounds,
            "context": {
                "pop_rank": dist["pop_rank"].get(zipcode),
                "pop_ranked": dist["pop_ranked"],
                "income_pct": percentile_rank(dist["income"], z.get("median_household_income")),
                "home_pct": percentile_rank(dist["home"], z.get("median_home_value")),
                "density_pct": percentile_rank(dist["density"], z.get("population_density")),
                "state_median_income": median(dist["income"]),
                "state_median_home": median(dist["home"]),
            },
            "city_zips": [c for c in city_index[cslug]["zips"] if c != zipcode][:24],
            "city_zip_count": len(city_index[cslug]["zips"]),
            "surrounding_zips": nearest_map.get(zipcode, []),
        }
        write_json(ZIPS_DIR / f"{zipcode}.json", record)

    # --- Rankings -------------------------------------------------------------
    def row(z):
        return {
            "zip": z["zipcode"], "city": z["major_city"], "state": z["state"],
            "county": z["county"] or "", "population": z.get("population"),
            "median_household_income": z.get("median_household_income"),
            "median_home_value": z.get("median_home_value"),
            "land_area_sqmi": z.get("land_area_in_sqmi"),
            "population_density": z.get("population_density"),
        }

    def top(pool, key, n, min_pop=0):
        cand = [z for z in pool if z.get(key) and (z.get("population") or 0) >= min_pop]
        cand.sort(key=lambda z: (-z[key], -(z.get("median_household_income") or 0), z["zipcode"]))
        return [row(z) for z in cand[:n]]

    rankings = {
        "national": {
            "population": top(zips, "population", 100),
            "income": top(zips, "median_household_income", 100, RANK_MIN_POP),
            "home_value": top(zips, "median_home_value", 100, RANK_MIN_POP),
            "density": top(zips, "population_density", 100, RANK_MIN_POP),
            "land_area": top(zips, "land_area_in_sqmi", 100),
        },
        "states": {},
    }
    for state, state_zips in by_state.items():
        rankings["states"][state] = {
            "population": top(state_zips, "population", 25),
            "income": top(state_zips, "median_household_income", 25, RANK_MIN_POP),
            "home_value": top(state_zips, "median_home_value", 25, RANK_MIN_POP),
            "land_area": top(state_zips, "land_area_in_sqmi", 25),
        }

    popular_zips = [
        {**r, "state_full": STATE_NAMES.get(r["state"], r["state"]),
         "lat": by_zip[r["zip"]]["lat"], "lng": by_zip[r["zip"]]["lng"]}
        for r in rankings["national"]["population"]
    ]

    # --- Write index files ------------------------------------------------
    print("Writing index files …")
    write_json(DATA_DIR / "state_index.json", state_index)
    write_json(DATA_DIR / "city_index.json", city_index)
    write_json(DATA_DIR / "county_index.json", county_index)
    write_json(DATA_DIR / "popular_zips.json", popular_zips)
    write_json(DATA_DIR / "rankings.json", rankings)
    write_json(DATA_DIR / "meta.json", {
        "zip_count": len(zips),
        "standard_count": sum(1 for z in zips if z.get("zipcode_type") == "STANDARD"),
        "po_box_count": sum(1 for z in zips if z.get("zipcode_type") == "PO BOX"),
        "state_count": len(state_index),
        "city_count": len(city_index),
        "county_count": len(county_index),
        "population": sum(z.get("population") or 0 for z in zips),
        "built": date.today().isoformat(),
    })

    # Compact index for client-side search / tools:
    # [zip, city, state, lat, lng, population]
    write_json(PUBLIC_DIR / "zip-index.json", [
        [z["zipcode"], z["major_city"], z["state"], z["lat"], z["lng"], z.get("population") or 0]
        for z in zips
    ])
    stale = PUBLIC_DIR / "search_index.json"
    if stale.exists():
        stale.unlink()

    # --- Generate _redirects ------------------------------------------------
    print("Generating _redirects …")
    lines = ["# City slug redirects (post_office_city → major_city, generated by build_data.py)"]
    seen: set[str] = set()
    for z in zips:
        poc = z.get("post_office_city") or ""
        if not poc:
            continue
        old_slug = city_slug(poc.rsplit(",", 1)[0].strip(), z["state"])
        new_slug = city_slug(z["major_city"], z["state"])
        if old_slug == new_slug or old_slug in city_index or old_slug in seen:
            continue
        seen.add(old_slug)
        lines.append(f"/city/{old_slug}/ /city/{new_slug}/ 301")
    (PUBLIC_DIR / "_redirects").write_text("\n".join(lines) + "\n", encoding="utf-8")

    elapsed = time.time() - t0
    print(f"\nBuild complete in {elapsed:.1f}s")
    print(f"  {len(zips):,} ZIP files in data/zips/")
    print(f"  {len(state_index)} states, {len(city_index):,} cities, "
          f"{len(county_index):,} counties")


if __name__ == "__main__":
    main()

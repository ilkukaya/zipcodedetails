import fs from 'node:fs';
import path from 'node:path';

/**
 * Build-time data access. Everything is read from disk once per process and
 * memoised, so generating ~72k pages does not re-parse the same JSON files.
 */

const DATA = path.join(process.cwd(), 'data');

function readJson<T>(file: string, fallback: T): T {
  try {
    return JSON.parse(fs.readFileSync(path.join(DATA, file), 'utf-8')) as T;
  } catch {
    return fallback;
  }
}

function memo<T>(fn: () => T): () => T {
  let v: T | undefined;
  let done = false;
  return () => {
    if (!done) {
      v = fn();
      done = true;
    }
    return v as T;
  };
}

export interface NearbyZip {
  zip: string;
  distance_mi: number;
}

export interface ZipRecord {
  zip: string;
  city: string;
  city_slug: string;
  post_office_city: string | null;
  state: string;
  state_full: string;
  county: string;
  county_slug: string;
  timezone: string;
  dst: boolean | null;
  lat: number;
  lng: number;
  zip_type: 'STANDARD' | 'PO BOX';
  radius_mi: number | null;
  land_area_sqmi: number | null;
  water_area_sqmi: number | null;
  population: number | null;
  population_density: number | null;
  housing_units: number | null;
  occupied_housing_units: number | null;
  median_home_value: number | null;
  median_household_income: number | null;
  bounds: { west: number; east: number; north: number; south: number } | null;
  context: {
    pop_rank: number | null;
    pop_ranked: number;
    income_pct: number | null;
    home_pct: number | null;
    density_pct: number | null;
    state_median_income: number | null;
    state_median_home: number | null;
  };
  city_zips: string[];
  city_zip_count: number;
  surrounding_zips: NearbyZip[];
}

export interface StateInfo {
  name: string;
  zip_count: number;
  zips: string[];
  population: number;
  housing_units: number;
  land_area_sqmi: number;
  water_area_sqmi: number;
  po_box_count: number;
  timezones: string[];
  city_count: number;
  county_count: number;
  largest_cities: string[];
  largest_counties: string[];
  avg_household_income: number | null;
  avg_home_value: number | null;
  median_zip_income: number | null;
  median_zip_home_value: number | null;
  zip_range: [string, string];
}

export interface CityInfo {
  city: string;
  state: string;
  state_full: string;
  county: string;
  counties: string[];
  zips: string[];
  population: number;
  housing_units: number;
  land_area_sqmi: number;
  timezone: string;
  lat: number;
  lng: number;
  avg_household_income: number | null;
  avg_home_value: number | null;
  nearby: { slug: string; city: string; distance_mi: number }[];
}

export interface CountyInfo {
  county: string;
  state: string;
  state_full: string;
  zips: string[];
  zip_count: number;
  population: number;
  housing_units: number;
  land_area_sqmi: number;
  cities: string[];
  lat: number;
  lng: number;
  timezone: string;
  avg_household_income: number | null;
  avg_home_value: number | null;
}

export interface RankRow {
  zip: string;
  city: string;
  state: string;
  county: string;
  population: number | null;
  median_household_income: number | null;
  median_home_value: number | null;
  land_area_sqmi: number | null;
  population_density: number | null;
}

export interface Rankings {
  national: Record<'population' | 'income' | 'home_value' | 'density' | 'land_area', RankRow[]>;
  states: Record<string, Record<'population' | 'income' | 'home_value' | 'land_area', RankRow[]>>;
}

export interface Meta {
  zip_count: number;
  standard_count: number;
  po_box_count: number;
  state_count: number;
  city_count: number;
  county_count: number;
  population: number;
  built: string;
}

export const getStates = memo(() => readJson<Record<string, StateInfo>>('state_index.json', {}));
export const getCities = memo(() => readJson<Record<string, CityInfo>>('city_index.json', {}));
export const getCounties = memo(() => readJson<Record<string, CountyInfo>>('county_index.json', {}));
export const getRankings = memo(() =>
  readJson<Rankings>('rankings.json', { national: {} as Rankings['national'], states: {} })
);
export const getPopular = memo(() => readJson<RankRow[]>('popular_zips.json', []));
export const getMeta = memo(() =>
  readJson<Meta>('meta.json', {
    zip_count: 0, standard_count: 0, po_box_count: 0, state_count: 0,
    city_count: 0, county_count: 0, population: 0, built: new Date().toISOString().slice(0, 10),
  })
);

export const getAllZips = memo(() => {
  const dir = path.join(DATA, 'zips');
  const map = new Map<string, ZipRecord>();
  if (!fs.existsSync(dir)) return map;
  for (const f of fs.readdirSync(dir).sort()) {
    if (!f.endsWith('.json')) continue;
    const rec = JSON.parse(fs.readFileSync(path.join(dir, f), 'utf-8')) as ZipRecord;
    map.set(rec.zip, rec);
  }
  return map;
});

export function getZip(zip: string): ZipRecord | undefined {
  return getAllZips().get(zip);
}

export function getZips(zips: string[]): ZipRecord[] {
  const all = getAllZips();
  return zips.map((z) => all.get(z)).filter((z): z is ZipRecord => !!z);
}

/** Groups cities by state for quick lookups. */
export const getCitiesByState = memo(() => {
  const out: Record<string, { slug: string; info: CityInfo }[]> = {};
  for (const [slug, info] of Object.entries(getCities())) {
    (out[info.state] ||= []).push({ slug, info });
  }
  return out;
});

export const getCountiesByState = memo(() => {
  const out: Record<string, { slug: string; info: CountyInfo }[]> = {};
  for (const [slug, info] of Object.entries(getCounties())) {
    (out[info.state] ||= []).push({ slug, info });
  }
  return out;
});

/**
 * Client-side ZIP index shared by the search box, search page and tools.
 * Row format (see scripts/build_data.py): [zip, city, state, lat, lng, population]
 */
export type Row = [string, string, string, number, number, number];

export interface ZipHit {
  kind: 'zip';
  zip: string;
  city: string;
  state: string;
  lat: number;
  lng: number;
  pop: number;
}
export interface CityHit {
  kind: 'city';
  city: string;
  state: string;
  count: number;
  pop: number;
  firstZip: string;
}
export interface StateHit {
  kind: 'state';
  state: string;
  name: string;
}
export type Hit = ZipHit | CityHit | StateHit;

export const STATE_NAMES: Record<string, string> = {
  AL: 'Alabama', AK: 'Alaska', AZ: 'Arizona', AR: 'Arkansas', CA: 'California', CO: 'Colorado',
  CT: 'Connecticut', DE: 'Delaware', FL: 'Florida', GA: 'Georgia', HI: 'Hawaii', ID: 'Idaho',
  IL: 'Illinois', IN: 'Indiana', IA: 'Iowa', KS: 'Kansas', KY: 'Kentucky', LA: 'Louisiana',
  ME: 'Maine', MD: 'Maryland', MA: 'Massachusetts', MI: 'Michigan', MN: 'Minnesota',
  MS: 'Mississippi', MO: 'Missouri', MT: 'Montana', NE: 'Nebraska', NV: 'Nevada',
  NH: 'New Hampshire', NJ: 'New Jersey', NM: 'New Mexico', NY: 'New York', NC: 'North Carolina',
  ND: 'North Dakota', OH: 'Ohio', OK: 'Oklahoma', OR: 'Oregon', PA: 'Pennsylvania',
  RI: 'Rhode Island', SC: 'South Carolina', SD: 'South Dakota', TN: 'Tennessee', TX: 'Texas',
  UT: 'Utah', VT: 'Vermont', VA: 'Virginia', WA: 'Washington', WV: 'West Virginia',
  WI: 'Wisconsin', WY: 'Wyoming', DC: 'District of Columbia', PR: 'Puerto Rico',
  VI: 'U.S. Virgin Islands', GU: 'Guam', AS: 'American Samoa', MP: 'Northern Mariana Islands',
  FM: 'Micronesia', MH: 'Marshall Islands', PW: 'Palau',
};

let rowsPromise: Promise<Row[]> | null = null;

export function loadIndex(): Promise<Row[]> {
  if (!rowsPromise) {
    rowsPromise = fetch('/zip-index.json')
      .then((r) => (r.ok ? r.json() : []))
      .catch(() => {
        rowsPromise = null;
        return [];
      });
  }
  return rowsPromise;
}

export function slugify(s: string): string {
  return s
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, '')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

export const citySlug = (city: string, state: string) => `${slugify(city)}-${state.toLowerCase()}`;

export function hitUrl(h: Hit): string {
  if (h.kind === 'zip') return `/zip/${h.zip}/`;
  if (h.kind === 'city') return `/city/${citySlug(h.city, h.state)}/`;
  return `/state/${h.state.toLowerCase()}/`;
}

const norm = (s: string) => s.toLowerCase().replace(/[^a-z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim();

export function search(rows: Row[], raw: string, limit = 8): Hit[] {
  const q = raw.trim();
  if (!q) return [];

  // Numeric → ZIP prefix
  const digits = q.replace(/\D/g, '');
  if (/^\d[\d\s-]*$/.test(q) && digits.length >= 1) {
    const pre = digits.slice(0, 5);
    const out: Hit[] = [];
    for (const r of rows) {
      if (r[0].startsWith(pre)) {
        out.push({ kind: 'zip', zip: r[0], city: r[1], state: r[2], lat: r[3], lng: r[4], pop: r[5] });
        if (out.length >= limit) break;
      }
    }
    return out;
  }

  // Text → state, city ("austin", "austin tx", "austin, texas")
  let text = norm(q);
  let stateFilter: string | null = null;
  const parts = text.split(' ');
  if (parts.length > 1) {
    const last = parts[parts.length - 1].toUpperCase();
    if (STATE_NAMES[last]) {
      stateFilter = last;
      text = parts.slice(0, -1).join(' ');
    } else {
      for (const [abbr, name] of Object.entries(STATE_NAMES)) {
        const n = norm(name);
        if (text.endsWith(' ' + n)) {
          stateFilter = abbr;
          text = text.slice(0, -(n.length + 1)).trim();
          break;
        }
      }
    }
  }

  const states: StateHit[] = [];
  if (!stateFilter) {
    for (const [abbr, name] of Object.entries(STATE_NAMES)) {
      const n = norm(name);
      if (n.startsWith(text) || (text.length === 2 && abbr.toLowerCase() === text)) {
        states.push({ kind: 'state', state: abbr, name });
      }
    }
  }

  const cities = new Map<string, CityHit & { score: number }>();
  if (text.length >= 2) {
    for (const r of rows) {
      if (stateFilter && r[2] !== stateFilter) continue;
      const c = norm(r[1]);
      let score = -1;
      if (c === text) score = 3;
      else if (c.startsWith(text)) score = 2;
      else if (text.length >= 3 && c.includes(' ' + text)) score = 1;
      else if (text.length >= 4 && c.includes(text)) score = 0;
      if (score < 0) continue;
      const key = r[1] + '|' + r[2];
      const cur = cities.get(key);
      if (cur) {
        cur.count++;
        cur.pop += r[5];
      } else {
        cities.set(key, { kind: 'city', city: r[1], state: r[2], count: 1, pop: r[5], firstZip: r[0], score });
      }
    }
  }
  const cityHits = [...cities.values()]
    .sort((a, b) => b.score - a.score || b.pop - a.pop)
    .slice(0, limit)
    .map(({ score, ...h }) => h as CityHit);

  return [...states.slice(0, 2), ...cityHits].slice(0, limit);
}

const R = 3958.8;
export function haversine(lat1: number, lng1: number, lat2: number, lng2: number): number {
  const toRad = (d: number) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

export function nearest(rows: Row[], lat: number, lng: number, n = 1, maxMi = Infinity) {
  const out: { row: Row; d: number }[] = [];
  for (const r of rows) {
    const d = haversine(lat, lng, r[3], r[4]);
    if (d > maxMi) continue;
    out.push({ row: r, d });
  }
  out.sort((a, b) => a.d - b.d);
  return n === Infinity ? out : out.slice(0, n);
}

export function esc(s: string): string {
  return s.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]!);
}

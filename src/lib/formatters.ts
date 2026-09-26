/** Format a number with commas (e.g., 21,741) */
export function formatNumber(value: number | null | undefined, digits = 0): string {
  if (value == null || Number.isNaN(value)) return '—';
  return value.toLocaleString('en-US', { maximumFractionDigits: digits, minimumFractionDigits: 0 });
}

/** Currency. The source data top-codes home values at $1,000,001. */
export function formatMoney(value: number | null | undefined): string {
  if (value == null) return '—';
  if (value >= 1_000_001) return '$1,000,000+';
  return '$' + Math.round(value).toLocaleString('en-US');
}

/** Compact currency, e.g. $132K, $1.2M */
export function formatMoneyShort(value: number | null | undefined): string {
  if (value == null) return '—';
  if (value >= 1_000_001) return '$1M+';
  if (value >= 1_000_000) return '$' + (value / 1_000_000).toFixed(1).replace(/\.0$/, '') + 'M';
  if (value >= 1000) return '$' + Math.round(value / 1000) + 'K';
  return '$' + value;
}

export function formatCompact(value: number | null | undefined): string {
  if (value == null) return '—';
  if (value >= 1_000_000) return (value / 1_000_000).toFixed(1).replace(/\.0$/, '') + 'M';
  if (value >= 10_000) return Math.round(value / 1000) + 'K';
  return value.toLocaleString('en-US');
}

/** Area in square miles (e.g., 5.71 sq mi) */
export function formatArea(sqmi: number | null | undefined): string {
  if (sqmi == null) return '—';
  if (sqmi >= 100) return Math.round(sqmi).toLocaleString('en-US') + ' sq mi';
  return sqmi.toFixed(2) + ' sq mi';
}

/** Coordinates (e.g., 34.0901° N, 118.4065° W) */
export function formatCoordinates(lat: number, lng: number): string {
  const latDir = lat >= 0 ? 'N' : 'S';
  const lngDir = lng >= 0 ? 'E' : 'W';
  return `${Math.abs(lat).toFixed(2)}° ${latDir}, ${Math.abs(lng).toFixed(2)}° ${lngDir}`;
}

export function plural(n: number, one: string, many = one + 's'): string {
  return `${n.toLocaleString('en-US')} ${n === 1 ? one : many}`;
}

export function ordinal(n: number): string {
  const s = ['th', 'st', 'nd', 'rd'];
  const v = n % 100;
  return n.toLocaleString('en-US') + (s[(v - 20) % 10] || s[v] || s[0]);
}

export function listJoin(items: string[]): string {
  if (items.length <= 1) return items.join('');
  if (items.length === 2) return `${items[0]} and ${items[1]}`;
  return `${items.slice(0, -1).join(', ')}, and ${items[items.length - 1]}`;
}

interface TzInfo {
  name: string;
  abbr: string;
  utc: string;
}

const TZ: Record<string, TzInfo> = {
  'America/New_York': { name: 'Eastern Time', abbr: 'ET', utc: 'UTC−5 / −4' },
  'America/Detroit': { name: 'Eastern Time', abbr: 'ET', utc: 'UTC−5 / −4' },
  'America/Indiana/Indianapolis': { name: 'Eastern Time', abbr: 'ET', utc: 'UTC−5 / −4' },
  'America/Indiana/Vincennes': { name: 'Eastern Time', abbr: 'ET', utc: 'UTC−5 / −4' },
  'America/Indiana/Marengo': { name: 'Eastern Time', abbr: 'ET', utc: 'UTC−5 / −4' },
  'America/Indiana/Petersburg': { name: 'Eastern Time', abbr: 'ET', utc: 'UTC−5 / −4' },
  'America/Indiana/Winamac': { name: 'Eastern Time', abbr: 'ET', utc: 'UTC−5 / −4' },
  'America/Indiana/Vevay': { name: 'Eastern Time', abbr: 'ET', utc: 'UTC−5 / −4' },
  'America/Kentucky/Louisville': { name: 'Eastern Time', abbr: 'ET', utc: 'UTC−5 / −4' },
  'America/Kentucky/Monticello': { name: 'Eastern Time', abbr: 'ET', utc: 'UTC−5 / −4' },
  'America/Chicago': { name: 'Central Time', abbr: 'CT', utc: 'UTC−6 / −5' },
  'America/Menominee': { name: 'Central Time', abbr: 'CT', utc: 'UTC−6 / −5' },
  'America/Indiana/Tell_City': { name: 'Central Time', abbr: 'CT', utc: 'UTC−6 / −5' },
  'America/Indiana/Knox': { name: 'Central Time', abbr: 'CT', utc: 'UTC−6 / −5' },
  'America/North_Dakota/Center': { name: 'Central Time', abbr: 'CT', utc: 'UTC−6 / −5' },
  'America/North_Dakota/New_Salem': { name: 'Central Time', abbr: 'CT', utc: 'UTC−6 / −5' },
  'America/North_Dakota/Beulah': { name: 'Central Time', abbr: 'CT', utc: 'UTC−6 / −5' },
  'America/Denver': { name: 'Mountain Time', abbr: 'MT', utc: 'UTC−7 / −6' },
  'America/Boise': { name: 'Mountain Time', abbr: 'MT', utc: 'UTC−7 / −6' },
  'America/Phoenix': { name: 'Mountain Standard Time', abbr: 'MST', utc: 'UTC−7' },
  'America/Los_Angeles': { name: 'Pacific Time', abbr: 'PT', utc: 'UTC−8 / −7' },
  'America/Anchorage': { name: 'Alaska Time', abbr: 'AKT', utc: 'UTC−9 / −8' },
  'America/Juneau': { name: 'Alaska Time', abbr: 'AKT', utc: 'UTC−9 / −8' },
  'America/Sitka': { name: 'Alaska Time', abbr: 'AKT', utc: 'UTC−9 / −8' },
  'America/Nome': { name: 'Alaska Time', abbr: 'AKT', utc: 'UTC−9 / −8' },
  'America/Yakutat': { name: 'Alaska Time', abbr: 'AKT', utc: 'UTC−9 / −8' },
  'America/Metlakatla': { name: 'Alaska Time', abbr: 'AKT', utc: 'UTC−9 / −8' },
  'America/Adak': { name: 'Hawaii-Aleutian Time', abbr: 'HAT', utc: 'UTC−10 / −9' },
  'Pacific/Honolulu': { name: 'Hawaii-Aleutian Standard Time', abbr: 'HST', utc: 'UTC−10' },
  'America/Puerto_Rico': { name: 'Atlantic Standard Time', abbr: 'AST', utc: 'UTC−4' },
  'America/St_Thomas': { name: 'Atlantic Standard Time', abbr: 'AST', utc: 'UTC−4' },
  'Pacific/Guam': { name: 'Chamorro Standard Time', abbr: 'ChST', utc: 'UTC+10' },
  'Pacific/Saipan': { name: 'Chamorro Standard Time', abbr: 'ChST', utc: 'UTC+10' },
  'Pacific/Pago_Pago': { name: 'Samoa Standard Time', abbr: 'SST', utc: 'UTC−11' },
  'Pacific/Chuuk': { name: 'Chuuk Time', abbr: 'CHUT', utc: 'UTC+10' },
  'Pacific/Pohnpei': { name: 'Pohnpei Time', abbr: 'PONT', utc: 'UTC+11' },
  'Pacific/Majuro': { name: 'Marshall Islands Time', abbr: 'MHT', utc: 'UTC+12' },
  'Pacific/Palau': { name: 'Palau Time', abbr: 'PWT', utc: 'UTC+9' },
};

function tzInfo(tz: string): TzInfo {
  return (
    TZ[tz] || {
      name: tz ? tz.split('/').pop()!.replace(/_/g, ' ') + ' Time' : 'Unknown',
      abbr: '',
      utc: '',
    }
  );
}

export const getTimezoneName = (tz: string) => tzInfo(tz).name;
export const getTimezoneAbbr = (tz: string) => tzInfo(tz).abbr;
export const getTimezoneUtc = (tz: string) => tzInfo(tz).utc;

export function slugify(text: string): string {
  return text
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, '')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

/** "Beverly Hills" + "CA" → "beverly-hills-ca" */
export function citySlug(city: string, state: string): string {
  return slugify(city) + '-' + state.toLowerCase();
}

/** "Los Angeles County" + "CA" → "los-angeles-county-ca" */
export function countySlug(county: string, state: string): string {
  return slugify(county) + '-' + state.toLowerCase();
}

/** Comparison phrase for a value against a reference. */
export function compare(value: number | null, ref: number | null): string | null {
  if (value == null || ref == null || ref === 0) return null;
  const diff = (value - ref) / ref;
  const pct = Math.round(Math.abs(diff) * 100);
  if (pct < 3) return 'about the same as';
  return `${pct}% ${diff > 0 ? 'above' : 'below'}`;
}

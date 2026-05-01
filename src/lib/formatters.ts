export function formatNumber(value: number | null | undefined): string {
  if (value == null) return 'N/A';
  return value.toLocaleString('en-US');
}

export function formatArea(sqmi: number | null | undefined): string {
  if (sqmi == null) return 'N/A';
  return sqmi.toFixed(2) + ' sq mi';
}

export function formatCoordinates(lat: number | null | undefined, lng: number | null | undefined): string {
  if (lat == null || lng == null) return 'N/A';
  const latDir = lat >= 0 ? 'N' : 'S';
  const lngDir = lng >= 0 ? 'E' : 'W';
  return `${Math.abs(lat).toFixed(4)}°${latDir}, ${Math.abs(lng).toFixed(4)}°${lngDir}`;
}

const TZ_ABBR: Record<string, string> = {
  'America/New_York': 'ET',
  'America/Chicago': 'CT',
  'America/Denver': 'MT',
  'America/Los_Angeles': 'PT',
  'America/Anchorage': 'AKT',
  'Pacific/Honolulu': 'HST',
  'America/Phoenix': 'MST',
  'America/Boise': 'MT',
  'America/Indiana/Indianapolis': 'ET',
  'America/Indiana/Knox': 'CT',
  'America/Indiana/Marengo': 'ET',
  'America/Indiana/Petersburg': 'ET',
  'America/Indiana/Tell_City': 'CT',
  'America/Indiana/Vevay': 'ET',
  'America/Indiana/Vincennes': 'ET',
  'America/Indiana/Winamac': 'ET',
  'America/Kentucky/Louisville': 'ET',
  'America/Kentucky/Monticello': 'ET',
  'America/Detroit': 'ET',
  'America/Menominee': 'CT',
  'America/North_Dakota/Center': 'CT',
  'America/North_Dakota/New_Salem': 'CT',
  'America/North_Dakota/Beulah': 'CT',
  'America/Nome': 'AKT',
  'America/Sitka': 'AKT',
  'America/Juneau': 'AKT',
  'America/Yakutat': 'AKT',
  'America/Metlakatla': 'AKT',
  'America/Adak': 'HST',
  'America/Puerto_Rico': 'AST',
  'America/St_Thomas': 'AST',
  'America/Virgin': 'AST',
  'Pacific/Guam': 'ChST',
  'Pacific/Saipan': 'ChST',
  'Pacific/Pago_Pago': 'SST',
  'Pacific/Midway': 'SST',
};

const TZ_NAME: Record<string, string> = {
  'America/New_York': 'Eastern Time',
  'America/Chicago': 'Central Time',
  'America/Denver': 'Mountain Time',
  'America/Los_Angeles': 'Pacific Time',
  'America/Anchorage': 'Alaska Time',
  'Pacific/Honolulu': 'Hawaii-Aleutian Time',
  'America/Phoenix': 'Mountain Standard Time',
  'America/Boise': 'Mountain Time',
  'America/Indiana/Indianapolis': 'Eastern Time',
  'America/Indiana/Knox': 'Central Time',
  'America/Indiana/Marengo': 'Eastern Time',
  'America/Indiana/Petersburg': 'Eastern Time',
  'America/Indiana/Tell_City': 'Central Time',
  'America/Indiana/Vevay': 'Eastern Time',
  'America/Indiana/Vincennes': 'Eastern Time',
  'America/Indiana/Winamac': 'Eastern Time',
  'America/Kentucky/Louisville': 'Eastern Time',
  'America/Kentucky/Monticello': 'Eastern Time',
  'America/Detroit': 'Eastern Time',
  'America/Menominee': 'Central Time',
  'America/North_Dakota/Center': 'Central Time',
  'America/North_Dakota/New_Salem': 'Central Time',
  'America/North_Dakota/Beulah': 'Central Time',
  'America/Nome': 'Alaska Time',
  'America/Sitka': 'Alaska Time',
  'America/Juneau': 'Alaska Time',
  'America/Yakutat': 'Alaska Time',
  'America/Metlakatla': 'Alaska Time',
  'America/Adak': 'Hawaii-Aleutian Time',
  'America/Puerto_Rico': 'Atlantic Time',
  'America/St_Thomas': 'Atlantic Time',
  'America/Virgin': 'Atlantic Time',
  'Pacific/Guam': 'Chamorro Time',
  'Pacific/Saipan': 'Chamorro Time',
  'Pacific/Pago_Pago': 'Samoa Time',
  'Pacific/Midway': 'Samoa Time',
};

export function getTimezoneAbbr(tz: string): string {
  return TZ_ABBR[tz] ?? (tz.split('/').pop()?.replace(/_/g, ' ') || tz);
}

export function getTimezoneName(tz: string): string {
  return TZ_NAME[tz] ?? (tz.split('/').pop()?.replace(/_/g, ' ') || tz);
}

export function citySlug(city: string, state: string): string {
  return (
    city
      .toLowerCase()
      .replace(/[^a-z0-9\s-]/g, '')
      .replace(/\s+/g, '-')
      .replace(/-+/g, '-') +
    '-' +
    state.toLowerCase()
  );
}

export function countySlug(county: string, state: string): string {
  return (
    county
      .toLowerCase()
      .replace(/[^a-z0-9\s-]/g, '')
      .replace(/\s+/g, '-')
      .replace(/-+/g, '-') +
    '-' +
    state.toLowerCase()
  );
}

export function slugToCity(slug: string): { city: string; state: string } {
  const parts = slug.split('-');
  const state = parts.pop()!.toUpperCase();
  const city = parts
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
  return { city, state };
}

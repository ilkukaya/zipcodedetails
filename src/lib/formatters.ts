/**
 * Format a number as US currency (e.g., $147,832)
 */
export function formatCurrency(value: number | null | undefined): string {
  if (value == null || value < 0) return 'N/A';
  return '$' + value.toLocaleString('en-US');
}

/**
 * Format a number with commas (e.g., 21,741)
 */
export function formatNumber(value: number | null | undefined): string {
  if (value == null) return 'N/A';
  return value.toLocaleString('en-US');
}

/**
 * Format area in square miles (e.g., 5.71 sq mi)
 */
export function formatArea(sqmi: number | null | undefined): string {
  if (sqmi == null) return 'N/A';
  return sqmi.toFixed(2) + ' sq mi';
}

/**
 * Format coordinates (e.g., 34.0901°N, 118.4065°W)
 */
export function formatCoordinates(lat: number, lng: number): string {
  const latDir = lat >= 0 ? 'N' : 'S';
  const lngDir = lng >= 0 ? 'E' : 'W';
  return `${Math.abs(lat).toFixed(4)}°${latDir}, ${Math.abs(lng).toFixed(4)}°${lngDir}`;
}

/**
 * Get timezone abbreviation from IANA timezone name
 */
export function getTimezoneAbbr(tz: string): string {
  const map: Record<string, string> = {
    'America/New_York': 'ET',
    'America/Chicago': 'CT',
    'America/Denver': 'MT',
    'America/Los_Angeles': 'PT',
    'America/Anchorage': 'AKT',
    'Pacific/Honolulu': 'HST',
    'America/Phoenix': 'MST',
    'America/Boise': 'MT',
    'America/Indiana/Indianapolis': 'ET',
    'America/Kentucky/Louisville': 'ET',
    'America/North_Dakota/Center': 'CT',
    'America/Adak': 'HST',
  };
  return map[tz] || tz.split('/').pop()?.replace(/_/g, ' ') || tz;
}

/**
 * Get human-readable timezone name
 */
export function getTimezoneName(tz: string): string {
  const map: Record<string, string> = {
    'America/New_York': 'Eastern Time',
    'America/Chicago': 'Central Time',
    'America/Denver': 'Mountain Time',
    'America/Los_Angeles': 'Pacific Time',
    'America/Anchorage': 'Alaska Time',
    'Pacific/Honolulu': 'Hawaii-Aleutian Time',
    'America/Phoenix': 'Mountain Standard Time',
    'America/Boise': 'Mountain Time',
    'America/Indiana/Indianapolis': 'Eastern Time',
    'America/Kentucky/Louisville': 'Eastern Time',
  };
  return map[tz] || tz.split('/').pop()?.replace(/_/g, ' ') || tz;
}

/**
 * Slugify a city+state combo for URL (e.g., "Beverly Hills" + "CA" → "beverly-hills-ca")
 */
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

/**
 * Reverse a city slug back to display name
 */
export function slugToCity(slug: string): { city: string; state: string } {
  const parts = slug.split('-');
  const state = parts.pop()!.toUpperCase();
  const city = parts
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
  return { city, state };
}

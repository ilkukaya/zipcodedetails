export interface ZipData {
  zip: string;
  city: string;
  state: string;
  state_full: string;
  county: string;
  timezone: string;
  population?: number | null;
  median_household_income?: number | null;
  surrounding_zips?: unknown[];
}

export function zipPageTitle(data: ZipData): string {
  return `ZIP Code ${data.zip} — ${data.city}, ${data.state} | ZIPCodeDetails.com`;
}

export function zipPageDescription(data: ZipData): string {
  const parts: string[] = [];
  if (data.population) parts.push(`Population ${data.population.toLocaleString('en-US')}`);
  if (data.median_household_income) {
    parts.push(`median income $${data.median_household_income.toLocaleString('en-US')}`);
  }
  const nearbyCount = Array.isArray(data.surrounding_zips)
    ? Math.min(data.surrounding_zips.length, 8)
    : 0;
  const nearbyStr = nearbyCount > 0 ? `, and ${nearbyCount} nearby ZIP codes` : '';
  const locationStr = `${data.city}, ${data.state} (${data.county})`;
  if (parts.length > 0) {
    return `${parts.join(', ')} in ${locationStr}. View time zone, coordinates${nearbyStr}.`;
  }
  return `ZIP code ${data.zip} is in ${locationStr}. View location, time zone, coordinates${nearbyStr}.`;
}

export function statePageTitle(stateFull: string, stateAbbr: string, count: number): string {
  return `${stateFull} ZIP Codes — All ${count.toLocaleString()} ZIP Codes | ZIPCodeDetails.com`;
}

export function statePageDescription(stateFull: string, count: number): string {
  return `Browse all ${count.toLocaleString()} ZIP codes in ${stateFull}. View county, city, time zone, and geographic data for every ZIP code in ${stateFull}.`;
}

export function cityPageTitle(city: string, state: string): string {
  return `${city}, ${state} ZIP Codes — All ZIP Codes in ${city} | ZIPCodeDetails.com`;
}

export function cityPageDescription(city: string, stateFull: string, count: number): string {
  return `Find all ${count} ZIP code${count !== 1 ? 's' : ''} for ${city}, ${stateFull}. View location, county, time zone, and geographic data for each ZIP code.`;
}

export function countyPageTitle(county: string, state: string, count: number): string {
  return `${county}, ${state} ZIP Codes — All ${count.toLocaleString()} ZIP Codes | ZIPCodeDetails.com`;
}

export function countyPageDescription(county: string, stateFull: string, count: number): string {
  return `Browse all ${count.toLocaleString()} ZIP codes in ${county}, ${stateFull}. View city, time zone, and geographic data for every ZIP code in ${county}.`;
}

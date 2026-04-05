export interface ZipData {
  zip: string;
  city: string;
  state: string;
  state_full: string;
  county: string;
  timezone: string;
}

export function zipPageTitle(data: ZipData): string {
  return `ZIP Code ${data.zip} — ${data.city}, ${data.state_full} | ZIPCodeDetails.com`;
}

export function zipPageDescription(data: ZipData): string {
  return `ZIP code ${data.zip} is in ${data.city}, ${data.state_full} (${data.county}). View location, time zone, coordinates, and nearby ZIP codes.`;
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

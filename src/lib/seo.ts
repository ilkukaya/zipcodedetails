export interface ZipData {
  zip: string;
  city: string;
  state: string;
  state_full: string;
  county: string;
  timezone: string;
}

export function zipPageTitle(data: ZipData): string {
  return `ZIP Code ${data.zip} — ${data.city}, ${data.state} | ZIPCodeDetails.com`;
}

export function zipPageDescription(data: ZipData): string {
  return `Complete details for ZIP code ${data.zip} in ${data.city}, ${data.state_full}. County: ${data.county}. Location, time zone, and geographic data.`;
}

export function statePageTitle(stateFull: string, stateAbbr: string, count: number): string {
  return `${stateFull} ZIP Codes — All ${count.toLocaleString()} ZIP Codes in ${stateAbbr}`;
}

export function statePageDescription(stateFull: string, count: number): string {
  return `Browse all ${count.toLocaleString()} ZIP codes in ${stateFull}. Find location, county, time zone, and geographic data for every ZIP code in the state.`;
}

export function cityPageTitle(city: string, state: string): string {
  return `${city}, ${state} ZIP Codes — All ZIP Codes in ${city}, ${state}`;
}

export function cityPageDescription(city: string, stateFull: string, count: number): string {
  return `Find all ${count} ZIP code${count !== 1 ? 's' : ''} for ${city}, ${stateFull}. View location, county, time zone, and geographic data.`;
}

import { formatNumber, formatCurrency } from './formatters';

export interface ZipData {
  zip: string;
  city: string;
  state: string;
  state_full: string;
  county: string;
  population: number | null;
  median_household_income: number | null;
  timezone: string;
}

export function zipPageTitle(data: ZipData): string {
  return `ZIP Code ${data.zip} — ${data.city}, ${data.state} | ZIPCodeDetails.com`;
}

export function zipPageDescription(data: ZipData): string {
  const parts = [`Complete details for ZIP code ${data.zip} in ${data.city}, ${data.state_full}.`];
  if (data.population != null) {
    parts.push(`Population: ${formatNumber(data.population)}`);
  }
  if (data.median_household_income != null) {
    parts.push(`Median Income: ${formatCurrency(data.median_household_income)}`);
  }
  parts.push(`County: ${data.county}`);
  return parts.join(' | ');
}

export function statePageTitle(stateFull: string, stateAbbr: string, count: number): string {
  return `${stateFull} ZIP Codes — All ${formatNumber(count)} ZIP Codes in ${stateAbbr}`;
}

export function statePageDescription(stateFull: string, count: number): string {
  return `Browse all ${formatNumber(count)} ZIP codes in ${stateFull}. Find demographics, population, income data, and more for every ZIP code in the state.`;
}

export function cityPageTitle(city: string, state: string): string {
  return `${city}, ${state} ZIP Codes — All ZIP Codes in ${city}, ${state}`;
}

export function cityPageDescription(city: string, stateFull: string, count: number): string {
  return `Find all ${count} ZIP code${count !== 1 ? 's' : ''} for ${city}, ${stateFull}. View demographics, population, income, and location data.`;
}

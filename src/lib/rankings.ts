import type { Metric } from '../components/RankTable.astro';

export interface RankingPage {
  slug: string;
  metric: Metric;
  key: 'population' | 'income' | 'home_value' | 'density' | 'land_area';
  title: string;
  h1: string;
  short: string;
  intro: string;
  note: string;
}

export const RANKING_PAGES: RankingPage[] = [
  {
    slug: 'richest-zip-codes',
    metric: 'income',
    key: 'income',
    title: 'Richest ZIP Codes in America – Top 100 by Household Income',
    h1: 'The 100 richest ZIP codes in America',
    short: 'Richest ZIP codes',
    intro: 'These US ZIP codes have the highest median household income, among ZIP codes with at least 1,000 residents.',
    note: 'Ranked by median household income (US Census Bureau estimates). ZIP codes with fewer than 1,000 residents are excluded to avoid small-sample noise.',
  },
  {
    slug: 'most-expensive-zip-codes',
    metric: 'home_value',
    key: 'home_value',
    title: 'Most Expensive ZIP Codes in the US – Top 100 by Home Value',
    h1: 'The most expensive ZIP codes in the US',
    short: 'Most expensive ZIP codes',
    intro: 'The US ZIP codes with the highest median home values. The Census reports values above $1 million as “$1,000,000+”, so ties at the top are listed by ZIP code.',
    note: 'Ranked by median value of owner-occupied homes. ZIP codes with fewer than 1,000 residents are excluded.',
  },
  {
    slug: 'most-populated-zip-codes',
    metric: 'population',
    key: 'population',
    title: 'Most Populated ZIP Codes in the US – Top 100 by Population',
    h1: 'The most populated ZIP codes in the United States',
    short: 'Most populated ZIP codes',
    intro: 'The 100 US ZIP codes with the most residents, according to US Census Bureau counts for ZIP Code Tabulation Areas.',
    note: 'Ranked by total population of the ZIP Code Tabulation Area.',
  },
  {
    slug: 'most-densely-populated-zip-codes',
    metric: 'density',
    key: 'density',
    title: 'Most Densely Populated ZIP Codes in the US – Top 100',
    h1: 'The most densely populated ZIP codes in the US',
    short: 'Densest ZIP codes',
    intro: 'The US ZIP codes with the most people per square mile of land — dominated by Manhattan and other big-city cores.',
    note: 'Ranked by residents per square mile of land. ZIP codes with fewer than 1,000 residents are excluded.',
  },
  {
    slug: 'largest-zip-codes',
    metric: 'land_area',
    key: 'land_area',
    title: 'Largest ZIP Codes in the US by Land Area – Top 100',
    h1: 'The largest ZIP codes in the US by area',
    short: 'Largest ZIP codes by area',
    intro: 'The biggest US ZIP codes by land area. Most are in Alaska and the rural West, where a single post office can serve thousands of square miles.',
    note: 'Ranked by land area in square miles (water excluded).',
  },
];

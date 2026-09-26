import { SITE } from '../config/site';
import { getMeta, getStates, getCities, getCounties, getAllZips, getRankings } from './data';
import { RANKING_PAGES } from './rankings';
import { GUIDES } from './guides';
import { zipUrl, cityUrl, countyUrl, stateUrl, topUrl } from './url';

export const ZIPS_PER_SITEMAP = 10000;

const xmlHeaders = { 'Content-Type': 'application/xml; charset=utf-8' };

export function urlset(paths: string[]): Response {
  const lastmod = getMeta().built;
  const body = paths
    .map((p) => `<url><loc>${SITE.url}${p}</loc><lastmod>${lastmod}</lastmod></url>`)
    .join('\n');
  return new Response(
    `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${body}\n</urlset>\n`,
    { headers: xmlHeaders }
  );
}

export function zipSitemapCount(): number {
  return Math.max(1, Math.ceil(getAllZips().size / ZIPS_PER_SITEMAP));
}

export function sitemapIndex(): Response {
  const lastmod = getMeta().built;
  const files = [
    'sitemap-pages.xml',
    'sitemap-states.xml',
    'sitemap-counties.xml',
    'sitemap-cities.xml',
    ...Array.from({ length: zipSitemapCount() }, (_, i) => `sitemap-zips-${i + 1}.xml`),
  ];
  const body = files
    .map((f) => `<sitemap><loc>${SITE.url}/${f}</loc><lastmod>${lastmod}</lastmod></sitemap>`)
    .join('\n');
  return new Response(
    `<?xml version="1.0" encoding="UTF-8"?>\n<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${body}\n</sitemapindex>\n`,
    { headers: xmlHeaders }
  );
}

export function staticPaths(): string[] {
  const states = Object.keys(getStates());
  const ranked = getRankings().states;
  return [
    '/', '/search/', '/what-is-my-zip-code/', '/states/', '/rankings/', '/tools/',
    '/tools/zip-code-distance/', '/tools/zip-code-radius/', '/guides/',
    ...GUIDES.map((g) => `/guides/${g.slug}/`),
    ...RANKING_PAGES.map((r) => `/rankings/${r.slug}/`),
    ...states.filter((s) => ranked[s]).map(topUrl),
    '/about/', '/data-sources/', '/contact/', '/privacy/', '/terms/', '/affiliate-disclosure/',
  ];
}

export const statePaths = () => Object.keys(getStates()).sort().map(stateUrl);
export const countyPaths = () => Object.keys(getCounties()).sort().map(countyUrl);
export const cityPaths = () => Object.keys(getCities()).sort().map(cityUrl);
export const zipPaths = (page: number) =>
  [...getAllZips().keys()].sort().slice((page - 1) * ZIPS_PER_SITEMAP, page * ZIPS_PER_SITEMAP).map(zipUrl);

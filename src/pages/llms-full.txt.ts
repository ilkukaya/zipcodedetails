import { SITE } from '../config/site';
import { getMeta, getStates, getRankings } from '../lib/data';

export const GET = () => {
  const m = getMeta();
  const n = (x: number | null | undefined) => (x == null ? 'n/a' : x.toLocaleString('en-US'));
  const usd = (x: number | null | undefined) => (x == null ? 'n/a' : x >= 1_000_001 ? '$1,000,000+' : '$' + n(x));
  const u = SITE.url;
  const states = Object.entries(getStates()).sort((a, b) => a[1].name.localeCompare(b[1].name));
  const r = getRankings().national;
  const list = (rows: any[], f: (x: any) => string) =>
    rows.slice(0, 25).map((x, i) => `${i + 1}. ${x.zip} — ${x.city}, ${x.state}: ${f(x)} (${u}/zip/${x.zip}/)`).join('\n');

  const body = `# ${SITE.brand} — extended summary

${SITE.description}
Coverage: ${n(m.zip_count)} ZIP codes. Source: US Census Bureau (${SITE.dataVintage}) and USPS. Updated ${m.built}.

## ZIP codes by state
| State | Code | ZIP codes | ZIP range | Cities | Counties | Median ZIP income | Page |
|---|---|---|---|---|---|---|---|
${states.map(([a, s]) => `| ${s.name} | ${a} | ${n(s.zip_count)} | ${s.zip_range[0]}–${s.zip_range[1]} | ${n(s.city_count)} | ${n(s.county_count)} | ${usd(s.median_zip_income)} | ${u}/state/${a.toLowerCase()}/ |`).join('\n')}

## Richest US ZIP codes (median household income, pop ≥ 1,000)
${list(r.income || [], (x) => usd(x.median_household_income))}

## Most expensive US ZIP codes (median home value, pop ≥ 1,000)
${list(r.home_value || [], (x) => usd(x.median_home_value))}

## Most populated US ZIP codes
${list(r.population || [], (x) => n(x.population) + ' residents')}

## Largest US ZIP codes by land area
${list(r.land_area || [], (x) => n(x.land_area_sqmi) + ' sq mi')}
`;
  return new Response(body, { headers: { 'Content-Type': 'text/plain; charset=utf-8' } });
};

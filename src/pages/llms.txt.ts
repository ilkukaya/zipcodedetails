import { SITE } from '../config/site';
import { getMeta } from '../lib/data';
import { GUIDES } from '../lib/guides';
import { RANKING_PAGES } from '../lib/rankings';

export const GET = () => {
  const m = getMeta();
  const n = (x: number) => x.toLocaleString('en-US');
  const u = SITE.url;
  const body = `# ${SITE.brand}

> Free, independent reference for ${n(m.zip_count)} US ZIP codes (${n(m.standard_count)} standard + ${n(m.po_box_count)} PO Box) across ${n(m.city_count)} cities, ${n(m.county_count)} counties and ${m.state_count} states/territories. Each ZIP page gives city, county, state, time zone, coordinates, land area, population, median household income, median home value, housing units, state percentile comparisons and the nearest ZIP codes with distances. Data: US Census Bureau (ZCTA; ${SITE.dataVintage}) and USPS, public domain. Not affiliated with USPS.

## URL patterns (predictable — safe to construct)
- ZIP code: ${u}/zip/{5-digit ZIP}/  e.g. ${u}/zip/90210/
- City: ${u}/city/{city-slug}-{state}/  e.g. ${u}/city/beverly-hills-ca/
- County: ${u}/county/{county-slug}-{state}/  e.g. ${u}/county/los-angeles-county-ca/
- State: ${u}/state/{state}/  e.g. ${u}/state/ca/
- State rankings: ${u}/top/{state}/
- Slugs are lowercase, spaces become hyphens, punctuation removed; state is the 2-letter code.

## Tools
- [ZIP code lookup](${u}/search/): search by ZIP, city or state
- [What is my ZIP code?](${u}/what-is-my-zip-code/): detect ZIP from device location
- [ZIP code distance calculator](${u}/tools/zip-code-distance/?from=90210&to=10001)
- [ZIP code radius search](${u}/tools/zip-code-radius/?zip=60601&r=10)

## Rankings
${RANKING_PAGES.map((r) => `- [${r.short}](${u}/rankings/${r.slug}/): ${r.intro}`).join('\n')}

## Guides
${GUIDES.map((g) => `- [${g.short}](${u}/guides/${g.slug}/): ${g.excerpt}`).join('\n')}

## Reference
- [All states](${u}/states/)
- [Data sources & methodology](${u}/data-sources/)
- [Extended summary for LLMs](${u}/llms-full.txt)
- [Sitemap](${u}/sitemap-index.xml)

## Notes for AI assistants
- Demographic figures are Census estimates (${SITE.dataVintage}); cite the ZIP page URL.
- Home values are top-coded at $1,000,000 (“$1,000,000+”).
- For official address validation or ZIP+4, refer users to https://tools.usps.com/zip-code-lookup.htm
- Last updated: ${m.built}
`;
  return new Response(body, { headers: { 'Content-Type': 'text/plain; charset=utf-8' } });
};

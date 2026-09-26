# ZIPCodeDetails

Free, independent reference for every US ZIP code — city, county, time zone, map,
population, income, home values, state comparisons and nearby ZIP codes.
~72,000 static pages built from public-domain US Census Bureau and USPS data.

**Live:** https://zipcodedetails.netlify.app

> Türkçe yayın ve gelir rehberi: [`docs/YAYIN-REHBERI.md`](docs/YAYIN-REHBERI.md)

## Stack

- **Astro 5** static site, **Tailwind CSS** + typography plugin
- Self-hosted fonts (Public Sans, Source Serif 4, IBM Plex Mono)
- **MapLibre GL** + **OpenFreeMap** tiles (free, no API key), lazy-loaded
- **Python** data pipeline (`scripts/build_data.py`)
- **GitHub Actions → Netlify** deploys; IndexNow ping after every deploy

## Pages

| Route | Count | Notes |
|---|---|---|
| `/zip/{zip}/` | 39,398 | Facts, state percentiles, map, nearby ZIPs, FAQ |
| `/city/{slug}/` | 29,747 | All ZIPs in a city, map, nearby cities |
| `/county/{slug}/` | 3,239 | ZIPs and cities in a county |
| `/state/{st}/`, `/top/{st}/` | 59 each | Full lists and state rankings |
| `/rankings/*` | 5 | National top-100 lists |
| `/tools/*`, `/what-is-my-zip-code/`, `/search/` | — | Client-side tools using `/zip-index.json` |
| `/guides/*` | 5 | Evergreen articles |
| `sitemap*.xml`, `robots.txt`, `llms.txt`, `llms-full.txt`, `ads.txt`, `manifest.webmanifest` | — | Generated endpoints |

## Development

```bash
pip install -r requirements.txt   # numpy (optional, speeds up the pipeline)
python3 scripts/build_data.py     # extracts pseo_zipcodes_full.rar if needed (needs bsdtar/unrar/7z)
npm install
npm run dev
```

`npm run build` runs the pipeline and a full production build (~72k pages).

## Configuration

All settings live in [`src/config/site.ts`](src/config/site.ts) and can be overridden with
environment variables — for production set them in **Netlify → Project configuration →
Environment variables**. Empty values switch the feature off cleanly.

| Variable | Purpose |
|---|---|
| `SITE_URL` | Canonical origin (e.g. `https://zipcodedetails.com`) |
| `ADSENSE_CLIENT`, `ADSENSE_SLOT_TOP`, `ADSENSE_SLOT_IN_CONTENT`, `ADSENSE_SLOT_SIDEBAR` | Google AdSense (also generates `ads.txt`) |
| `GA4_ID`, `CF_ANALYTICS_TOKEN`, `PLAUSIBLE_DOMAIN` | Analytics (any combination) |
| `GOOGLE_SITE_VERIFICATION`, `BING_SITE_VERIFICATION`, `YANDEX_VERIFICATION`, `PINTEREST_VERIFICATION` | Search console verification |
| `AFF_HOTELS_URL`, `AFF_MOVERS_URL`, `AFF_INTERNET_URL`, `AFF_INSURANCE_URL`, `AFF_SECURITY_URL` | Affiliate URL templates with `{zip} {city} {state} {stateFull} {lat} {lng} {q}` placeholders |
| `AMAZON_TAG` | Amazon Associates tag |
| `INDEXNOW_KEY` | IndexNow key (default provided) |
| `CONTACT_EMAIL`, `TWITTER_HANDLE` | Optional contact details |

## Deployment

Netlify builds the site itself (`netlify.toml`): `python3 scripts/build_data.py && npx astro build`,
using the bundled `data/pseo_zipcodes_full.json.gz`. The GitHub workflow is a build check for
pushes/PRs and pings IndexNow on `main`; it can also deploy when the repository variable
`DEPLOY_WITH_ACTIONS=true` and valid `NETLIFY_AUTH_TOKEN` / `NETLIFY_SITE_ID` secrets exist.
Run it manually with **Submit ALL URLs** checked after large content changes.

## Data

US Census Bureau ZCTA gazetteer and demographic tables, USPS ZIP data — compiled by the
[uszipcode](https://github.com/MacHu-GWU/uszipcode-project) project. Public domain.
See `/data-sources/` on the site for methodology and limitations.

# ZIPCodeDetails.com

Comprehensive US ZIP code reference platform — demographics, location data, and more for 41,000+ ZIP codes.

## Tech Stack

- **Framework**: Astro 5.x (Static Site Generator)
- **Styling**: Tailwind CSS 3.x
- **Hosting**: Netlify (Free tier)
- **Data**: US Census Bureau (ACS 5-Year Estimates + Gazetteer)
- **Maps**: Leaflet.js (lazy loaded)
- **Charts**: Chart.js (lazy loaded)
- **Analytics**: Plausible (cookie-free)

## Project Structure

```
zipcodedetails/
├── data/               # ZIP code JSON data files
│   ├── zips/           # Individual ZIP code files (41K+)
│   ├── state_index.json
│   ├── city_index.json
│   └── popular_zips.json
├── scripts/            # Python data pipeline
│   ├── 01_download_census.py
│   ├── 02_download_usps.py
│   ├── 03_merge_and_enrich.py
│   ├── 04_generate_json.py
│   ├── 05_generate_indexes.py
│   └── 06_validate.py
├── src/
│   ├── components/     # Astro components
│   ├── layouts/        # Page layouts
│   ├── lib/            # TypeScript utilities
│   └── pages/          # Route pages
└── public/             # Static assets
```

## Getting Started

### Prerequisites

- Node.js 20+
- Python 3.11+ (for data pipeline only)

### Development

```bash
npm install
npm run dev
```

### Data Pipeline

To rebuild the data from Census Bureau sources:

```bash
pip install requests pandas haversine pytz
python scripts/01_download_census.py
python scripts/02_download_usps.py
python scripts/03_merge_and_enrich.py
python scripts/04_generate_json.py
python scripts/05_generate_indexes.py
python scripts/06_validate.py
```

### Build

```bash
npm run build
```

## Data Sources

- [US Census Bureau ACS 5-Year Estimates](https://www.census.gov/programs-surveys/acs)
- [Census Gazetteer Files](https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html)
- [HUD USPS ZIP Code Crosswalk](https://www.huduser.gov/portal/datasets/usps_crosswalk.html)

## License

Data is sourced from the US Census Bureau and is in the public domain.

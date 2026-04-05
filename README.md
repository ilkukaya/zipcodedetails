# ZIPCodeDetails.com

Comprehensive US ZIP code reference platform — location and geographic data for 41,000+ ZIP codes.

## Tech Stack

- **Framework**: Astro 5.x (Static Site Generator)
- **Styling**: Tailwind CSS 3.x
- **Hosting**: Netlify (Free tier)
- **Data**: US Census Bureau (Gazetteer) + USPS Postal Data
- **Maps**: Leaflet.js (lazy loaded)
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
pip install -r requirements.txt
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

- [Census Gazetteer Files](https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html)
- [USPS ZIP Codes by Area and District Codes](https://postalpro.usps.com/ZIP_Locale_Detail)

## License

Data is sourced from the US Census Bureau and USPS, and is in the public domain.

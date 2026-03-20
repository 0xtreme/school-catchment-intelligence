# school-catchment-intelligence

Australia-wide school catchment intelligence app using official public datasets.

## What this project does

- Ingests Australian school profile and location data (ACARA)
- Joins schools to ABS SA2 affordability and population context
- Loads official state catchment polygons where openly published (NSW, VIC, QLD)
- Scores schools by quality + affordability + teaching-capacity proxy
- Publishes an interactive map with filters and evidence charts

## Data integrity rules

- No synthetic or fabricated records
- Derived fields are deterministic formulas only
- Source URLs and methods are documented in `docs/sources.md`

## Quick start

```bash
npm install
npm run build
npm run serve
```

Then open `http://127.0.0.1:4173/public/`.

## Project structure

- `scripts/fetch-data.mjs`: Downloads all raw official sources
- `scripts/build-dataset.mjs`: Builds school and catchment outputs
- `scripts/sync-pages-assets.mjs`: Mirrors `public/` assets into `docs/` for GitHub Pages
- `data/raw/`: Raw source files + source manifest
- `data/processed/`: Processed JSON/CSV outputs
- `public/`: Static app files
- `docs/`: GitHub Pages app files

## Main outputs

- `data/processed/school-catchment-dataset.json`
- `data/processed/top-school-recommendations.csv`
- `data/processed/catchments/NSW.geojson`
- `data/processed/catchments/VIC.geojson`
- `data/processed/catchments/QLD.geojson`

# Data Sources and Method Notes

This app uses official public datasets and deterministic transformations only.

## Official sources

1. ACARA My School Data Access Program
   - School Profile 2025 (ICSEA, enrolments, staffing)
   - School Location 2025 (lat/lon, SA2, remoteness)
   - URL: https://acaraweb.azurewebsites.net/contact-us/acara-data-access

2. ABS Data API
   - Census 2021 G02 SA2 medians (income, rent, mortgage and related medians)
   - Annual ERP ASGS2021 (population and growth context)
   - URL: https://data.api.abs.gov.au/

3. NSW Department of Education catchments (Data NSW)
   - NSW government school intake zones
   - URL: https://data.nsw.gov.au/

4. Victorian Department of Education catchments (DataVic)
   - Victorian Government School Zones 2026
   - URL: https://discover.data.vic.gov.au/

5. Queensland Department of Education catchments (data.qld.gov.au)
   - Primary, junior secondary, and senior secondary catchments 2025
   - URL: https://www.data.qld.gov.au/

## Scoring approach

For each school, a composite `value_score` is calculated on a 0-100 scale:

- 50% school quality percentile (ICSEA)
- 35% affordability percentile (inverse SA2 housing burden)
- 15% teaching-capacity percentile (inverse student-teacher ratio)

Where:

- housing burden is estimated from ABS SA2 medians:
  - weekly rent / weekly family income
  - annualised mortgage repayment / annualised family income

## Coverage note

- Schools: Australia-wide (ACARA)
- Catchment polygons: currently integrated from official open datasets for NSW, VIC, and QLD.

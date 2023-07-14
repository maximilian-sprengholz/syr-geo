# Regio data prep

Data approach:
  - offer everything that is available:
    - Regierungsbezirk
    - smallest entity per indicator and year (should be ok to use all 2022 stuff; offer complete list to allow others to check for additional data - which we could merge easily?)
  - specifically requested stuff: 
    - Ausländerstatistik
    - Konfessionen

## Fetch

- INKAR (use `bonn` pkg for R, which uses an undocumented API)
- DESTATIS (use RESTful/JSON API)
- BBSR?
- AZR?

## Merge

Merge everything in one dataset per geo level? or all in one?
- Wohngebiet
- PLZ 
- Kreis
  - get shapefiles for aggregation
  - assignment possibilities:
    - wohngebiet centroid (definite)
    - wohngebiet max. shared area (definite)
    - wohngebiet shared area as weight per kreis (would only work with expanded dataset and corresponding weights similar to income estimation in Microcensus; for any estimation SE clustering is necessary)
- RB (direct correspondence)


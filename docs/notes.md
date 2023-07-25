# Regio data prep

## Approach

- offer everything that is available:
  - smallest entity per indicator and year (should be ok to use all 2022 stuff; offer complete list to allow others to check for additional data - which we could merge easily?)
- crosswalk by area to postcode / wohngebiet

## Fetch

- [x] INKAR (use `bonn` pkg for R, which uses an undocumented API) [complete for 2020 data]
- [x] DESTATIS (use RESTful/JSON API) [complete for Bevölkerungs- und Ausländerstatistik]
- [] BBSR pop by nationality city grids
- [] AZR(?)

## Publish

- Datenbereitstellung SYR mitdenken:
  - Lizenz
  - Geodaten OK?
  - Google Daten OK?
# SYR Survey Geographical Context Data

This repository fetches and processes geographical context data which supplement a self-conducted online survey in the research project [Seeing Your Religion](https://www.dezim-institut.de/en/dezim-research-community/research-alliance-on-discrimination-and-racism/fodira-projekt-seeing-your-religion/).

## Project organization

```
.
├── .gitignore
├── environment.yml
├── LICENSE.md
├── README.md
├── docs                    <- Documents
└── src                     <- Source code
```

## Notes
- The following data sources are used:
  - Bundesagentur für Arbeit
  - Breitbandatlas
  - Destatis
  - DWD
  - Google
  - INKAR
  - IVW
- There is no master file. For each data source, the fetch + process routine in `src` has to be run manually (`src/BA_amr_process.do` is Stata code, the rest `R` code)
- Whenever data is not fetched in the code, the data was available to us locally, but is not shared in this repo. Fetching via API (Destatis and Google) requires API keys.
- We harmonize external data geographies over time using crosswalks (e.g., due to reform of administrative units such as municipalities).
- All processed data is merged via `src/merge`, where input geographies from externmal sources are translated to the output geographies from the survey (postcode, individual self-reported residential area) also via crosswalks.

## License

This project is licensed under the terms of the [MIT License](/LICENSE.md)
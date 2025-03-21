# SYR Survey Geographical Context Data

This repository fetches and processes geographical/spatial context data from external sources which supplement the self-conducted SYR online survey in the research project [Seeing Your Religion — Regional Variation of Anti-Muslim Discrimination and Racism on the German Labor Market](https://www.dezim-institut.de/en/dezim-research-community/research-alliance-on-discrimination-and-racism/fodira-projekt-seeing-your-religion/).

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
- We use the following data sources:
  - Bundesagentur für Arbeit
  - Breitbandatlas
  - Destatis
  - DWD
  - Google
  - INKAR
  - IVW
- There is no master file. For each data source, the fetch + process routine in `src` has to be run manually (`src/BA_amr_process.do` is Stata code, the rest `R` code)
- Whenever data is not fetched in the code, the data was available to us locally, but is not shared in this repo. Fetching via API (Destatis and Google) requires API keys in `src/.env`
- We harmonize external data geographies over time using crosswalks (e.g., due to reform of administrative units such as municipalities).
- All processed data is merged via `src/merge.R`, where input geographies from externmal sources are translated to the output geographies from the survey (postcode, individual self-reported residential area) also via crosswalks.
- Shout-out to the authors of these great packages: [`ags`](https://github.com/sumtxt/ags), [`bonn`](https://github.com/sumtxt/bonn), [`dotenv`](https://github.com/gaborcsardi/dotenv), [`exactextractr`](https://github.com/isciences/exactextractr), [`googleway`](https://github.com/SymbolixAU/googleway), [`httr`](https://github.com/r-lib/httr2), [`jsonlite`](https://github.com/jeroen/jsonlite), [`lwgeom`](https://github.com/r-spatial/lwgeom), [`ncdf4`](https://cran.r-project.org/web/packages/ncdf4/index.html), [`pbapply`](https://github.com/psolymos/pbapply), [`restatis`](https://github.com/CorrelAid/restatis), [`sf`](https://github.com/r-spatial/sf), [`terra`](https://github.com/rspatial/terra), [`tidyterra`](https://github.com/dieghernan/tidyterra), [`tidyverse`](https://github.com/tidyverse)

## License

This project is licensed under the terms of the [MIT License](/LICENSE.md)
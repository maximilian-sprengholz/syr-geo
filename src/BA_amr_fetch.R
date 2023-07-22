#
# This file:
# - fetches Arbeitsmarkreports (amr) from the BA Website for all Kreise as of Nov 2022
#

### config (relative to working dir syr-geo!)
source("src/_config.R")

### packages

# conda managed
library(rvest)
library(stringr)


### scraper

# URL-Komponenten
base_url <- "https://statistik.arbeitsagentur.de/SiteGlobals/Forms/Suche/Einzelheftsuche_Formular.html"
url_pt2 <- "?gtp=15084_list%253D"
url_pt4 <- "&regiontype_f=Politisch&topic_f=amr-amr&dateOfRevision=202211-202211"

# Ergebnisseiten durchlaufen
for (x in 1:1) {
  # URL für jede Ergebnisseite erstellen
  link <- paste0(base_url, url_pt2, x, url_pt4)
  
  # Seite abrufen
  page <- read_html(link)
  
  # Alle Links zu den einzelnen Spreadsheets sammeln
  links <- page %>% 
    html_nodes("a") %>% 
    html_attr("href")
  
  # Durch alle Links iterieren und Spreadsheets herunterladen
  for (spreadsheet_link in links) {
    isxlsx <- str_detect(spreadsheet_link, "\\.*xlsx")
    # Prüfen, ob der Link ein Spreadsheet ist
    if (!is.na(isxlsx)) {
      if (isxlsx == TRUE) {
        # Spreadsheet herunterladen
        linkstub <- str_extract(spreadsheet_link, ".*.xlsx")
        # Dateiname extrahieren
        file_name <- basename(linkstub)
        download.file(
          paste0("https://www.statistik.arbeitsagentur.de", linkstub), 
          paste0(data, "/external/raw/BA/amr/", file_name)
          )
        }
      }
    }
  # add short break before next page
  Sys.sleep(1) 
  }
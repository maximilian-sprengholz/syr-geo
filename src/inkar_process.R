#
# This file fetches INKAR data via API (pkg bonn):
# - ALL indicators available
#   - for 2020 or the next available year before (currently most recent year)
#   - for the smallest available level (should be Gemeinde or Kreis)
#

### config (relative to working dir syr-geo!)
source("src/_config.R")

### packages

# conda managed
library(dplyr)
library(jsonlite)
library(readr)
library(stringr)
library(tibble)

# CRAN
if (!require(pbapply)) install.packages("pbapply")
library(pbapply)

# github
if (!require(bonn)) remotes::install_github("sumtxt/bonn", force=TRUE)

### xwalk 2020 -> 2022 #############################################################################

### variable index: use to identify xwalk categories; and save that info to the index
# determine by regex; SHKs can correct
# very few indicators relate to area, these can be manually specified here
# for pop we can regex absolute stuff easily, rest is relative (most indicators)

df_vars <- read_delim(paste0(data, "/external/processed/INKAR/variable_index.csv"), delim = ";")

# variables
names <- df_vars$Name
nonnumeric <- c(
  "Zentralörtlicher Status",
  "Zentralörtlicher Status (zusammengefasst)",
  "Zentralörtlicher Status der Gemeinde (differenziert)",
  "Stadt-/Gemeindetyp des BBSR",
  "Raumtyp nach Lage des BBSR",
  "Regionalstatistischer Raumtyp für die Mobilitäts- und Verkehrsforschung",
  "Höchste medizinische Versorgungsstufe je Gemeinde"
  ) # these will not have weights -> not possible to assign
# area abs
area_w_abs <- c("Katasterfläche in km²")
# area rel
pattern <- c(
  "je km²", 
  "je m²",
  "Überschuss der Stickstoff-Flächenbilanz",
  "an der Fläche in %"
  )
area_w_rel <- names[str_detect(names, paste0(pattern, collapse = "|"))]
# pop abs
pattern <- c(
  "Allgemeinbildende Schulen mit Förderschwerpunkt",
  "Schüler an allgemeinbildenden Schulen mit Förderschwerpunkt$",
  "Bruttoinlandsprodukt (BIP) absolut in Millionen Euro",
  "^Zahl\\s.*",
  "^Anzahl\\s.*"
)
pop_w_abs <- names[str_detect(names, paste0(pattern, collapse = "|"))]

# generate indicator denoting which weight is appropriate
df_vars <- df_vars %>%
  mutate(xwalk_time_weight = case_when(
    Name %in% area_w_abs ~ "area_w_abs",
    Name %in% area_w_rel ~ "area_w_rel",
    Name %in% pop_w_abs ~ "pop_w_abs",
    Name %in% nonnumeric ~ NA,
    .default = "pop_w_rel"
  ))

# update variable index
write_delim(df_vars, paste0(data, "/external/processed/INKAR/variable_index.csv"), delim = ";")

### xwalk 2020 -> 2022
for (geo in c("gem", "gvb", "kre")) {
  
  # read
  df <- read_delim(paste0(data, "/external/raw/INKAR/", tolower(geo), ".csv"), delim = ";")
  df <- df %>% select(!matches(paste0(geo, "_pop|", ".*_year"))) # detailed year info in `raw`
  
  # weight
  dfs <- list()
  for (w in c("area", "pop")) {
    for (wref in c("abs", "rel")) {
      vars <- df_vars[
        df_vars$xwalk_time_weight == paste0(w, "_w_", wref) & df_vars$Raumbezug == toupper(geo), 
        "variable"
        ]
      vars <- vars[!is.na(vars)]
      if (length(vars) > 0) {
        dfs[[paste0(w, "_w_", wref)]] <- xwalk(
          df = df,
          geo = geo,
          to = 2022,
          ars = paste0(geo, "_ars"),
          arsref = 2020, 
          weight = w,
          weightref = wref,
          variables = vars
          )
        }
      }
    } 
  
  # merge
  df <- dfs[[1]]
  if (length(dfs) > 1) {
    sapply(
      dfs[2:length(dfs)],
      function(df_w) {
        #message(colnames(df_w))
        df <<- merge(
          df, 
          df_w %>% select(!any_of(paste0(geo, "_ars_", c("name", "ref")))), 
          by = paste0(geo, "_ars")
          )
        TRUE
        }
      )
    }

  # order
  vars <- df_vars[!is.na(df_vars$xwalk_time_weight) & df_vars$Raumbezug == toupper(geo), "variable"]
  df <- df %>% select(matches(paste0(geo, "_ars")), all_of(vars[!is.na(vars)])) 

  # write
  write_delim(df, paste0(data, "/external/processed/INKAR/", tolower(geo), ".csv"), delim = ";")

}
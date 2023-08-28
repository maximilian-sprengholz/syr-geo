#
# This file merges Destatis data (fetched via API) for selected indicators
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
library(tidyr)
library(readxl)

# CRAN
if (!require(pbapply)) install.packages("pbapply")
library(pbapply)


### get print numbers for überregionale tageszeitungen

# read data
df <- read_excel(paste0(data, "/external/raw/IVW/IVWGES_neu.xlsx"))
df <- df %>% 
    select(all_of(
      c("AGS", "Titel/Gesamtbel", "Einzelbelegung", "Ort", "Art", "Gesamt P+eP", "EW ges", "HH ges")
      ))
colnames(df) <- c(
  "gem_ars", "name", "name2", "place_name", "place_type", "auflage", "pop_p", "pop_hh"
  )

# filter überregionale tageszeitungen; ab Auflage 20.000/Tag in D; Q4 2022
# BILD, Sueddeutsche, FAZ, Welt, Neues Deutschland
# Handelsblatt, taz, ZEIT nicht Teil der Daten ... (eventuell Fehler, könnten wir sicher
# noch mal nachhaken)
tz <- c(
  "BILD/B.Z. Deutschland-Gesamt",
  "Süddeutsche Zeitung",
  "Frankfurter Allgemeine",
  "DIE WELT",
  "Neues Deutschland Gesamt"
)
df <- df %>% filter(name %in% tz & name2 %in% tz)

# drop rest regions; small dev. from total
df <- df %>% filter(place_type != "R")

# look up what aggregation is: is no row = 0 auflage?
df <- df %>% mutate(p = case_when(
  place_type %in% c("S", "G") ~ "G",
  .default = "K"
))
df <- df %>% mutate(kre_ars = substring(gem_ars, 1, 5))
print(df %>% group_by(name, kre_ars, p) %>% summarize(csum = sum(auflage)), n = 100)

# it seems that the data has both aggregated and disaggregated values:
# place_type == K data is always aggregated from S (cities) and G (Gemeinden)
# for kreisfreie Staedte, this means that K = S
# K can be dropped; then also the ars works (AGS 8-digit code)

# gen auflage per person / hh in gemeinde (according to their own data)
df <- df %>%
  mutate(
    auflage_pref = auflage * 1000 / pop_p,
    auflage_hhref = auflage * 1000 / pop_hh
    )

# reshape non-K rows
df <- df %>% 
  filter(p != "K") %>% 
  select(gem_ars, name, auflage_pref, auflage_hhref) %>%
  mutate(name = case_when(
    name == "BILD/B.Z. Deutschland-Gesamt" ~ "bild",
    name == "Süddeutsche Zeitung" ~ "sz",
    name == "Frankfurter Allgemeine" ~ "faz",
    name == "DIE WELT" ~ "welt",
    name == "Neues Deutschland Gesamt" ~ "nd"
    )) %>%
  arrange(as.numeric(gem_ars), name) %>%
  pivot_wider(
    names_from = name,
    names_glue = "{.value}_{name}",
    values_from = c(auflage_pref, auflage_hhref),
    values_fill = 0 # NA = auflage 0
    )

# merge to ars base file; reference is 2021
df_ars <- read_csv(paste0(data, "/external/processed/ars/ars2021.csv"))
df <- tibble(merge(
  df_ars %>% select(gem_ars),
  df,
  by = "gem_ars",
  all.x = TRUE,
  all.y = FALSE
))

# varlist
vars <- c(
  paste0("auflage_pref_", c("bild", "faz", "nd", "sz", "welt")),
  paste0("auflage_hhref_", c("bild", "faz", "nd", "sz", "welt"))
  )

# fill NAs with 0
df <- df %>% mutate(across(all_of(vars), function(x) ifelse(is.na(x), 0, x)))

# xwalk to 2022 geo units
df <- xwalk(
  df = df, 
  geo = "gem", 
  to = 2022,
  ars = "gem_ars",
  arsref = 2021,
  weight = "pop",
  weightref = "rel",
  variables = vars
  )

# write
write_delim(
  df, 
  paste0(data, "/external/processed/IVW/gem.csv"), 
  delim = ";", 
  na = ""
  )


### save to variable index

# description names of print media
descnames <- c(
  "BILD/B.Z.",
  "Süddeutsche Zeitung",
  "Frankfurter Allgemeine",
  "DIE WELT",
  "Neues Deutschland"
)

# build data
varindex <- tibble(
  variable = vars,
  desc = c(
    paste(descnames, "Auflage pro 1000 Personen"), 
    paste(descnames, "Auflage pro 1000 Haushalte")
    )
)
varindex$geo <- "gem"
varindex$year <- 2021
varindex$month <- 11
varindex$source <- "IVW"
varindex$source_detail <- "Informationsgemeinschaft zur Feststellung der Verbreitung von Werbeträgern e. V.; Referenzwoche 08.-14.11.2021"
varindex$xwalk_time_weight <- "pop_w_rel"
varindex$xwalk_geo_weight <- "area_w_rel"

# write
write_delim(
  varindex, 
  paste0(data, "/external/processed/IVW/variable_index.csv"), 
  delim = ";", 
  na = ""
  )
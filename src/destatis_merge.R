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

# CRAN
if (!require(pbapply)) install.packages("pbapply")
library(pbapply)
if (!require(restatis)) install.packages("restatis")
library(restatis)
# gen_auth_save() # do once manually, only works with libssl.so.1.1; needs to be in shared env

# github
if (!require(ags)) remotes::install_github("sumtxt/ags", force=TRUE)
library(ags)


### foreigners by nationality per kreis ############################################################
# https://www-genesis.destatis.de/genesis//online?operation=table&code=12521-0041

# process and combine files
# Create nationality groups based on Pfündel et al. 2021, p. 38
# - auslaender
# - auslaender_mehrheit_muslime (nationalities where >50 % are Muslim & Serbien (-> Yugosl. corresp.))
# - auslaender_muslime: estimated for all countries noted (which are the most important)
df <- NULL
muslimshare <- tibble(
  nat = c(
    "Afghanistan", "Bangladesch", "Iran", "Pakistan", "Irak", "Jordanien", "Libanon", "Syrien",
    "Jemen", "Saudi-Arabien", "Vereinigte Arabische Emirate", "Marokko", "Ägypten", "Algerien",
    "Libyen", "Tunesien", "Albanien", "Bosnien und Herzegowina", "Kosovo", "Montenegro", 
    "Nordmazedonien", "Serbien", "Türkei",
    # the following: weighted average of constituents
    "Jugoslawien",
    "Serbien und Kosovo",
    "Serbien und Montenegro"
    ),
  share = c(
    93.5, 86.0, 29.0, 96.3, 37.4, 89.3, 92.7, 86.5, 96.2, 96.2, 96.2, 95.1, 86.1, 86.1, 86.1, 86.1,
    58.1, 57.2, 86.9, 64.4, 80.0, 48.0, 87.2, 
    67.4,
    70.9,
    49.5
    )
)

# loop over yearly files
refyear <- 2020
for (year in 1998:refyear) {
  
  ### read raw data
  df_year <- read_delim(
    paste0(data, "/external/raw/Destatis/Ausländerstatistik/kre_", year, ".csv"),
    delim = ";"
    )
  # keep relevant columns and label them
  df_year <- df_year %>% 
    select(Zeit, `1_Auspraegung_Code`, matches("[2-9]_Auspraegung_Label|BEV\\S"))
  colnames(df_year) <- c("zeit", "kre_ars", "geschlecht", "staatsang", "auslaender")
  # rename nationalities (repeated names do not matter, will be summed up)
  df_year[df_year$staatsang %in% c(
    "Jugoslawien, Soz. Föd. Republik (bis 26.04.1992)",
    "Jugoslawien, Bundesrep. (27.04.1992-04.02.2003)"),
    "staatsang"] <- "Jugoslawien"
  df_year[df_year$staatsang == "Jugoslawien, Soz. Föd. Republik (bis 26.04.1992)",  "staatsang"] <- "Jugoslawien"
  df_year[df_year$staatsang == "Montenegro (ab 03.06.2006)", "staatsang"] <- "Montenegro"
  df_year[df_year$staatsang == "Serbien (einschl. Kosovo) (03.06.2006-16.02.2008)", 
  "staatsang"] <- "Serbien und Kosovo"
  df_year[df_year$staatsang == "Serbien und Montenegro (05.02.2003-02.06.2006)", 
  "staatsang"] <- "Serbien und Montenegro"
  # counts as numeric, replace NA with 0
  df_year$auslaender <- as.numeric(df_year$auslaender)
  df_year[is.na(df_year$auslaender), "auslaender"] <- 0
  # lowercase
  df_year$geschlecht <- tolower(df_year$geschlecht)

  ### calculations
  # calculate n for preodiminantly Muslim nationalities
  df_wide1 <- df_year %>% 
    mutate(staatsang_gruppe = case_when(
      staatsang == "Insgesamt" ~ "insgesamt",
      staatsang %in% muslimshare$nat[!muslimshare$nat %in% c("Iran", "Irak")] ~ "muslim_staat",
      .default = "andere"
    )) %>% 
    group_by(kre_ars, geschlecht, staatsang_gruppe) %>% 
    summarize(auslaender = sum(auslaender)) %>%
    pivot_wider(names_from = staatsang_gruppe, values_from = auslaender, names_prefix = "auslaender_") %>%
    select(!c("auslaender_andere")) %>% rename(auslaender = auslaender_insgesamt)
  # calculate n from Muslim shares from all nationalities noted in Pfuendel et al. table
  df_year <- merge(df_year, muslimshare, by.x="staatsang", by.y="nat", all.x = TRUE, all.y = FALSE)
  df_year[is.na(df_year$share), "share"] <- 0
  df_wide2 <- df_year %>% 
    group_by(kre_ars, geschlecht) %>% 
    mutate(auslaender_muslim = auslaender * share/100) %>% 
    summarize(auslaender_muslim_est = sum(auslaender_muslim))
  df_year <- merge(df_wide1, df_wide2, by=c("kre_ars", "geschlecht"))

  ### "crosswalk": weight by pop
  # cases where no disaggregated data is available: arbitrary new ids
  df_year$kre_ars_merged <- df_year$kre_ars
  df_year[
    df_year$kre_ars %in% c("10041", "10042", "10043", "10044", "10045", "10046"), 
    "kre_ars_merged"
    ] <- "10099" # Saarland
  if (year >= 2008) df_year[df_year$kre_ars %in% c("06611", "06633"), "kre_ars_merged"] <- "06699" # Kassel
  if (year >= 2013) df_year[df_year$kre_ars %in% c("12052", "12071"), "kre_ars_merged"] <- "12099" # Cottbus
  # merge pop data by which to weight
  df_pop <- read_delim(paste0(data, "/external/raw/Destatis/Bevölkerung/kre.csv"), delim = ";")  
  df_year <- merge(df_year, df_pop %>% filter(zeit == paste0("31.12.", year)), by="kre_ars")
  # weight
  vars <- c("auslaender", "auslaender_muslim_staat", "auslaender_muslim_est")
  df_year <- df_year %>%
    filter(!is.na(pop)) %>% # this is also a check if the kreis exists at all at this timepoint
    mutate(auslaender = sum(auslaender), .by = c("kre_ars_merged", "geschlecht")) %>%
    mutate(auslaender_muslim_staat = sum(auslaender_muslim_staat), .by = c("kre_ars_merged", "geschlecht")) %>%
    mutate(auslaender_muslim_est = sum(auslaender_muslim_est), .by = c("kre_ars_merged", "geschlecht")) %>%
    mutate(popsum = sum(pop), .by = c("kre_ars_merged", "geschlecht")) %>%
    mutate_at(
      vars,
      ~ round(.x * pop/popsum)
      )

### ags crosswalk until 2020
df_xwalk <- NULL
for (g in unique(df_year$geschlecht)) {
  ars <- xwalk_ags(
    data = df_year %>% 
      filter(!is.na(pop) & geschlecht == g) %>%
      select(!c(geschlecht)) %>%
      mutate(zeit = as.numeric(substr(zeit, 7, 10))),
    ags = "kre_ars",
    time = "zeit",
    xwalk = "xd20",
    variables = vars,
    weight = "pop"
    )
  if (g != "insgesamt") {
    if (g == "männlich") sfx <- "_m" else sfx <- "_f"
    ars <- rename_with(ars, ~ paste0(.x, sfx), starts_with("auslaender"))
    ars <- ars %>% ungroup() %>% select(!c(zeit, ags20))
  }
  df_xwalk <- bind_cols(df_xwalk, ars %>% ungroup())
  }

  ### merge all years
  df <- bind_rows(df, df_xwalk)

}

# label
df <- df %>% rename(year = zeit, kre_ars = ags20) %>% mutate(kre_ars_ref = refyear) 
df_ars <- read_csv(paste0(data, "/external/processed/ars/ars", ref, ".csv"))
df_ars <- df_ars %>% distinct(kre_ars, kre_name)
df <- merge(df, df_ars, by="kre_ars")
cols <- colnames(df)
firstcols <- c("kre_ars", "kre_name", "kre_ars_ref", "year")
df <- df %>% select(all_of(c(firstcols, cols[!cols %in% firstcols])))

### xwalk again for 2020 -> 2022

# generic function
xwalk22 <- function(df, lvl, arscol, arsref = 2020, yearcol = NA, weight = "pop", variables) {
  # read correspondence table
  df_xwalk <- read_delim(
    paste0(data, "/external/processed/ars/xwalk_", tolower(lvl), "_", arsref, "_2022.csv"),
    delim = ";"
    )
  # merge to passed df
  df <- merge(df, df_xwalk, by.x = arscol, by.y = paste0(lvl, "_ars_", arsref), all.x = TRUE)
  # grouping
  arsnew = paste0(lvl, "_ars_2022")
  namenew = paste0(lvl, "_name_2022")
  if (!is.na(yearcol)) groupcols <- c(arsnew, namenew, yearcol) else groupcols <- c(arsnew, namenew)
  # weight data
  df <- df %>% 
    mutate_at(variables, ~ round(.x * .data[[paste0(weight, "_w")]])) %>%
    group_by_at(groupcols) %>%
    summarize(across(variables, ~ sum(.x, na.rm = TRUE))) %>%
    rename(!!sym(arscol) := arsnew) %>%
    rename(!!sym(paste0(arscol, "_name")) := namenew) %>%
    mutate(!!sym(paste0(arscol, "_ref")) := 2022) %>%
    select(matches(paste0("^", arscol, ".*")), !!sym(yearcol), sort(colnames(.)))
}

# xwalk
df <- xwalk22(
  df = df, 
  lvl = "kre", 
  arscol = "kre_ars",
  yearcol = "year", 
  weight = "pop", 
  variables = colnames(df)[str_detect(colnames(df), "^auslaender_\\S*")]
  )
# save
write_delim(df, paste0(data, "/external/processed/Destatis/kre.csv"), delim = ";")

# Table notes:
# 06611 Kassel, kreisfreie Stadt,
# 06633 Kassel, Landkreis:
# Kassel documenta-Stadt und der Landkreis Kassel werden von
# einer Ausländerbehörde bearbeitet und können daher nicht
# getrennt ausgewiesen werden [ab Berichtsjahr 2008, MS].

# 10041 Regionalverband Saarbrücken, Landkreis,
# 10042 Merzig-Wadern, Landkreis,
# 10043 Neunkirchen, Landkreis,
# 10044 Saarlouis, Landkreis,
# 10045 Saarpfalz-Kreis,
# 10046 Sankt Wendel, Landkreis:
# Für Saarland liegen keine Daten nach Kreisen vor, weil es im
# Saarland nur eine einzige für alle Kreise zuständige
# Ausländerbehörde gibt. Alle Fälle des Saarlandes sind im
# Kreis "Saarlouis" nachgewiesen, in dem diese Behörde ihren
# Sitz hat [durchgehend in Daten, MS].

# 12052 Cottbus, kreisfreie Stadt,
# 12071 Spree-Neiße, Landkreis:
# Cottbus und der Landkreis Spree-Neiße werden von einer
# Ausländerbehörde bearbeitet und können daher nicht getrennt
# ausgewiesen werden (ab Berichtsjahr 2013).
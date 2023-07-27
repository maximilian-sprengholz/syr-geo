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
for (year in 1998:2022) {
  
  ### read raw data
  df_year <- read_delim(
    paste0(data, "/external/raw/Destatis/Ausländerstatistik/kre_", year, ".csv"),
    delim = ";",
    show_col_types = FALSE
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
  df_pop <- read_delim(
    paste0(data, "/external/raw/Destatis/Bevölkerung/kre.csv"), 
    delim = ";",
    show_col_types = FALSE
    )  
  df_year <- merge(df_year, df_pop %>% filter(zeit == paste0("31.12.", year)), by = "kre_ars")
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
      ) %>%
    select(!c(pop, popsum, kre_ars_merged))
  
  ### reshape to wide and gather new varnames
  df_year <- df_year %>%
    rename(year = zeit) %>%
    mutate(year = as.numeric(substr(year, 7, 10))) %>%
    pivot_wider(
      names_from = geschlecht, 
      values_from = all_of(vars)
      )
  colnames(df_year) <- gsub("_insgesamt", "", colnames(df_year))
  colnames(df_year) <- gsub("_männlich", "_m", colnames(df_year))
  colnames(df_year) <- gsub("_weiblich", "_w", colnames(df_year))
  vars <- colnames(df_year)[str_detect(colnames(df_year), "auslaender")]

  ### xwalks by time
  # first to 2020
  if (year <= 2019) {
    df_year <- xwalk_ags(
      data = df_year,
      ags = "kre_ars",
      time = "year",
      xwalk = "xd20",
      variables = vars,
      weight = "pop"
      )
    df_year <- df_year %>% rename(kre_ars = ags20)
    arsref <- 2020
  } else {
    arsref <- year
  }
  # then to 2022
  df_year <- xwalk(
    df = df_year, 
    geo = "kre", 
    to = 2022,
    ars = "kre_ars",
    arsref = arsref,
    years = "year", 
    weight = "pop",
    weightref = "abs",
    variables = vars
    )

  # merge all years
  df <- bind_rows(df, df_year)

}

# reshape wide
vars <- colnames(df)[str_detect(colnames(df), "auslaender")]
df <- df %>% pivot_wider(
  names_from = year, 
  values_from = all_of(vars)
)

# save
write_delim(df, paste0(data, "/external/processed/Destatis/kre.csv"), delim = ";")


### save to variable index

# build data
desc <- c(
  "ohne deutsche Staatsangehörigkeit",
  "ohne deutsche, mit Staatsangehörigkeit eines mehrh. muslimischen Herkunftslandes (wichtigste)",
  "ohne deutsche Staatsangehörigkeit; mit muslimischer Religionsangehörigkeit"
)
vars <- colnames(df)[str_detect(colnames(df), "auslaender")]
gender <- c("Personen", "Männer", "Frauen")
varindex <- tibble(
  variable = vars,
  desc = rep(
    paste("Anzahl", c(paste(gender, desc[1]), paste(gender, desc[2]), paste(gender, desc[3]) )), 
    each = length(vars)/9
    ),
  desc_detail = NA
)
varindex[str_detect(varindex$variable, "_staat"), "desc_detail"] <- paste(
  "Nach Pfündel et al. (2021):",
  paste(muslimshare$nat[!muslimshare$nat %in% c("Iran", "Irak")], collapse = ", ")
)
varindex[str_detect(varindex$variable, "_est"), "desc_detail"] <- paste(
  "Schätzung basiert auf dem in Pfündel et al. (2021) ausgewiesenen Anteil an Muslim*innen an",
  "Personen mit Migrationshintergrund (in Deutschland) aus bestimmten Herkunftsländern (hier mit",
  "Staatsangehörigkeit gleichgesetzt; inkl. Iran/Irak):", paste(muslimshare$nat, collapse = ", ")
  )
varindex$geo <- "kre"
varindex$year <- as.numeric(str_extract(varindex$variable, "\\d+"))
varindex$month <- 12
varindex$source <- "Destatis"
varindex$source_detail <- "Ausländerstatistik"
varindex <- varindex %>% mutate(xwalk_time_weight = ifelse(year < 2022, "pop_w_abs", NA))
varindex$xwalk_geo_weight <- "area_w_abs"

# append or new file
file <- paste0(data, "/external/processed/Destatis/variable_index.csv")
if (file.exists(file)) {
  varindex_old <- read_delim(file, delim = ";")
  varindex <- bind_rows(varindex, varindex_old)
  varindex <- varindex %>% distinct(variable, .keep_all = TRUE)
}
write_delim(varindex, file, delim = ";", na = "")

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
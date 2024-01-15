#
# This file merges BBA data (mobile and broadband internet coverage and infrastructure)
# from June and December 2022
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
if (!require(readxl))install.packages("readxl")

#
# Breitband-Infrastruktur (KRE)
#

### merge data
prefixes <- paste0("bb_", 
  c(
    "funkmast_n", "glasfaser_km", "holzmast_n", "hvt_n", "kvz_n",
    "leerrohr_km", "pop_n", "richtfunk_km", "ampel_n", "strassenlaterne_n"
    )
  )

df1 <- read_excel(paste0(data, "/external/raw/BBA/BBA_06_2022.xlsx"), sheet=13, skip=3)
df1 <- subset(df1, Verwaltungsebene == "3 - Kreis")
df1 <- df1 %>% select(-Land, -Verwaltungsebene)
colnames(df1) <- c("kre_ars", "kre_ars_name", paste0(prefixes, "_jun"))

df2 <- read_excel(paste0(data, "/external/raw/BBA/BBA_12_2022.xlsx"), sheet=13, skip=3)
df2 <- subset(df2, Verwaltungsebene == "3 - Kreis")
df2 <- df2 %>% select(-Land, -Verwaltungsebene) 
colnames(df2) <- c("kre_ars", "kre_ars_name", paste0(prefixes, "_dez"))

df_infra <- merge(df1, df2 %>% select(-kre_ars_name), by = "kre_ars")
df_infra$kre_ars_ref <- "2022"

# missing = 0
df_infra <- df_infra %>% replace(is.na(.), 0)

# create difference between months
jun_columns <- paste0(prefixes, "_jun")
dez_columns <- paste0(prefixes, "_dez")
diff_columns <- paste0(prefixes, "_diff")
df_infra[, diff_columns] <- round(df_infra[, dez_columns] - df_infra[, jun_columns], 2)

# order
df_infra <- df_infra %>%
  select(
    kre_ars, kre_ars_name, kre_ars_ref,
    starts_with(prefixes)
  )

# write
write_delim(df_infra, paste0(data, "/external/processed/BBA/kre.csv"), delim = ";")


### save to variable index

# variables
vars <- colnames(df_infra)
vars <- vars[4:length(vars)]

# description 
desc_names <- c(
  "Funkmast (Anzahl)",
  "Glasfaserleitung (km)",
  "Holzmast (Anzahl)",
  "Hauptverteiler (HVt) (Anzahl)",
  "Kabelverzweiger (KVz)",
  "Schutz-/Leerrohr (km)",
  "Point of Presence (PoP) (Anzahl)",
  "Richtfunkstrecke (km)",
  "Lichtzeichenanlage (Ampel) (Anzahl)",
  "Straßenlaterne (Anzahl)"
)
desc <- c()
for (name in desc_names) {
  desc <- c(desc, paste0(name, c(", Juni", ", Dezember", ", Differenz Dezember - Juni")))
}

# build data
varindex <- tibble(
  variable = vars,
  desc = desc
)
varindex$geo <- "kre"
varindex$year <- 2022
varindex$month <- rep(c(6, 12, 12), length(desc_names))
varindex$source <- "BBA"
varindex$source_detail <- "Breitbandatlas der zentralen Informationsstelle des Bundes (ZIS) der Bundesnetzagentur"
varindex$xwalk_geo_weight <- "area_w_abs"

# write
write_delim(
  varindex, 
  paste0(data, "/external/processed/BBA/variable_index.csv"),
  delim = ";", 
  na = ""
  )


#
# Privathaushalte (GEM)
#

### merge data
prefixes <- paste0("bb_hh_", c(paste0("fn", c(16, 30, 50, 100)), "5G"))

df1 <- read_excel(paste0(data, "/external/raw/BBA/BBA_06_2022.xlsx"), sheet=2, skip=3)
df1 <- subset(df1, Verwaltungsebene == "4 - Gemeinde")
df1 <- df1[, c(1,2,7:10,34)]
colnames(df1) <- c("gem_ars", "gem_ars_name", paste0(prefixes, "_jun"))

df2 <- read_excel(paste0(data, "/external/raw/BBA/BBA_12_2022.xlsx"), sheet=2, skip=3)
df2 <- subset(df2, Verwaltungsebene == "4 - Gemeinde")
df2 <- df2[, c(1,2,7:10,32)]
colnames(df2) <- c("gem_ars", "gem_ars_name", paste0(prefixes, "_dez"))

df_hh <- merge(df1, df2 %>% select(-gem_ars_name), by = "gem_ars")
df_hh$gem_ars_ref <- "2022"

# missing = 0
df_hh <- df_hh %>% replace(is.na(.), 0)

# create difference between months, order
order <- c()
for (prefix in prefixes) {
  order <- c(order, paste0(prefix, c("_jun", "_dez", "_diff")))
  df_hh[[paste0(prefix, "_diff")]] <- round(
    df_hh[[paste0(prefix, "_dez")]] - df_hh[[paste0(prefix, "_jun")]], 
    2
    )
}
df_hh <- df_hh %>% select(starts_with("gem"), all_of(order))

# write
write_delim(df_hh, paste0(data, "/external/processed/BBA/gem.csv"), delim = ";")


### save to variable index

# variables
vars <- names(df_hh)
vars <- vars[4:length(vars)]

# description         
desc_names <- c(
  paste("Festnetzverfügbarkeit", c(16, 30, 50, 100), "Mbit/s (%), Privathaushalte"),
  "5G Verfügbarkeit (%), Privathaushalte"
  )
desc <- c()
for (name in desc_names) {
  desc <- c(desc, paste0(name, c(", Juni", ", Dezember", ", Differenz Dezember - Juni")))
}

# build data
varindex <- tibble(
  variable = vars,
  desc = desc
)
varindex$geo <- "gem"
varindex$year <- 2022
varindex$month <- rep(c(6, 12, 12), length(desc_names))
varindex$source <- "BBA"
varindex$source_detail <- "Breitbandatlas der zentralen Informationsstelle des Bundes (ZIS) der Bundesnetzagentur"
varindex$xwalk_geo_weight <- "area_w_rel"

# write (append)
varindex <- bind_rows(
  read_delim(paste0(data, "/external/processed/BBA/variable_index.csv"), delim = ";", ),
  varindex
  )
write_delim(
  varindex, 
  paste0(data, "/external/processed/BBA/variable_index.csv"),
  delim = ";", 
  na = ""
  )


#
# Fläche
#

### merge data
prefixes <- paste0("bb_flaeche_", "5G") # just one

df1 <- read_excel(paste0(data, "/external/raw/BBA/BBA_06_2022.xlsx"), sheet=3, skip=3)
df1 <- subset(df1, Verwaltungsebene == "4 - Gemeinde")
df1 <- df1[, c(1,2,10)]
colnames(df1) <- c("gem_ars", "gem_ars_name", paste0(prefixes, "_jun"))

df2 <- read_excel(paste0(data, "/external/raw/BBA/BBA_12_2022.xlsx"), sheet=3, skip=3)
df2 <- subset(df2, Verwaltungsebene == "4 - Gemeinde")
df2 <- df2[, c(1,2,10)]
colnames(df2) <- c("gem_ars", "gem_ars_name", paste0(prefixes, "_dez"))

df_flaeche <- merge(df1, df2 %>% select(-gem_ars_name), by = "gem_ars")
df_flaeche$gem_ars_ref <- "2022"

# missing = 0
df_flaeche <- df_flaeche %>% replace(is.na(.), 0)

# create difference between months, order
order <- c()
for (prefix in prefixes) {
  order <- c(order, paste0(prefix, c("_jun", "_dez", "_diff")))
  df_flaeche[[paste0(prefix, "_diff")]] <- round(
    df_flaeche[[paste0(prefix, "_dez")]] - df_flaeche[[paste0(prefix, "_jun")]], 
    2
    ) 
}
df_flaeche <- df_flaeche %>% select("gem_ars", all_of(order))

# merge & write (merge hh & area)
write_delim(
  merge(
    read_delim(paste0(data, "/external/processed/BBA/gem.csv"), delim = ";"),
    df_flaeche, 
    by = "gem_ars"
    ), 
  paste0(data, "/external/processed/BBA/gem.csv"), 
  delim = ";"
  )


### save to variable index

# variables
vars <- names(df_flaeche)
vars <- vars[4:length(vars)]

# description         
desc_names <- c(
  "5G Verfügbarkeit (%), Fläche"
  )
desc <- c()
for (name in desc_names) {
  desc <- c(desc, paste0(name, c(", Juni", ", Dezember", ", Differenz Dezember - Juni")))
}
# build data
varindex <- tibble(
  variable = vars,
  desc = desc
)
varindex$geo <- "gem"
varindex$year <- 2022
varindex$month <- rep(c(6, 12, 12), length(desc_names))
varindex$source <- "BBA"
varindex$source_detail <- "Breitbandatlas der zentralen Informationsstelle des Bundes (ZIS) der Bundesnetzagentur"
varindex$xwalk_geo_weight <- "area_w_rel"

# write (append)
varindex <- bind_rows(
  read_delim(paste0(data, "/external/processed/BBA/variable_index.csv"), delim = ";", ),
  varindex
  )
write_delim(
  varindex, 
  paste0(data, "/external/processed/BBA/variable_index.csv"),
  delim = ";", 
  na = ""
  )
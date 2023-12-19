install.packages("languageserver")
install.packages("httpgd") 
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
if (!require(readxl))install.packages("readxl")


df1 <- read_excel(paste0(data, "/external/raw/BBA/BBA_06_2022.xlsx", sheet = 13)
head(df)

df1 <- subset(df1, Verwaltungsebene == "3 - Kreis")

df1 <- df1 %>%
  select(-Land, -Verwaltungsebene)


#Datensatz Dezember laden
df2 <- read_excel(paste0(data, "/external/raw/BBA/BBA_12_2022.xlsx", sheet = 13)
df2 <- subset(df2, Verwaltungsebene == "3 - Kreis")

df2 <- df2 %>%
  select(-Land, -Verwaltungsebene) 

merged_df <- merge(df1, df2, by = "AGS")

ls(merged_df)

merged_df <- merged_df[, !(colnames(merged_df) %in% c("Name.y"))]

colnames(merged_df)[colnames(merged_df) == "Name.x"] <- "kre_ars_name"

colnames(merged_df)[colnames(merged_df) == "AGS"] <- "kre_ars"
merged_df$kre_ars_ref <- "2022"


colnames(merged_df) <- c("kre_ars", "kre_ars_name", "funkmast_n_x", "glasfaser_km_x", "holzmast_n_x", "hvt_n_x", "kvz_n_x", "leerrohr_km_x", "pop_n_x", "richtfunk_km_x", "ampel_n_x", "strassenlaterne_n_x", "funkmast_n_y", "glasfaser_km_y", "holzmast_n_y", "hvt_n_y", "kvz_n_y", "leerrohr_km_y", "pop_n_y", "richtfunk_km_y", "ampel_n_y", "strassenlaterne_n_y", "kre_ars_ref")

colnames(merged_df) <- gsub("_x", "_jun", gsub("_y", "_dez", colnames(merged_df)))

prefixes <- c("funkmast_n", "glasfaser_km", "holzmast_n", "hvt_n", "kvz_n",
              "leerrohr_km", "pop_n", "richtfunk_km", "ampel_n", "strassenlaterne_n")

jun_columns <- paste0(prefixes, "_jun")


dez_columns <- paste0(prefixes, "_dez")


diff_columns <- paste0(prefixes, "_diff")


merged_df[, diff_columns] <- merged_df[, dez_columns] - merged_df[, jun_columns]

merged_df <- merged_df %>%
  select(
    kre_ars, kre_ars_name, kre_ars_ref,
    starts_with("funkmast_n"),
    starts_with("glasfaser_km"),
    starts_with("holzmast_n"),
    starts_with("hvt_n"),
    starts_with("kvz_n"),
    starts_with("leerrohr_km"),
    starts_with("pop_n"),
    starts_with("richtfunk_km"),
    starts_with("ampel_n"),
    starts_with("strassenlaterne_n")
  )

merged_df <- merged_df %>% replace(is.na(.), 0)

# write
write_delim(
  merged_df, 
  paste0(data, "/external/processed/BBA/kre.csv.csv"), 
  delim = ";", 
  na = ""
  )


### save to variable index

#description infrastructure BBA
base_names <- c(
  "Amtlicher Gemeindeschlüssel auf Kreisebene",
  "Kreisname",
  "Referenzjahr",
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

# Define time periods
time_periods <- c("Juni", "Dezember", "Differenz Dezember-Juni")

# Create the vector of column names without time periods for specific base names
descnames <- unlist(lapply(base_names, function(base) {
  if (base %in% c("Amtlicher Gemeindeschlüssel auf Kreisebene", "Kreisname", "Referenzjahr")) {
    base
  } else {
    lapply(time_periods, function(period) {
      paste(base, period, sep = " ")
    })
  }
}))

# build data
varindex <- names(merged_df)
varindex <- data.frame(varindex)
varindex$geo <- "kre"
varindex$year <- 2022
varindex$month <- "6 und 12"
varindex$source <- "BBA"
varindex$source_detail <- "Breitbandatlas der zentralen Informationsstelle des Bundes (ZIS) der 
                          Bundesnetzagentur"


# write
write_delim(
  varindex, 
  paste0(data, "/external/processed/BBA/variable_index.csv"), 
  delim = ";", 
  na = ""
  )

#Gemeindebene
df1 <- read_excel(paste0(data, "/external/raw/BBA/BBA_06_2022.xlsx", sheet = 2)


df1 <- subset(df1, Verwaltungsebene == "4 - Gemeinde")

df1 <- df1 %>%
  select(-Land,- Raumkategorie, -Verwaltungsebene, -Kreis,)

df1 <- df1 %>%
  select(-`≥ 400 Mbit/s...20`, -`≥ 1000 Mbit/s...21`)

spaltennamen <- colnames(df1)
spaltennamen


# Aktuelle Spaltennamen
current_names <- c("ags", "name", "all_tech_16_mbits", "all_tech_30_mbits", "all_tech_50_mbits",
                   "all_tech_100_mbits", "all_tech_200_mbits", "all_tech_400_mbits", "all_tech_1000_mbits",   "fttbh_1000_mbits",
                   "fttc_16_mbits", "fttc_30_mbits", "fttc_50_mbits", "fttc_100_mbits", "fttc_200_mbits",
                   "hfc_16_mbits", "hfc_30_mbits", "hfc_50_mbits",
                   "hfc_100_mbits", "hfc_200_mbits", "hfc_400_mbits", "hfc_1000_mbits", "sont_16_mbits",
                   "sont_30_mbits", "2g", "4g", "5g_dss", "5g")

# Ersetze die unerwünschten Zeichen und passe die Schreibweise an
new_names <- tolower(current_names)
new_names <- gsub(" ", "_", new_names)
new_names <- gsub("/", "_", new_names)
new_names <- gsub("≥", "", new_names)
new_names <- gsub(" mbit/s.*", "_mbits", new_names)
new_names <- gsub("mbit/s", "mbits", new_names)
new_names <- gsub("\\W\\d*$", "", new_names)  # Entfernt Punkte und Zahlen am Ende
new_names <- gsub("^_", "", new_names)  # Entfernt _ am Anfang jeder Variable

new_names <- paste0(new_names, "_jun")

colnames(df1) <- new_names


# Ändere "ags_jun" in "ags"
colnames(df1)[colnames(df1) == "ags_jun"] <- "ags"



#Datensatz Dezember laden
df2 <- read_excel(paste0(data, "/external/raw/BBA/BBA_12_2022.xlsx", sheet = 2)

df2 <- subset(df2, Verwaltungsebene == "4 - Gemeinde")

df2 <- df2 %>%
  select(-Land,- Raumkategorie, -Verwaltungsebene, -Kreis)

spaltennamen <- colnames(df2)
spaltennamen

# Aktuelle Spaltennamen
current_names <- c("ags", "name", "all_tech_16_mbits", "all_tech_30_mbits", "all_tech_50_mbits",
                   "all_tech_100_mbits", "all_tech_200_mbits", "all_tech_400_mbits", "all_tech_1000_mbits",   "fttbh_1000_mbits",
                   "fttc_16_mbits", "fttc_30_mbits", "fttc_50_mbits", "fttc_100_mbits", "fttc_200_mbits",
                   "hfc_16_mbits", "hfc_30_mbits", "hfc_50_mbits",
                   "hfc_100_mbits", "hfc_200_mbits", "hfc_400_mbits", "hfc_1000_mbits", "sont_16_mbits",
                   "sont_30_mbits", "2g", "4g", "5g_dss", "5g")

# Ersetze die unerwünschten Zeichen und passe die Schreibweise an
new_names <- tolower(current_names)
new_names <- gsub(" ", "_", new_names)
new_names <- gsub("/", "_", new_names)
new_names <- gsub("≥", "", new_names)
new_names <- gsub(" mbit/s.*", "_mbits", new_names)
new_names <- gsub("mbit/s", "mbits", new_names)
new_names <- gsub("\\W\\d*$", "", new_names)  # Entfernt Punkte und Zahlen am Ende
new_names <- gsub("^_", "", new_names)  # Entfernt _ am Anfang jeder Variable


new_names <- paste0(new_names, "_dez")

colnames(df2) <- new_names

colnames(df2)[colnames(df2) == "ags_dez"] <- "ags"

merged_df2 <- merge(df1, df2, by = "ags")

colnames(merged_df2)[colnames(merged_df2) == "name_dez"] <- "gem_ars_name"

colnames(merged_df2)[colnames(merged_df2) == "ags"] <- "gem_ars"
merged_df2$gem_ars_ref <- "2022"

merged_df2 <- merged_df2 %>%
  select(-"name_jun")

prefixes <- c("all_tech_16_mbits", "all_tech_30_mbits", "all_tech_50_mbits", 
              "all_tech_100_mbits", "all_tech_200_mbits", "all_tech_400_mbits", 
              "all_tech_1000_mbits", "fttbh_1000_mbits", "fttc_16_mbits", 
              "fttc_30_mbits", "fttc_50_mbits", "fttc_100_mbits", 
              "fttc_200_mbits", "hfc_16_mbits", "hfc_30_mbits", "hfc_50_mbits", 
              "hfc_100_mbits", "hfc_200_mbits", "hfc_400_mbits", "hfc_1000_mbits", 
              "sont_16_mbits", "sont_30_mbits", "2g", "4g", "5g_dss", "5g")

for (prefix in prefixes) {
  diff_col_name <- paste0(prefix, "_diff")
  merged_df2[[diff_col_name]] <-  merged_df2[[paste0(prefix, "_dez")]] - merged_df2[[paste0(prefix, "_jun")]] 
}

spaltennamen <- colnames(merged_df2)
spaltennamen

gewuenschte_spalten <- c(
  "gem_ars", "gem_ars_name", "gem_ars_ref", "all_tech_16_mbits_jun", "all_tech_16_mbits_dez", "all_tech_16_mbits_diff",
  "all_tech_30_mbits_jun", "all_tech_30_mbits_dez", "all_tech_30_mbits_diff",
  "all_tech_50_mbits_jun", "all_tech_50_mbits_dez", "all_tech_50_mbits_diff",
  "all_tech_100_mbits_jun", "all_tech_100_mbits_dez", "all_tech_100_mbits_diff",
  "all_tech_200_mbits_jun", "all_tech_200_mbits_dez", "all_tech_200_mbits_diff",
  "all_tech_400_mbits_jun", "all_tech_400_mbits_dez", "all_tech_400_mbits_diff",
  "all_tech_1000_mbits_jun", "all_tech_1000_mbits_dez", "all_tech_1000_mbits_diff",
  "fttbh_1000_mbits_jun", "fttbh_1000_mbits_dez", "fttbh_1000_mbits_diff",
  "fttc_16_mbits_jun", "fttc_16_mbits_dez", "fttc_16_mbits_diff",
  "fttc_30_mbits_jun", "fttc_30_mbits_dez", "fttc_30_mbits_diff",
  "fttc_50_mbits_jun", "fttc_50_mbits_dez", "fttc_50_mbits_diff",
  "fttc_100_mbits_jun", "fttc_100_mbits_dez", "fttc_100_mbits_diff",
  "fttc_200_mbits_jun", "fttc_200_mbits_dez", "fttc_200_mbits_diff",
  "hfc_16_mbits_jun", "hfc_16_mbits_dez", "hfc_16_mbits_diff",
  "hfc_30_mbits_jun", "hfc_30_mbits_dez", "hfc_30_mbits_diff",
  "hfc_50_mbits_jun", "hfc_50_mbits_dez", "hfc_50_mbits_diff",
  "hfc_100_mbits_jun", "hfc_100_mbits_dez", "hfc_100_mbits_diff",
  "hfc_200_mbits_jun", "hfc_200_mbits_dez", "hfc_200_mbits_diff",
  "hfc_400_mbits_jun", "hfc_400_mbits_dez", "hfc_400_mbits_diff",
  "hfc_1000_mbits_jun", "hfc_1000_mbits_dez", "hfc_1000_mbits_diff",
  "sont_16_mbits_jun", "sont_16_mbits_dez", "sont_16_mbits_diff",
  "sont_30_mbits_jun", "sont_30_mbits_dez", "sont_30_mbits_diff",
  "2g_jun", "2g_dez", "2g_diff",
  "4g_jun", "4g_dez", "4g_diff",
  "5g_dss_jun", "5g_dss_dez", "5g_dss_diff",
  "5g_jun", "5g_dez", "5g_diff"
)

# Anordnen Sie die Spalten entsprechend der gewünschten Reihenfolge
merged_df2 <- merged_df2[, gewuenschte_spalten]

merged_df2 <- merged_df2 %>% replace(is.na(.), 0)



df1 <- read_excel(paste0(data, "/external/raw/BBA/BBA_06_2022.xlsx", sheet = 3, skip = 3)


df1 <- subset(df1, Verwaltungsebene == "4 - Gemeinde")

df1 <- df1 %>%
  select(-Land,- Raumkategorie, -Verwaltungsebene, -Kreis,)


spaltennamen <- colnames(df1)
spaltennamen

# Aktuelle Spaltennamen
current_names <- c("ags", "name", "all_2g_jun" , "all_4g_jun" , "all_5g_dSS_jun", "all_5g_jun", "telefónica_2g_jun",    "telefónica_4g_jun","telefónica_5g_dss_jun" ,"telefónica_5g_jun", "telekom_2g_jun", "telekom_4g_jun","telekom_5g _dss_jun" ,"telekom_5g_jun", "vodafone_2g_jun","vodafone_4g_jun", "vodafone_5g_dss_jun", "vodafone_5g_jun")

new_names <- paste0(current_names)

colnames(df1) <- new_names

#Datensatz Dezember laden
df2 <- read_excel(paste0(data, "/external/raw/BBA/BBA_12_2022.xlsx", sheet=3, skip=3)

df2 <- subset(df2, Verwaltungsebene == "4 - Gemeinde")

df2 <- df2 %>%
  select(-Land,- Raumkategorie, -Verwaltungsebene, -Kreis)

spaltennamen <- colnames(df2)
spaltennamen

current_names <- c("ags", "name", "all_2g_dez" , "all_4g_dez" , "all_5g_dSS_dez", "all_5g_dez", "telefónica_2g_dez",    "telefónica_4g_dez","telefónica_5g_dss_dez" ,"telefónica_5g_dez", "telekom_2g_dez", "telekom_4g_dez","telekom_5g _dss_dez" ,"telekom_5g_dez", "vodafone_2g_dez","vodafone_4g_dez", "vodafone_5g_dss_dez", "vodafone_5g_dez")

new_names <- paste0(current_names)

colnames(df2) <- new_names

merged_df3 <- merge(df1, df2, by = "ags")

colnames(merged_df3)[colnames(merged_df3) == "name.y"] <- "gem_ars_name"

colnames(merged_df3)[colnames(merged_df3) == "ags"] <- "gem_ars"
merged_df3$gem_ars_ref <- "2022"

merged_df3 <- merged_df3 %>%
  select(-"name.x")

spaltennamen <- colnames(merged_df3)
spaltennamen

merged_df3$all_2g_diff <-  merged_df3$all_2g_dez - merged_df3$all_2g_jun 
merged_df3$all_4g_diff <-  merged_df3$all_4g_dez - merged_df3$all_4g_jun 
merged_df3$all_5g_dSS_diff <-  merged_df3$all_5g_dSS_dez - merged_df3$all_5g_dSS_jun 
merged_df3$all_5g_diff <- merged_df3$all_5g_dez -  merged_df3$all_5g_jun 
merged_df3$telefónica_2g_diff <- merged_df3$telefónica_2g_dez - merged_df3$telefónica_2g_jun 
merged_df3$telefónica_4g_diff <-  merged_df3$telefónica_4g_dez - merged_df3$telefónica_4g_jun 
merged_df3$telefónica_5g_dss_diff <-  merged_df3$telefónica_5g_dss_dez - merged_df3$telefónica_5g_dss_jun 
merged_df3$telefónica_5g_diff <-  merged_df3$telefónica_5g_dez - merged_df3$telefónica_5g_jun 
merged_df3$telekom_2g_diff <-  merged_df3$telekom_2g_dez - merged_df3$telekom_2g_jun 
merged_df3$telekom_4g_diff <-  merged_df3$telekom_4g_dez - merged_df3$telekom_4g_jun 
merged_df3$vodafone_2g_diff <-  merged_df3$vodafone_2g_dez - merged_df3$vodafone_2g_jun 
merged_df3$vodafone_4g_diff <-  merged_df3$vodafone_4g_dez - merged_df3$vodafone_4g_jun 
merged_df3$vodafone_5g_dss_diff <-  merged_df3$vodafone_5g_dss_dez - merged_df3$vodafone_5g_dss_jun 
merged_df3$vodafone_5g_diff <-  merged_df3$vodafone_5g_dez - merged_df3$vodafone_5g_jun 



spaltennamen <- colnames(merged_df3)
spaltennamen

gewuenschte_spalten <- c(
  "gem_ars", "gem_ars_name", "gem_ars_ref", "all_2g_jun", "all_2g_dez", "all_2g_diff",
  "all_4g_jun", "all_4g_dez", "all_4g_diff", "all_5g_dSS_jun", "all_5g_dSS_dez", "all_5g_dSS_diff",
  "all_5g_jun", "all_5g_dez", "all_5g_diff", "telefónica_2g_jun", "telefónica_2g_dez", "telefónica_2g_diff",
  "telefónica_4g_jun", "telefónica_4g_dez", "telefónica_4g_diff", "telefónica_5g_dss_jun", "telefónica_5g_dss_dez", "telefónica_5g_dss_diff",
  "telefónica_5g_jun", "telefónica_5g_dez", "telefónica_5g_diff", "telekom_2g_jun", "telekom_2g_dez", "telekom_2g_diff",
  "telekom_4g_jun", "telekom_4g_dez", "telekom_4g_diff", "vodafone_2g_jun", "vodafone_2g_dez", "vodafone_2g_diff",
  "vodafone_4g_jun", "vodafone_4g_dez", "vodafone_4g_diff", "vodafone_5g_dss_jun", "vodafone_5g_dss_dez", "vodafone_5g_dss_diff",
  "vodafone_5g_jun", "vodafone_5g_dez", "vodafone_5g_diff"
)

# Anordnen Sie die Spalten entsprechend der gewünschten Reihenfolge
merged_df3 <- merged_df3[, gewuenschte_spalten]


merged_df3 <- merged_df3 %>% replace(is.na(.), 0)

merged_df4 <- merge(merged_df2, merged_df3, by = "gem_ars")

merged_df3 <- merged_df3 %>%
  select(-"gem_ars_name")

merged_df3 <- merged_df3 %>%
  select(-"gem_ars_ref")

spaltennamen <- colnames(merged_df4)
spaltennamen

merged_df4 <- merge(merged_df2, merged_df3, by = "gem_ars")

write_delim(merged_df4, "gem.csv", delim = ";")

#try to solve umlauts problem
write.table(merged_df4, file = 'gem.csv', sep = ';', row.names = FALSE, col.names = TRUE, quote = FALSE, fileEncoding = 'UTF-8')

write.csv(merged_df4, file = 'gem.csv', row.names = FALSE, fileEncoding = 'UTF-8')



#
# This file:
# - unifies the Amtlicher Regionalschlüssel (ARS) levels across 2019-2022
# - these files are the basis to merge together contextual info on each level
#

### config (relative to working dir syr-geo!)
source("src/_config.R")

### packages

# conda managed
library(dplyr)
library(readxl)
library(readr)
library(stringr)
library(tibble)

### ARS ############################################################################################

### process GV data to consistent format
process_gv <- function(year) {
  # read and name
  fpath <- paste0(data, "/external/raw/Destatis/GV/3112", year, "_Auszug_GV.xlsx")
  gv <- read_excel(fpath, sheet = 2, skip = 6, col_names = FALSE)
  gv <- gv[,1:13]
  gv <- gv[,-2]
  colnames(gv) <- c(
    "satzart", "bl", "rbz", "kre", "gvb", "gem", "name", 
    "area", "pop", "pop_m", "pop_w", "popdensity"
    )
  # split aggregate levels and merge to gemeinde level
  gv <- gv %>% filter(satzart %in% c("10", "40", "50", "60")) 
  bl <- gv %>% filter(satzart == "10") %>% select(bl, name) %>% rename(bl_name = name)
  kre <- gv %>% 
    filter(satzart == "40") %>% 
    mutate(kre = paste0(bl, rbz, kre)) %>% 
    select(kre, name) %>% 
    rename(kre_name = name)
  gvb <- gv %>% 
    filter(satzart == "50") %>% 
    mutate(gvb = paste0(bl, rbz, kre, gvb)) %>% 
    select(gvb, name) %>%
    rename(gvb_name = name)
  gv <- gv %>% 
    filter(satzart == "60") %>%
    mutate(rbz = paste0(bl, rbz)) %>%
    mutate(kre = paste0(rbz, kre)) %>%
    mutate(gvb = paste0(kre, gvb)) %>% 
    mutate(gem = paste0(gvb, gem))
  # recode and lab rbz manually to correspond to INKAR
  gv <- gv %>% mutate(
    rbz = case_when(
      rbz %in% c("031", "032", "033", "034") ~ "030",
      rbz %in% c("071", "072", "073") ~ "070",
      rbz %in% c("145", "146", "147") ~ "140",
      .default = rbz
    )
  )
  gv <- gv %>% mutate(rbz_name = case_when(
    rbz == "010" ~	"Schleswig-Holstein",
    rbz == "020" ~	"Hamburg",
    rbz == "030" ~	"Niedersachsen",
    rbz == "040" ~	"Bremen",
    rbz == "051" ~	"Düsseldorf",
    rbz == "053" ~	"Köln",
    rbz == "055" ~	"Münster",
    rbz == "057" ~	"Detmold",
    rbz == "059" ~	"Arnsberg",
    rbz == "064" ~	"Darmstadt",
    rbz == "065" ~	"Gießen",
    rbz == "066" ~	"Kassel",
    rbz == "070" ~	"Rheinland-Pfalz",
    rbz == "081" ~	"Stuttgart",
    rbz == "082" ~	"Karlsruhe",
    rbz == "083" ~	"Freiburg",
    rbz == "084" ~	"Tübingen",
    rbz == "091" ~	"Oberbayern",
    rbz == "092" ~	"Niederbayern",
    rbz == "093" ~	"Oberpfalz",
    rbz == "094" ~	"Oberfranken",
    rbz == "095" ~	"Mittelfranken",
    rbz == "096" ~	"Unterfranken",
    rbz == "097" ~	"Schwaben",
    rbz == "100" ~	"Saarland",
    rbz == "110" ~	"Berlin",
    rbz == "120" ~	"Brandenburg",
    rbz == "130" ~	"Mecklenburg-Vorpommern",
    rbz == "140" ~	"Sachsen",
    rbz == "150" ~	"Sachsen-Anhalt",
    rbz == "160" ~	"Thüringen"
  ))
  # merge to original
  gv <- merge(gv, bl, by="bl", all.x = TRUE, all.y = FALSE)
  gv <- merge(gv, kre, by="kre", all.x = TRUE, all.y = FALSE)
  gv <- merge(gv, gvb, by="gvb", all.x = TRUE, all.y = FALSE)
  gv <- gv %>% select(-c(satzart)) %>% 
    select(
      gem, name, area, pop, pop_m, pop_w, bl, bl_name, rbz, rbz_name, kre, kre_name, gvb, 
      gvb_name
      ) %>%
    rename(gem_ars = gem) %>%
    rename(gem_name = name) %>%
    rename(rbz_ars = rbz) %>%
    rename(bl_ars = bl) %>%
    rename(kre_ars = kre) %>%
    rename(gvb_ars = gvb) %>%
    mutate(year = year) # add year for clarity
}
years <- 2019:2022
dfs_gv <- lapply(years, process_gv)
names(dfs_gv) <- paste0("gv", years)

### provide correspondence from base year to target year 2022
# gemeinden changed, so did the ars -> allow merge of data from different years
process_gvchange <- function(year) {
  # read and name
  fpath <- paste0(data, "/external/raw/Destatis/GV/3112", year, "_Aenderungen_GV.xlsx")
  gv <- read_excel(fpath, sheet = 2, skip = 7, col_names = FALSE)
  gv <- gv[, c(2, 3, 9)]
  colnames(gv) <- c("lvl", paste0("ars_", year-1), paste0("ars_", year))
  # split aggregate levels and merge to gemeinde level
  gv <- gv %>% filter(lvl == "Gemeinde") %>% select(-c(lvl))
}

df_ars <- dfs_gv$gv2019 %>% select(gem_ars)
colnames(df_ars) <- c("ars_2019")
for (year in 2020:2022) {
  df_ars <- merge(df_ars, process_gvchange(year), by=paste0("ars_", year-1), all.x = TRUE)
  df_ars <- df_ars %>% mutate(
    !!sym(paste0("ars_", year)) := case_when(
      is.na(!!sym(paste0("ars_", year))) ~ !!sym(paste0("ars_", year-1)),
      .default = !!sym(paste0("ars_", year)) 
      )
    )
}

df_ars <- df_ars[,c(sort(colnames(df_ars)))]
colnames(df_ars) <- paste0("gem_", colnames(df_ars))
df_ars <- df_ars %>%
  mutate(bl_ars_2022 = substr(gem_ars_2022,1,2)) %>%
  mutate(rbz_ars_2022 = substr(gem_ars_2022,1,3)) %>%
  mutate(
    rbz_ars_2022 = case_when(
      rbz_ars_2022 %in% c("031", "032", "033", "034") ~ "030",
      rbz_ars_2022 %in% c("071", "072", "073") ~ "070",
      rbz_ars_2022 %in% c("145", "146", "147") ~ "140",
      .default = rbz_ars_2022
    )
  ) %>%
  mutate(kre_ars_2022 = substr(gem_ars_2022,1,5)) %>%
  mutate(gvb_ars_2022 = substr(gem_ars_2022,1,9))

# merge to gv
merge_2022 <- function(df_gv, df_ars) {
  year <- df_gv$year[1] # constant
  if (year < 2022) {
    df_gv <- merge(
      df_gv, df_ars %>% select(matches(paste0(c(year,"2022"), "$", collapse="|"))), 
      by.x="gem_ars", by.y=paste0("gem_ars_", year), all.x = TRUE, all.y = FALSE
    )
  }
  return(df_gv)
}
dfs_gv <- lapply(dfs_gv, merge_2022, df_ars)

### merge plz (info from 31.12.2021, accessed Feb 28, 2023; last updated Feb 2022)
# https://downloads.suche-postleitzahl.org/v2/public/zuordnung_plz_ort.csv
df_ags_plz <- read_csv(paste0(data, "/external/raw/OSM/zuordnung_plz_ort.csv"))
dfs_gv <- lapply(dfs_gv, function(df_gv, df_ags_plz) {
    # create ags for merging (=ars without gemeindevarband code in between)
    if (df_gv$year[1] == 2022) df_gv <- rename(df_gv, gem_ars_2022 = gem_ars)
    df_gv <- df_gv %>% mutate(ags = paste0(substr(gem_ars_2022,1,5), substr(gem_ars_2022,10,12)))
    # merge
    df_gv <- merge(df_gv, df_ags_plz %>% select(ags, plz), by="ags", all.x = TRUE, all.y = FALSE)
    # rename again
    if (df_gv$year[1] == 2022) df_gv <- rename(df_gv, gem_ars = gem_ars_2022)
    # drop ags
    df_gv <- df_gv %>% select(-c(ags))
    },
    df_ags_plz
  )

# export, but DROP if pop 0 prior
dfs_gv <- lapply(dfs_gv, function(df_gv) {
  year <- df_gv$year[1]
  df_gv <- df_gv %>% filter(pop > 0)
  write_csv(df_gv, paste0(data, "/external/processed/ars/ars", year, ".csv"))
  })

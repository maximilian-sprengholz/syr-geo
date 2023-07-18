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
    mutate(gem_ars = paste0(substr(gem,1,5), substr(gem,10,12))) %>% # AGS, is used in all off. data
    select(
      gem, gem_ars, name, area, pop, pop_m, pop_w, bl, bl_name, rbz, rbz_name, kre, kre_name, gvb, 
      gvb_name
      ) %>%
    rename(gem_ars_full = gem) %>% # full digits (included gvb ids)
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

# crosswalks are necessary between years
# crosswalks until 2020 are provided in the `ags` package
# manual extension: crosswalk.R

# export
dfs_gv <- lapply(dfs_gv, function(df_gv) {
  year <- df_gv$year[1]
  # we keep the entities without pop to allow for area based aggregation,
  # but we drop 3 GEM which have no KRE and are missing in most official data (INKAR, Shapefiles)
  df_gv <- df_gv %>% filter(!gem_ars_full %in% c("070009999999", "100429999999", "130009999999"))
  write_csv(df_gv, paste0(data, "/external/processed/ars/ars", year, ".csv"))
  })

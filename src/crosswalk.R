#
# This file performs geo crosswalks:
# - over time for the same units
# - between different geo units for the same time
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
library(terra)
library(sf)
sf_use_s2(FALSE)
library(units)

# CRAN
if (!require(pbapply)) install.packages("pbapply")
library(pbapply)
if (!require(tidyterra)) install.packages("tidyterra")
library(tidyterra)

# github
if (!require(ags)) remotes::install_github("sumtxt/ags", force=TRUE)
library(ags)


### Shapefiles German administrative units #########################################################
# https://daten.gdz.bkg.bund.de/produkte/vg/vg5000_1231/aktuell/vg5000_12-31.utm32s.shape.ebenen.zip
# RBZ seem to be incomplete
# historically available since 2020: possible to use for xwalk if area is not noted somewhere else
p <- vect(paste0(data, "/external/raw/Shapefiles/vg/vg5000_ebenen_1231/VG5000_KRS.shp"))

### XXX ############################################################################################

data(btw_sn)
df <- tibble(btw_sn)
btw_sn_ags20 <- xwalk_ags(
  data=btw_sn, 
  ags="district", 
  time="year", 
  xwalk="xd20"
  )
btw_sn_ags20


### GV #############################################################################################

#
# DUMP! does not work (you'd need to build a list of dfs dfs_gv from `external/processed/ars`)
#

### provide correspondence from base year to target year 2022
# gemeinden changed, so did the ars -> allow merge of data from different years
process_gvchange <- function(year) {
  # read and name
  fpath <- paste0(data, "/external/raw/Destatis/GV/3112", year, "_Aenderungen_GV.xlsx")
  gv <- read_excel(fpath, sheet = 2, skip = 6, col_names = FALSE)
  gv <- gv[, c(2, 3, 7, 8, 9)]
  colnames(gv) <- c("lvl", paste0("ars_", year-1), "area", "pop", paste0("ars_", year))
  gv$area <- as.numeric(gv$area)
  gv$pop <- as.numeric(gv$pop)
  # split aggregate levels and merge to gemeinde level
  gv <- gv %>% filter(lvl == "Gemeinde") %>% select(-c(lvl))
  # generate weights
}

# DOES 2022 make sense? End of 2022 ok; but datawise?

year <- 2022
gvchange <- process_gvchange(year)
gvchange <- gvchange %>% 
  mutate(area_tot = sum(area), .by=c(paste0("ars_", year))) %>%
  mutate(pop_tot = sum(pop), .by=c(paste0("ars_", year))) %>%
  mutate(area_w = area/area_tot) %>%
  mutate(pop_w = pop/pop_tot) %>%
  select(starts_with("ars_") | ends_with("_w"))
gvchange[is.na(gvchange$area_w), "area_w"] <- 1 # fully dissolved
gvchange[is.na(gvchange$pop_w), "pop_w"] <- 1 # fully dissolved
tibble(gvchange)

# THIS IS INCORRECT
# we need to create weights by (1) area and (2) pop to allow for the xwalk
# th number of changes is super low though! -> should not make any real difference I reckon
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

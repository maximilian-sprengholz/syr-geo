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


### STEP 1: GV - ADMINISTRATIVE CHANGES 2020 - 2022 ################################################

# INKAR correspondence tables refer to changes BETWEEN years y1 and y2 at 31.12.
# = equivalent to GV change tables WITHIN y2
# There seems to be some threshold to which changes are considered for INKAR:
# very small changes or simultaneous and counteracting changes are omitted
# (e.g. 20% of gem1 go to gem2 and approx. vice versa)

### function creating ars correspondence table per year
process_gvchange <- function(year) {
  # set in and out ars x year for clarity
  year_in <- year - 1
  year_out <- year 
  ars_in <- paste0("gem_ars_full_", year_in)
  ars_out <- paste0("gem_ars_full_", year_out)
  # read and name
  fpath <- paste0(data, "/external/raw/Destatis/GV/3112", year_out, "_Aenderungen_GV.xlsx")
  gv <- read_excel(fpath, sheet = 2, skip = 4, col_names = FALSE)
  gv <- gv[, c(2, 3, 6, 7, 8, 9)]
  colnames(gv) <- c("lvl", ars_in, "type", "area", "pop", ars_out)
  gv$area <- as.numeric(gv$area)
  gv$pop <- as.numeric(gv$pop)
  # look only at changes for GEM; omit namechanges (these will be available from the GV per year)
  gv <- gv %>% filter(lvl == "Gemeinde" & type != "4") %>% select(-c(lvl, type))
  # generate weights
  gv <- gv %>% 
    mutate(area_w = area / sum(area, na.rm = TRUE), .by = ars_in) %>%
    mutate(pop_w = pop / sum(pop, na.rm = TRUE), .by = ars_in) %>%
    select(!c(area, pop)) # changes only, get area for all units from base ars file
  # merge to base ars
  df_ars <- read_csv(paste0(data, "/external/processed/ars/ars", year_in, ".csv"))
  df_ars <- df_ars %>% 
    select(gem_ars_full, gem_ars, gem_name, area, pop) %>%
    rename_with(~ paste0(.x, "_", year_in), starts_with("gem_"))
  df_ars <- merge(df_ars, gv, by = ars_in, all.x = TRUE)
  # default values: no change to ars or fully dissolved in new entity
  df_ars[is.na(df_ars[[ars_out]]), ars_out] <- df_ars[is.na(df_ars[[ars_out]]), ars_in]
  df_ars[is.na(df_ars$area_w), "area_w"] <- 1
  df_ars[is.na(df_ars$pop_w), "pop_w"] <- 1
  # return
  return(df_ars %>% rename_with(~ paste0(.x, "_", year_in), ends_with("_w")))
}

### do for 2021 and 2022, merge together
df_gem <- merge(
  process_gvchange(2021), 
  process_gvchange(2022) %>% select(!c(area, pop)), 
  by = "gem_ars_full_2021", 
  all.x = TRUE,
  all.y = TRUE
  )
# multiply weights
df_gem$area_w <- df_gem$area_w_2020 * df_gem$area_w_2021
df_gem$pop_w <- df_gem$pop_w_2020 * df_gem$pop_w_2021
# keep relevant columns and save
df_gem <- df_gem %>% 
  select(gem_ars_full_2020, gem_ars_2020, gem_name_2020, area, pop, area_w, pop_w, gem_ars_full_2022)
# most changes are negligible (direct assignment would be ok)
print(df_gem %>% filter(area_w < 1 & area_w > 0), n = 300, na.print = "")
# write
write_delim(df_gem, paste0(data, "/external/processed/ars/xwalk_gem_2020_2022.csv"), delim = ";")

### aggregate for GVB and KRE
df_gvb <- df_gem %>%
  mutate(gvb_ars_2020 = substr(gem_ars_full_2020, 1, 9)) %>%
  mutate(gvb_ars_2022 = substr(gem_ars_full_2022, 1, 9)) %>%
  add_count(gem_ars_full_2020, name = "dupn") %>%
  mutate(
    area_tot = sum(area / dupn, na.rm = TRUE),
    pop_tot = sum(pop / dupn, na.rm = TRUE),
    .by = c(gvb_ars_2020)
    ) %>%
  mutate(
    area_w = sum(area * area_w, na.rm = TRUE) / area_tot,
    pop_w = sum(pop * pop_w, na.rm = TRUE) / pop_tot,
    .by = c(gvb_ars_2020, gvb_ars_2022)
    ) %>%
  distinct(gvb_ars_2020, gvb_ars_2022, .keep_all = TRUE) %>%
  select(starts_with("gvb_"), area_w, pop_w)
df_gvb[is.na(df_gvb$area_w), "area_w"] <- 1
df_gvb[is.na(df_gvb$pop_w), "pop_w"] <- 1
print(tibble(df_gvb %>% filter(area_w != 1)), n = 50)
# write
write_delim(df_gvb, paste0(data, "/external/processed/ars/xwalk_gvb_2020_2022.csv"), delim = ";")

df_kre <- df_gem %>%
  mutate(kre_ars_2020 = substr(gem_ars_full_2020, 1, 5)) %>%
  mutate(kre_ars_2022 = substr(gem_ars_full_2022, 1, 5)) %>%
  add_count(gem_ars_full_2020, name = "dupn") %>%
  mutate(
    area_tot = sum(area / dupn, na.rm = TRUE),
    pop_tot = sum(pop / dupn, na.rm = TRUE),
    .by = c(kre_ars_2020)
    ) %>%
  mutate(
    area_w = sum(area * area_w, na.rm = TRUE) / area_tot,
    pop_w = sum(pop * pop_w, na.rm = TRUE) / pop_tot,
    .by = c(kre_ars_2020, kre_ars_2022)
    ) %>%
  distinct(kre_ars_2020, kre_ars_2022, .keep_all = TRUE) %>%
  select(starts_with("kre_"), area_w, pop_w)
df_kre[is.na(df_kre$area_w), "area_w"] <- 1
df_kre[is.na(df_kre$pop_w), "pop_w"] <- 1
print(tibble(df_kre %>% filter(area_w != 1)), n = 50)
print(tibble(df_kre %>% filter(kre_ars_2020 == "16056")), n = 50) # check: eisenach -> erfurt
# write
write_delim(df_kre, paste0(data, "/external/processed/ars/xwalk_kre_2020_2022.csv"), delim = ";")


### STEP 2: XWALK GEO ENTITIES - WOHNGEBIET / PLZ -> GEM/GVB/KRE ###################################

# where to get the plz info? ideally 2022 areas with pop
# wohngebiet area: use intersections as weights

### Shapefiles German administrative units #########################################################
# https://daten.gdz.bkg.bund.de/produkte/vg/vg5000_1231/aktuell/vg5000_12-31.utm32s.shape.ebenen.zip
# RBZ seem to be incomplete
# historically available since 2020: possible to use for xwalk if area is not noted somewhere else
p <- vect(paste0(data, "/external/raw/Shapefiles/vg/vg5000_ebenen_1231/VG5000_KRS.shp"))


### merge plz (info from 31.12.2021, accessed Feb 28, 2023; last updated Feb 2022)
# https://downloads.suche-postleitzahl.org/v2/public/zuordnung_plz_ort.csv
# df_ags_plz <- read_csv(paste0(data, "/external/raw/OSM/zuordnung_plz_ort.csv"))
# dfs_gv <- lapply(dfs_gv, function(df_gv, df_ags_plz) {
#     # create ags for merging (=ars without gemeindevarband code in between)
#     if (df_gv$year[1] == 2022) df_gv <- rename(df_gv, gem_ars_2022 = gem_ars)
#     df_gv <- df_gv %>% mutate(ags = paste0(substr(gem_ars_2022,1,5), substr(gem_ars_2022,10,12)))
#     # merge
#     df_gv <- merge(df_gv, df_ags_plz %>% select(ags, plz), by="ags", all.x = TRUE, all.y = FALSE)
#     # rename again
#     if (df_gv$year[1] == 2022) df_gv <- rename(df_gv, gem_ars = gem_ars_2022)
#     # drop ags
#     df_gv <- df_gv %>% select(-c(ags))
#     },
#     df_ags_plz
#   )
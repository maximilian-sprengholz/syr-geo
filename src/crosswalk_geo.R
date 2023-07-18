#
# This file performs geo crosswalks:
# - between different geo units for 2022
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


### XWALK GEO ENTITIES - WOHNGEBIET / PLZ -> GEM/GVB/KRE ###########################################

# Final result is a correspondence table, linking all persons in the SYR data via their
# - wohngebiet (via pid)
# - postcode
# to the administrative units for which we have data:
# - GEM
# - GVB
# - KRE
# Table characteristics:
# - one table per geo in x geo out (= 6 tables)
# - multiple lines per person whenever assignment is not unambiguous
# - area weights and (pop weights? would be postcode only and with data from 2011 census)

# 
# geo1_id  geo1_area  geo2_id  geo2_area  geo_is_area -> geo_is_weight -> geo2_weight
# 

# df <- readRDS(file = paste0(data, "/syr.rds"))
# postcodes <- unique(df$postcode) # 5448

### info by ars from Destatis GV
df_ars <- read_csv(paste0(data, "/external/processed/ars/ars2022.csv"))

### wohngebiete
p_wohngebiet <- readRDS(file = paste0(data, "/syr_wohngebiet.rds"))
p_wohngebiet <- vect(p_wohngebiet[!st_is_empty(p_wohngebiet), c("pid", "response_multisub_rank")])

### Shapefiles PLZ (info from 31.12.2021, accessed Feb 28, 2023; last updated Feb 2022)
# https://downloads.suche-postleitzahl.org/v2/public/plz-5stellig.shp.zip
# somehow the area size in the shape files slightly deviates from the values terra calculates
# for consistency reasons we replace the data (which is merged later) with the terra values
p_postcode <- vect(paste0(data, "/external/raw/Shapefiles/plz/plz-5stellig.shp"))
p_postcode <- p_postcode[c("plz")]

### Shapefiles German administrative units (RBZ seem to be incomplete)
# https://daten.gdz.bkg.bund.de/produkte/vg/vg5000_1231/aktuell/vg5000_12-31.utm32s.shape.ebenen.zip
# needs re-projection to CRS:4326, which wohngebiete, google data, and postcodes have

p_gem <- project(
  vect(paste0(data, "/external/raw/Shapefiles/vg/vg5000_ebenen_1231/VG5000_GEM.shp")),
  "epsg:4326" 
  )
p_gvb <- project(
  vect(paste0(data, "/external/raw/Shapefiles/vg/vg5000_ebenen_1231/VG5000_VWG.shp")),
  "epsg:4326" 
  )
p_kre <- project(
  vect(paste0(data, "/external/raw/Shapefiles/vg/vg5000_ebenen_1231/VG5000_KRS.shp")),
  "epsg:4326" 
  )

### estimate intersection area (increadibly easy to do with terra, the long output is perfect)
# there are probably some inaccuracies in the postcode shapes which lead to non-perfect alignment
# I guess it is ok to just keep those intersections contributing at least 1 % of the postcode area

geo_map <- list(
  geos_in = list(
    gem = p_gem,
    gvb = p_gvb,
    kre = p_kre
    ),
  geos_out = list(
    wohngebiet = p_wohngebiet,
    postcode = p_postcode
    )
)

for (geo_out in names(geo_map$geos_out)) {
  
  # define output, get area
  p_out <- geo_map$geos_out[[geo_out]]
  p_out$area_out <- expanse(p_out)/1000000 # km2

  # groupcols
  if (geo_out == "postcode") groupcols <- names(p_out)[1] else groupcols <- names(p_out)[1:2]
  
  # loop over input levels
  for (geo_in in names(geo_map$geos_in)) {

    # define input, subset to ars, get area
    p_in <- geo_map$geos_in[[geo_in]]
    p_in <- p_in["ARS"]
    p_in$area_in <- expanse(p_in)/1000000 # km2

    # intersect
    p <- intersect(p_in, p_out)
    p$area_shared <- expanse(p)/1000000 # km2
    p <- tibble(as.data.frame(p))
    
    # weights
    # omit all overlaps of < 1 % of out entity to keep everything simple 
    # (might introduce some inaccuracy)
    p$area_w_abs <- p$area_shared / p$area_in # absolute from in entity
    p$area_w_rel <- p$area_shared / p$area_out # relative to out entity (%)
    p <- p %>% 
      mutate(toosmall = ifelse(area_w_rel < 0.01, TRUE, FALSE)) %>%
      mutate(area_upscale = area_out / sum(area_shared), .by = all_of(c(groupcols, "toosmall"))) %>%
      mutate(area_w_abs = (area_shared * area_upscale) / area_in) %>% 
      mutate(area_w_rel = (area_shared * area_upscale) / area_out) %>%
      filter(toosmall == FALSE) %>%
      select(!c(toosmall, area_upscale))
    
    # label and order
    if (geo_out == "wohngebiet") idcols <- groupcols else idcols <- geo_out
    colnames(p) <- c(
      paste0(geo_in, "_ars"), paste0(geo_in, "_area"), idcols, paste0(geo_out, "_area"),
      "area_shared", "area_w_abs", "area_w_rel", "toosmall", "area_upscale"
      )
    p <- p %>% arrange(all_of(c(idcols, paste0(geo_in, "_ars"))))

    # GEM specific
    if (geos_in == "gem") {
      p <- p %>% 
        rename(gem_ars_full = gem_ars) %>% 
        mutate(gem_ars = paste0(substr(gem_ars_full,1,5), substr(gem_ars_full,10,12)))
      }

    # write
    write_delim(
      p, 
      paste0(data, "/external/processed/ars/xwalk_", geo_in, "_", geo_out, ".csv"), 
      delim = ";"
      )

    }
  }


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
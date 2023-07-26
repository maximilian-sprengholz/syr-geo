#
# This file:
# - merges all context variables on all levels
# - on wohngebiet and postcode level by xwalks

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

# ### postcode
# p <- vect(paste0(data, "/external/raw/Shapefiles/plz/plz-5stellig.shp"))
# p$qkm <- expanse(p)/1000000

# # merge postcode area and inhabitant info to df for plausibility checks
# df_postcode <- data.frame(
#   postcode = p$plz,
#   postcode_area = p$qkm,
#   postcode_pop = p$einwohner
#   )
# df <- merge(df, df_postcode, by="postcode", all.x = TRUE, all.y = FALSE)
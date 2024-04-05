#
# This file processes various DWD geo-referenced data:
# - historical precipitation and temperature
# - precip/temp at time of interview (Nov/Dec 22)
#

### config (relative to working dir syr-geo!)
source("src/_config.R")

### packages

# conda managed
library(terra)
library(tibble)
library(dplyr)
library(sjlabelled)

### Shapefiles PLZ (info from 31.12.2021, accessed Feb 28, 2023; last updated Feb 2022)
# https://downloads.suche-postleitzahl.org/v2/public/plz-5stellig.shp.zip
p_postcode <- vect(paste0(data, "/external/raw/Shapefiles/plz/plz-5stellig.shp"))
p_postcode <- p_postcode[c("plz")]
names(p_postcode) <- c("postcode")
c_postcode <- centroids(p_postcode, inside = TRUE) # centroids

### stack month x indicator; per year
### indicators: mean, min, max, days low values, days high values (low/high needs def.!)
agg_month <- function (x, r) {
  m <- tapp(r, "month", match.fun(x), na.rm = TRUE) # estimate monthly value
  }

# precipitation
stub <- "precip"
path_in <- paste0(data, "/external/raw/DWD/Precipitation_Daily/")
path_out <- paste0(data, "/external/processed/DWD/", stub, "/historical_m")
d_low <- function(x, na.rm) { sum(x < 0.1, na.rm = na.rm) } # no rain
d_high <- function(x, na.rm) { sum(x > 25, na.rm = na.rm) } # Unwetterwarnung DWD
indicators <- c("mean", "min", "max", "d_low", "d_high")
for (year in 1990:1990) {
  # read yearly file
  r <- rast(paste0(path_in, "/pr_hyras_1_", year, "_v5-0_de.nc"))
  # calculate monthly values and stack
  stack <- sapply(indicators, agg_month, r, simplify = FALSE)
  names(stack) <- paste(stub, indicators, year, sep = "_")
  stack <- rast(stack)
  # save for grid
  writeRaster(
    stack, 
    file = paste0(path_out, "/", stub, "_", year, "_grid.tif"), 
    overwrite = TRUE
    )
  # save for postcode (which can be merged directly)
  sjlabelled::write_stata(
    as.data.frame(
      extract(stack, project(c_postcode, crs(stack)), ID = FALSE, bind = TRUE)
      ), 
    paste0(path_out, "/", stub, "_", year, "_postcode.dta"), 
    version=14
  )
}
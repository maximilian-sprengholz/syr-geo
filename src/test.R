#
# This file processes various DWD geo-referenced data:
# - historical pritation and temperature
# - pr/temp at time of interview (Nov/Dec 22)
#

### config (relative to working dir syr-geo!)
rm(list = ls())
source("src/_config.R")

### packages

# conda managed
library(tibble)
library(dplyr)
library(tidyr)
library(sjlabelled)
library(lubridate)
library(tictoc)
library(sf)
library(terra)
terra::gdalCache(8000) # 8 GB per chunk
library(ncdf4) # necessary for writing .nc
library(future.apply)
plan(multicore, workers = 2)
plan(sequential)

# CRAN only
if (!require(rdwd)) install.packages("rdwd")
library(rdwd)
if (!require(dwdradar)) install.packages("dwdradar")
library(dwdradar)
if (!require(pbapply)) install.packages("pbapply")
library(pbapply)
if (!require(exactextractr)) install.packages("exactextractr")
library(exactextractr)

### Shapefiles PLZ (info from 31.12.2021, accessed Feb 28, 2023; last updated Feb 2022)
# https://downloads.suche-postleitzahl.org/v2/public/plz-5stellig.shp.zip
p_postcode <- vect(paste0(data, "/external/raw/Shapefiles/plz/plz-5stellig.shp"))
p_postcode <- p_postcode[c("plz")]
names(p_postcode) <- c("postcode")
c_postcode <- centroids(p_postcode, inside = TRUE) # centroids

### functions ######################################################################################

### survey time 28.10.2022 - 09.12.2022 (5 minute intervals -> no aggregation, just passthrough)

# custom DWD projection: needs to be converted with proper crs
crs <- "PROJCS[\"Radolan projection\",
    GEOGCS[\"Radolan Coordinate System\",
        DATUM[\"Radolan Kugel\",
            SPHEROID[\"Erdkugel\",6370040.0,0.0,
            LENGTHUNIT[\"metre\",1]]],
        PRIMEM[\"Greenwich\",0,
            ANGLEUNIT[\"Degree\",0.017453292519943295]]],
    CONVERSION[\"North_Pole_Stereographic\",
        METHOD[\"Polar Stereographic (variant A)\",
            ID[\"EPSG\",9810]],
        PARAMETER[\"Latitude of natural origin\",90,
            ANGLEUNIT[\"Degree\",0.017453292519943295],
            ID[\"EPSG\",8801]],
        PARAMETER[\"Longitude of natural origin\",10,
            ANGLEUNIT[\"Degree\",0.017453292519943295],
            ID[\"EPSG\",8802]],
        PARAMETER[\"Scale factor at natural origin\",1,
            SCALEUNIT[\"unity\",0.9330127019],
            ID[\"EPSG\",8805]],
        PARAMETER[\"False easting\",0,
            LENGTHUNIT[\"metre\",1],
            ID[\"EPSG\",8806]],
        PARAMETER[\"False northing\",0,
            LENGTHUNIT[\"metre\",1],
            ID[\"EPSG\",8807]]],
        PARAMETER[\"Latitude of standard parallel\",60,
            ANGLEUNIT[\"Degree\",0.017453292519943295],
            ID[\"EPSG\",8832]],
    CS[Cartesian,2],
        AXIS[\"(E)\",south,
            ANGLEUNIT[\"degree\",0.017453292519943295],
            ORDER[1],
            LENGTHUNIT[\"metre\",1]],
        AXIS[\"(N)\",south,
            MERIDIAN[180,
                ANGLEUNIT[\"degree\",0.017453292519943295]],
            ORDER[2],
            LENGTHUNIT[\"metre\",1]]
    ]"

# define output units and corresponding files
files <- list.files(paste0(data, "/external/raw/DWD/pr_sum_5min/2022"))
files <- files[yday(as.Date("2022-10-28")):yday(as.Date("2022-12-09"))]
yf <- data.frame(
  unit = rep("all", length(files)),
  input = paste0(paste0(data, "/external/raw/DWD/pr_sum_5min/2022/"), files)
)
# time comp extract terra vs. exactextractr
t <- rast(sapply(yf$input, rast))
t <- c(rast(yf[1,2]), rast(yf[1,2]), rast(yf[1,2]), rast(yf[1,2]), rast(yf[1,2]), rast(yf[1,2]), rast(yf[1,2]))
names(t) <- paste0("name_", 1:nlyr(t))
crs(t) <- crs
sf <- sf::st_as_sf(project(p_postcode, crs(t)))
values <- data.frame(postcode = sf[[1]])
tic()
chunksize = 200
if (nlyr(t) > chunksize) {
  set <- list()
  chunks <- ceiling(nlyr(t)/chunksize)
  for (i in 1:chunks) {
    lo <- (i-1) * chunksize + 1
    if (i < chunks) hi <- i * chunksize else hi <- nlyr(t)
    set[[i]] <- wrap(t[[lo:hi]])
  }
} else {
  set <- list(wrap(t))
}
values <- data.frame(
  values,
  future_sapply(
    set,
    function(r, y, fun, force_df, max_cells_in_memory) {
      exactextractr::exact_extract(
        unwrap(r),
        y = y,
        fun = fun,
        force_df = force_df,
        max_cells_in_memory = max_cells_in_memory
      )
    },
    y = sf,
    fun = "mean",
    force_df = TRUE,
    max_cells_in_memory = 9e+08,
    future.seed=TRUE
  )
)
toc()

# mem size
for (thing in ls()) { message(thing); print(object.size(get(thing)), units='auto') }

plot(t1[[1]])
plot(project(p_postcode, crs(t1)), add = TRUE, alpha = 0.5)

7570103904
176048928
100000000

11975040000
285120000


a <- list(data.frame(a = 1:10, b = 11:20), data.frame(c = 1:10, d = 11:20))
b <- data.frame(a)
head(b)

library(datasets)
library(stats)
y <- future_lapply(mtcars, FUN = mean, trim = 0.10)
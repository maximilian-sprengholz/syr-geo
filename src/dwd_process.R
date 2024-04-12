#
# This file processes various DWD geo-referenced data:
# - historical precipitation and temperature
# - precip/temp at time of interview (Nov/Dec 22)
#

### config (relative to working dir syr-geo!)
rm(list = ls())
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


### Monthly values per year: historical_m ##########################################################
### Daily values survey period: survey_d ###########################################################
# aggregation function
sragg <- function (file, aggfun, aggby) {
  # read
  r <- rast(file)
  # aggregate (just reads the file if nothing is specified)
  if (!is.null(aggfun)) {
    if (is.null(aggby)) {
      app(r, match.fun(aggfun), na.rm = TRUE) # aggregate per cell
    } else {
      tapp(r, aggby, match.fun(aggfun), na.rm = TRUE) # aggregate per cell x time (e.g. month)
    }
  }
}

# stacking function
srstack <- function(aggfun, files, aggby) {
  # aggregate each SpatRaster
  stack <- sapply(files, sragg, aggfun, aggby, simplify = FALSE)
  # names(stack) <- paste(funs, unit, sep = "_")
  # stack in one SpatRaster
  stack <- rast(stack)
}

# processing function
srproc <- function(
    df, # dataframe of units to input by (e.g. years) and corresponding files
    path = NULL, # out dir
    stub = NULL, # stub prepended to name
    aggfuns = c("mean", "min", "max"), # vector of function names (strings) to be estimated from data
    aggby = NULL, # passed on to terra::tap (one of the named time units, e.g. "month", or index)
    extract = list() # named list of point spatVectors for which to extract values (e.g. postcode centroids)
    ) {
  stopifnot(length(df) == 2)
  if (length(extract) > 0) stopifnot(!is.null(names(extract)))
  stopifnot(length(aggby) <= 1)
  # loop over output units (determines the aggregation level and what is in a single saved file)
  for (unit in unique(df[,1])) {
    # input files passed by user
    infiles <- df[,2]
    # loop over aggegration functions, which form the sds subsets and can be accessed by name
    # creates list of aggregated SpatRasters, then stack to SpatRasterDataset if multiple agg. stats
    stack <- sapply(aggfuns, srstack, files = infiles, aggby = aggby)
    if (length(stack) > 1) stack <- sds(stack)
    # write out?
    if (!is.null(path)) {
      # save for grid
      outfile <- paste0(path, "/", paste(stub, unit, "grid", sep = "_"), ".tif")
      writeRaster(stack, file = file, overwrite = TRUE)
      log <- data.frame(unit, outfile)
      # extract and save value for particular geometries
      for (geo in names(extract)) {
        outfile <- paste0(path, "/", paste(stub, unit, geo, sep = "_"), ".dta")
        sjlabelled::write_stata(
          as.data.frame(extract(stack, project(extract[[geo]], crs(stack)), ID = FALSE, bind = TRUE)), 
          outfile, 
          version=14
        )
        log <- rbind(log, data.frame(unit, outfile))
      }
    }
  }
  if (!is.null(path)) {
    colnames(log) <- c(colnames(df)[[1]], "output")
    return(log)
  } else {
    return(stack)
  }
}

yf <- data.frame(year = 1990:1990)
yf$input <- paste0(
  data, "/external/raw/DWD/Precipitation_Daily/pr_hyras_1_", yf$year, "_v5-0_de.nc"
  )
t <- srproc(
  yf,
  aggfuns = c("mean", "min"),
  aggby = "month"
  )
t["mean"]
writeCDF(t, file = "/home/max/Desktop/test.nc", overwrite = TRUE)

# look at contents
# how would you specify for SDSs which layer has to be used by which aggregation function?
# if names are the same, then this should be easy - but it would involve slicing and stacking
# should alread work if the split is implemented in sragg?!

# check if reading works in the same way for SDS! then just condition!


# precipitation
d_low <- function(x, na.rm) { sum(x < 0.1, na.rm = na.rm) } # no rain
d_high1 <- function(x, na.rm) { sum(x >= 50, na.rm = na.rm) } # DWD Ergbiebiger Dauerregen
d_high2 <- function(x, na.rm) { sum(x > 80, na.rm = na.rm) } # DWD extrem ergbiebiger Dauerregen
yf <- data.frame(year = 1990:1990)
yf$input <- paste0(
  data, "/external/raw/DWD/Precipitation_Daily/pr_hyras_1_", yflist$year, "_v5-0_de.nc"
  )
agg(
  yf,
  path = paste0(data, "/external/processed/DWD/precip/historical_m"),
  stub = "precip",
  funs = c("mean", "min", "max", "d_low", "d_high1", "d_high2"),
  agg = "month",
  extract = list(postcode = c_postcode)
  )

# temperature
d_low <- function(x, na.rm) { sum(x < -10, na.rm = na.rm) } # selbstdefiniert: < -10 Grad
d_high1 <- function(x, na.rm) { sum(x > 32, na.rm = na.rm) } # DWD starke Wärmebelastung
d_high2 <- function(x, na.rm) { sum(x > 38, na.rm = na.rm) } # DWD extreme Wärmebelastung

### Values at survey time ##########################################################
# processing function: rolling mean
daily <- function(
      df, # dataframe of units to input by (e.g. years) and corresponding files
      path, # out dir
      stub, # stub prepended to name
      funs = c("mean", "min", "max"), # vector of function names (strings) to be estimated from data
      agg = "month",
      extract = list() # named list of point spatVectors for which to extract values (e.g. postcode centroids)
    ) {
  


  }



r <- rast("/home/max/Seafile/FoDiRa-SYR/survey/data/external/raw/DWD/Temperature_Hourly/tas_1hr_HOSTRADA-v1-0_BE_gn_2022120100-2022123123.nc")
system.time(app(r, mean, na.rm = TRUE))
r <- c(r,r)
system.time(app(r, mean, na.rm = TRUE))


r <- rast("/home/max/Seafile/FoDiRa-SYR/survey/data/external/raw/DWD/Temperature_Hourly/tas_1hr_HOSTRADA-v1-0_BE_gn_2022120100-2022123123.nc")
r <- r[[1:72]]
rmin <- tapp(r, "day", "min", na.rm = TRUE)
rmax <- tapp(r, "day", "max", na.rm = TRUE)
stack1 <- sds(rmin, rmax)
names(stack1) <- c("min", "max")

r <- rast("/home/max/Seafile/FoDiRa-SYR/survey/data/external/raw/DWD/Temperature_Hourly/tas_1hr_HOSTRADA-v1-0_BE_gn_2022120100-2022123123.nc")
r <- r[[73:144]]
rmin <- tapp(r, "day", "min", na.rm = TRUE)
rmax <- tapp(r, "day", "max", na.rm = TRUE)
stack2 <- sds(rmin, rmax)
names(stack2) <- c("min", "max")

sds <- sds(c(stack1["min"], stack2["min"]), c(stack1["max"], stack2["max"]))

tibble(as.data.frame(extract(stack["min"], project(c_postcode, crs(r)), ID = FALSE, bind = TRUE)))

# list of extracted dataframes: per statistic/fun
t <- sapply(
  names(stack),
  function(x) {
    as.data.frame(extract(stack[x], project(c_postcode, crs(r)), ID = FALSE, bind = TRUE))
    },
  simplify = FALSE
  )

# build wide df with suffix
df <- data.frame(postcode = c_postcode$postcode)
for (x in names(t)) {
  df <- merge(df, t[[x]], by="postcode")
}

t <- as.data.frame(extract(stack, project(c_postcode, crs(r)), ID = FALSE, bind = TRUE))
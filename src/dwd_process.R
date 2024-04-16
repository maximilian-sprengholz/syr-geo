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
library(ncdf4) # necessary for writing .nc
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

### functions

# aggregation function:
# 1. reads in a raster file
# 2. aggregates using passed vector of c(aggfunction, subset)
# 3. aggregates by passed index (either integer or one of the predefined time values, e.g. "month")
sragg <- function (file, aggfun, aggby) {
  # read
  r <- rast(file)
  # aggregate (just reads the file if nothing is specified)
  if (!is.null(aggfun)) {
    # allow direct passing of functions
    if (typeof(aggfun) %in% c("closure", "builtin")) aggfun <- list(aggfun)
    stopifnot(is.function(aggfun[[1]]))
    # subset if requested
    if (length(aggfun) > 1) {
      stopifnot(length(aggfun) == 2)
      stopifnot(aggfun[[2]] %in% varnames(r))
      r <- r[aggfun[[2]]]
    }
    # aggregate 
    if (is.null(aggby)) {
      app(r, aggfun[[1]], na.rm = TRUE) # aggregate per cell
    } else {
      tapp(r, aggby, aggfun[[1]], na.rm = TRUE) # aggregate per cell x time (e.g. month)
    }
  }
}

# stacking function: creates a new SpatRaster combining the layers of each aggregated SpatRaster
srstack <- function(aggfun, files, aggby) {
  # aggregate each SpatRaster
  stack <- sapply(files, sragg, aggfun, aggby, simplify = FALSE, USE.NAMES = FALSE)
  # stack in one SpatRaster; assign temporary, consecutively numbered names
  # naming is consistent if aggr. units are unique per output unit (e.g. months in a year;
  # aggregating over multiple years would have the second january suffixed with _13, and so on)
  # will be overwritten by aggregation function name in srproc
  stack <- rast(stack)
  if (!is.null(aggby)) names(stack) <- paste0(aggby, "_", 1:length(names(stack)))
  return(stack)
}

# processing function
# - Creates a SDS with a layer for each aggregation function (indicator)
#   - Each layer is an aggregation according to the passed df: unit x files
#   - [ Saves one aggregated SpatRaster per unit ]
#   - [ Saves data extracted from aggr. SpatRaster for list of output geoms (e.g. postcode centr.) ]
srproc <- function(
    df, # dataframe of units to input by (e.g. years) and corresponding files
    aggfuns = NULL, # list of aggregation functions and raster subset the functions are applied to
    aggby = NULL, # passed on to terra::tap (one of the named time units, e.g. "month", or index)
    extract = list(), # named list of spatVectors for which to extract values (e.g. postcode centr.)
    path = NULL, # out dir
    stub = NULL # stub prepended to name of variables / output files
    ) {
  stopifnot(length(df) == 2) # two columns: input unit and corresponding files
  stopifnot(length(aggby) <= 1) # no or single aggregation function
  if (length(unique(df[, 1])) > 1) stopifnot(!is.null(path)) # direct returns only for single unit
  if (length(extract) > 0) stopifnot(!is.null(names(extract))) # only names inputs
  # log
  log <- data.frame(matrix(ncol = 5, nrow = 0))
  # loop over output units (determines the aggregation level and what is in a single saved file)
  for (unit in unique(df[, 1])) {
    # input files passed by user
    infiles <- df[df[[1]] == unit, 2]
    # loop over aggegration functions, which form the sds subsets and can be accessed by name
    # creates list of aggregated SpatRasters, then stack to SpatRasterDataset
    # name stack by aggregation function names
    stack <- sapply(aggfuns, srstack, files = infiles, aggby = aggby)
    for (n in names(stack)) {
      names(stack[[n]]) <- gsub("^\\D+", paste0(n, "_"), names(stack[[n]]))
      if (!is.null(stub)) names(stack[[n]]) <- paste(stub, names(stack[[n]]), sep = "_")
    }
    stack <- sds(stack)
    # save
    if (!is.null(path)) {
      # save for grid
      outfile <- paste0(
        path, "/", ifelse(is.null(stub), paste(unit), paste(stub, unit, sep = "_")), "_grid.nc"
        )
      writeCDF(stack, file = outfile, overwrite = TRUE)
      log <- rbind(
        log, 
        data.frame(unit, outfile, geo = "grid", paste(varnames(stack), sep = ", "), aggby)
        )
      # extract and save value for particular geometries as Stata file
      for (geo in names(extract)) {
        outfile <- paste0(
          path, "/", 
          ifelse(is.null(stub), paste(unit, geo, sep = "_"), paste(stub, unit, geo, sep = "_")),
          ".dta"
        )
        values <- extract(stack, project(extract[[geo]], crs(stack)), ID = FALSE, bind = TRUE)
        sjlabelled::write_stata(as.data.frame(values), outfile, version = 14)
        log <- rbind(log, data.frame(unit, outfile, geo, paste(varnames(stack), sep = ", "), aggby))
      }
    }
  }
  # return processing log; or sds object if single unit and no path
  if (!is.null(path)) {
    colnames(log) <- c(colnames(df)[[1]], "file", "geo", "agg", "aggby")
    return(log)
  } else {
    return(stack) 
  }
}


### precipitation

# define output units and corresponding files
yf <- data.frame(year = 1995:1996)
yf$input <- paste0(
  data, "/external/raw/DWD/precip_sum_daily/pr_hyras_1_", yf$year, "_v5-0_de.nc"
  )
# specify aggregation functions
aggfuns <- list(
  mean = mean,
  min = min,
  max = max,
  d_low = function(x, na.rm) { sum(x < 0.1, na.rm = na.rm) }, # no rain,
  d_high1 = function(x, na.rm) { sum(x >= 50, na.rm = na.rm) }, # DWD Ergbiebiger Dauerregen
  d_high2 = function(x, na.rm) { sum(x > 80, na.rm = na.rm) } # DWD extrem ergbiebiger Dauerregen
)
# call processing function
log <- srproc(
  yf,
  aggfuns = aggfuns,
  aggby = "month",
  path = paste0(data, "/external/processed/DWD/precip/historical_monthly"),
  stub = "precip",
  extract = list(postcode = c_postcode)
  )
# write out cleaned log with added info
colnames(log) <- c("fileref", colnames(log)[2:length(colnames(log))])
log <- data.frame(
  indicator = "precip",
  scope = "historical",
  fileby = "year",
  log)
log$file <- gsub(data, "", log$file)
write.table(
  log,
  file = paste0(data, "/external/processed/DWD/log.csv"),
  sep = ";", 
  row.names = FALSE
  )


### temperature: first aggregate to daily values

# define output units and corresponding files
yf <- data.frame(input = list.files(paste0(data, "/external/raw/DWD/temp_mean_hourly")))
yf <- data.frame(year = gsub("(.+)(\\d{4})(\\d{6})(-.+)", "\\2", t), yf)
# specify aggregation functions
aggfuns <- list(
  mean = mean,
  min = min,
  max = max
)
# call processing function
log <- srproc(
  yf,
  aggfuns = aggfuns,
  aggby = "day",
  path = paste0(data, "/external/temp/DWD/temp/daily"),
  stub = "temp"
  )


### temperature: historical monthly values

# define output units and corresponding files
yf <- data.frame(year = 1995:2022)
yf$input <- paste0(
  data, "/external/temp/DWD/temp/daily/temp_", yf$year, "_grid.nc"
  )
# specify aggregation functions: with reference to variable names stacked in sds
aggfuns <- list(
  mean = list(mean, "mean"),
  min = list(min, "min"),
  max = list(max, "max"),
  d_low = list(
    function(x, na.rm) { sum(x < -10, na.rm = na.rm) }, # selbstdefiniert: < -10 Grad,
    "min"
  ),
  d_high1 = list(
    function(x, na.rm) { sum(x > 32, na.rm = na.rm) }, # DWD starke Wärmebelastung
    "max"
  ),
  d_high2 = list(
    function(x, na.rm) { sum(x > 38, na.rm = na.rm) }, # DWD extreme Wärmebelastung
    "max"
)
# call processing function
log <- srproc(
  yf,
  aggfuns = aggfuns,
  aggby = "month",
  path = paste0(data, "/external/processed/DWD/temp/historical_monthly"),
  stub = "temp"
  )




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

### testing area

r1 <- rast(yf[1,2])
names(r1) <- rep(paste0("testnames1_", 1:31), 12)[1:365]
r2 <- rast(yf[2,2])
names(r2) <- rep(paste0("testnames2_", 1:31), 12)[1:366]
t <- list(r1, r2)
names(t) <- c("lol", "lol")
t <- rast(t)
names(t) <- paste0("lol_", 1:length(names(t)))
tt <- as.data.frame(extract(t, project(c_postcode, crs(t)), ID = FALSE, bind = TRUE))



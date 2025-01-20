#
# This file processes various DWD geo-referenced data:
# - historical precipitation and temperature
# - pr/temp over 14 days up to (includign survey date)
# - pr/temp at time of interview
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
#library(lubridate)
library(zoo)
library(sf)
library(terra)
terra::gdalCache(8000) # 12 GB per chunk
library(ncdf4) # necessary for writing .nc

# CRAN only
if (!require(pbapply)) install.packages("pbapply")
library(pbapply)
if (!require(exactextractr)) install.packages("exactextractr")
library(exactextractr)

### Shapefiles PLZ (info from 31.12.2021, accessed Feb 28, 2023; last updated Feb 2022)
# https://downloads.suche-postleitzahl.org/v2/public/plz-5stellig.shp.zip
p_postcode <- vect(paste0(data, "/external/raw/Shapefiles/plz/plz-5stellig.shp"))
p_postcode <- p_postcode[c("plz")]
names(p_postcode) <- c("postcode")


### functions ######################################################################################

# aggregation function:
# 1. reads in a raster file
# 2. aggregates using passed vector of c(aggfunction, subset)
# 3. aggregates by passed index (either integer or one of the predefined time values, e.g. "month")
sragg <- function (file, aggfun, aggby, forcetime, cores) {
  # read
  r <- terra::rast(file)
  ### aggregation related checks and subsetting (rolling agg. extends period by 'window' ext(lo, hi))
  ext <- c(0, 0)
  if (!is.null(aggfun)) {
    # allow direct passing of functions
    if (typeof(aggfun) %in% c("closure", "builtin")) aggfun <- list(aggfun)
    stopifnot(is.function(aggfun[[1]]))
    # subset by aggregation layer if requested
    # only relevant for files which have been pre-aggregated (e.g. 10min -> days -> months)
    # and the 'days' file contains already, e.g., min/max values, which become monthly min/max
    if (length(aggfun) > 1) {
      stopifnot(length(aggfun) == 2)
      stopifnot(aggfun[[2]] %in% varnames(r))
      r <- r[aggfun[[2]]]
    }
    # estimate values for rolling 'window' needed for aggregation used in subsequent subsetting
    if (!is.null(forcetime) && !is.null(forcetime$period) && length(aggby) == 2) {
      if (aggby[[1]] == "to") ext <- c(aggby[[2]]-1, 0)
      if (aggby[[1]] == "around") ext <- c(ceiling((aggby[[2]]-1)/2), floor((aggby[[2]]-1)/2))
      if (aggby[[1]] == "from") ext <- c(0, aggby[[2]]-1)
    }
  }
  # force time format and zone; limiting to length(time(r)) ensures [start, end[ if output steps
  # more detailed than input time (e.g. ymd_h instead of ymd, and last datetime is then ymd 23:00)
  if (!is.null(forcetime)) {
    # datetime conversion
    if (!is.null(forcetime$step) && forcetime$step %in% c("years", "months", "days")) {
      terra::time(r) <- as.Date(terra::time(r), tz = forcetime$timezone)
      if (!is.null(forcetime$period)) {
        forcetime$period$start <- as.Date(forcetime$period$start, tz = forcetime$timezone)
        forcetime$period$end <- as.Date(forcetime$period$end, tz = forcetime$timezone)
      }
    } else {
      terra::time(r) <- as.POSIXct(terra::time(r), tz = forcetime$timezone)
      if (!is.null(forcetime$period)) {
        forcetime$period$start <- as.POSIXct(forcetime$period$start, tz = forcetime$timezone)
        forcetime$period$end <- as.POSIXct(forcetime$period$end, tz = forcetime$timezone)
      }
    }
    # force time steps
    if (!is.null(forcetime$step)) {
      terra::time(r) <- seq(
        from = terra::time(r)[1],
        to = terra::time(r)[length(terra::time(r))] + 1,
        by = forcetime$step
      )[1:length(terra::time(r))]
    }
    # subset by period
    if (!is.null(forcetime$period)) {
      r <- r[[terra::time(r) >= forcetime$period$start - ext[[1]] & terra::time(r) <= forcetime$period$end + ext[[2]]]]
    }
  }
  # aggregate 
  if (!is.null(aggfun)) {
    if (is.null(aggby)) {
      # per cell
      r <- terra::app(r, aggfun[[1]], na.rm = FALSE, cores = cores)
    } else if (length(aggby) == 1) {
      # per cell x time (e.g. month)
      r <- terra::tapp(r, aggby, aggfun[[1]], na.rm = FALSE, cores = cores)
    } else {
      # per cell x moving n time units, e.g. 14 days up to certain date
      time <- terra::time(r)
      r <- terra::roll(r, aggby[[2]], aggfun[[1]], aggby[[1]], na.rm = FALSE)
      terra::time(r) <- time # timestamps are lost when using custom functions...
      # trim rolling overhangs from period kept for aggregation
      if (!is.null(forcetime$period)) {
        r <- r[[terra::time(r) >= forcetime$period$start & terra::time(r) <= forcetime$period$end]]
      }
    }
  }
  # return
  return(r)
}

# stacking function: creates a new SpatRaster combining the layers of each aggregated SpatRaster
srstack <- function(aggfun, files, crs, aggby, forcetime, cores) {
  # aggregate each SpatRaster
  stack <- lapply(
    files,
    sragg,
    aggfun,
    aggby,
    forcetime,
    cores
  )
  # stack in one SpatRaster; assign temporary, consecutively numbered names
  # naming is consistent if aggr. units are unique per output unit (e.g. months in a year;
  # aggregating over multiple years would have the second january suffixed with 13, and so on)
  # will be overwritten by aggregation function name in srproc
  stack <- terra::rast(stack)
  if (!is.null(aggby)) {
    names(stack) <- paste(paste0(aggby, collapse = ""), 1:length(names(stack)), sep = "_")
  }
  # set custom crs if requested
  if (!is.null(crs)) crs(stack) <- crs
  # return
  return(stack)
}

# processing function
# - Creates a SDS with a layer for each aggregation function (indicator)
#   - Each layer is an aggregation according to the passed df: unit x files
#   - [ Saves one aggregated SpatRaster per unit ]
#   - [ Saves data extracted from aggr. SpatRaster for list of output geoms (e.g. postcode centr.) ]
srproc <- function(
    df, # dataframe of units to input by (e.g. years) and corresponding files
    crs = NULL, # set passed crs for opened file (usually not necessary, but depends on file, e.g. pr 5min)
    aggfuns = list(NULL), # list of aggregation functions and raster subset the functions are applied to
    aggby = NULL, # passed on to terra::tap (one of the named time units, e.g. "month", or index)
    forcetime = NULL, # list, e.g.: step = "days" (hours, ...), timezone = "CET", and period = list(start = Date1, end = Date2)
    path = NULL, # out dir
    cdf = FALSE, # write cdf?
    extract = list(), # named list of sf objects for which to extract values (e.g. postcode polygons)
    chunksize = 200, # number of layers processed by extract sequentially until all layers processed
    timelong = FALSE, # reshape extracted data frame?
    stub = NULL, # stub prepended to name of variables / output files
    cores = NULL
    ) {
  # two columns: input unit and corresponding files
  stopifnot(length(df) == 2 | nrow(df) == length(unique(df[[2]])))
  # no/single aggregation level; but allow for rolling n parameter
  if (length(aggby) > 1) stopifnot(length(aggby) == 2 & aggby[[1]] %in% c("around", "from", "to"))
  # direct returns only for single unit
  if (length(unique(df[, 1])) > 1) stopifnot(!is.null(path))
  # check if forcetime is list and has timezone set
  if (!is.null(forcetime)) {
    stopifnot(is.list(forcetime))
    stopifnot(!is.null(forcetime$timezone))
  }
  # only named inputs
  if (length(extract) > 0) stopifnot(!is.null(names(extract)))
  # log
  log <- data.frame(matrix(ncol = 5, nrow = 0))
  # loop over output units (determines the aggregation level and what is in a single saved file)
  i <- 0
  for (unit in unique(df[[1]])) {
    i <- i + 1
    print(paste0("Processing output '", unit, "' (", i, "/", length(unit), " output files)."))
    # input files passed by user
    infiles <- df[df[[1]] == unit, 2]
    # loop over aggegration functions, which form the sds subsets and can be accessed by name
    # creates list of aggregated SpatRasters, then stack to SpatRasterDataset
    # name stack by aggregation function names
    # if no aggfun is specified, the default is just a passthru with no agg
    print("Aggregating/stacking...")
    stack <- pblapply(
      aggfuns,
      srstack,
      files = infiles,
      crs = crs,
      aggby = aggby,
      forcetime = forcetime,
      cores = cores
    )
    for (i in 1:length(stack)) {
      if (is.null(aggfuns[[1]])) {
        # default names irrespective of input
        names(stack[[i]]) <- paste0("value_", 1:length(names(stack[[i]])))
      } else {
        # aggregation function names
        names(stack[[i]]) <- gsub("^.+_", paste0(names(stack)[[i]], "_"), names(stack[[i]]))
      }
      # prepend stub?
      if (!is.null(stub)) names(stack[[i]]) <- paste(stub, names(stack[[i]]), sep = "_")
    } 
    # save as SDS which keeps indicators separate but can be saved in a single cdf file
    if (!is.null(path)) {
      aggnames <- names(stack)
      if (cdf == TRUE) {
        print("Saving CDF...")
        # save for grid
        outfile <- paste0(
          path, "/", ifelse(is.null(stub), paste(unit), paste(stub, unit, sep = "_")), "_grid.nc"
          )
        terra::writeCDF(terra::sds(stack), file = outfile, overwrite = TRUE, compression = 5)
        log <- rbind(
          log, 
          data.frame(
            unit, outfile, geo = "grid", paste(aggnames, collapse = ", "), paste(aggby, collapse = "")
          )
        )
      }
      # extract and save value for particular geometries as Stata file
      if (length(extract) > 0) {
        if (timelong == TRUE) time <- time(stack[[1]]) # save time per aggregation for reshaping
        names(stack) <- NULL # avoid auto-naming by layer name
        stack <- terra::rast(stack)
        for (geo in names(extract)) {
          print(paste0("Extracting values for ", geo, " and saving for Stata..."))
          outfile <- paste0(
            path, "/", 
            ifelse(is.null(stub), paste(unit, geo, sep = "_"), paste(stub, unit, geo, sep = "_")),
            ".dta"
          )
          log <- rbind(
            log, 
            data.frame(
              unit, outfile, geo, paste(aggnames, collapse = ", "), paste(aggby, collapse = "")
            )
          )
          # extract weighted mean of raster cells covered by polygons
          # this has to be done in chunks of layers sequentially if the object is big
          # there is some pre-chunking going on in extract if chunksize is big
          # for radolan data, 200 layers per chunk seem to work well
          # with 12GB available to GDAL and appropriate max_cells_in_memory
          sf <- sf::st_as_sf(project(extract[[geo]], crs(stack)))
          values <- as.data.frame(extract[[geo]][, 1]) # geo ids
          if (nlyr(stack) > chunksize) {
            chunks <- list()
            nchunks <- ceiling(nlyr(stack)/chunksize)
            for (i in 1:nchunks) {
              lo <- (i-1) * chunksize + 1
              if (i < nchunks) hi <- i * chunksize else hi <- nlyr(stack)
              chunks[[i]] <- stack[[lo:hi]]
            }
          } else {
            chunks <- list(stack)
          }
          values <- data.frame(
            values,
            pblapply(
              chunks,
              exactextractr::exact_extract,
              y = sf,
              fun = "mean",
              force_df = TRUE,
              max_cells_in_memory = 9e+08,
              progress = FALSE
            )
          )
          colnames(values) <- gsub("^mean\\.", "", colnames(values))
          # reshape by time if requested; assign values from time dimension
          if (timelong == TRUE) {
            values <- pivot_longer(
              values,
              !any_of(c(geo)),
              names_to = c(".value", "time"),
              names_pattern = "(^.+)_(\\d+)"
            )
            if (timeInfo(stack)$step == "yearmonths") time <- as.Date.yearmon(time) # format exception
            values$time <- rep(time, length(unique(values[[geo]])))
          } else if (timeInfo(stack)$step == "yearmonths") {
            names <- colnames(values)
            names <- gsub("_\\d+$", "", names[2:length(names)])
            time <- gsub("\\s|-|-\\d{2}$", "", as.Date.yearmon(time(stack)))
            colnames(values) <- c(geo, paste(names, time, sep = "_"))
          }
          # save silently
          capture.output(sjlabelled::write_stata(values, outfile, version = 14), file = nullfile())
        }
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

# write out cleaned log returned by srproc, add info per arguments
writelog <- function(log, indicator, scope, file, gsubfilepath = NULL, colnames = FALSE, append = FALSE) {
  stopifnot(is.data.frame(log))
  # write out cleaned log with added info
  colnames(log) <- c("subset", colnames(log)[2:length(colnames(log))])
  log <- data.frame(indicator = indicator, scope = scope, log)
  if (!is.null(gsubfilepath)) log$file <- gsub(gsubfilepath, "", log$file)
  write.table(
    log,
    file = file,
    sep = ";", 
    row.names = FALSE,
    col.names = colnames,
    append = append
  )
  print(paste0("Written to ", file, ":"))
  print(log)
}


### precipitation ##################################################################################

### historical, monthly

# define output units and corresponding files
yf <- data.frame(unit = 1995:2022)
yf$input <- paste0(data, "/external/raw/DWD/pr_sum_daily/pr_hyras_1_", yf$unit, "_v5-0_de.nc")
yf$unit <- "all"
# specify aggregation functions
aggfuns <- list(
  mean = mean,
  min = min,
  max = max,
  d_low = function(x, na.rm = FALSE) { sum(x < 0.1, na.rm = na.rm) }, # no rain,
  d_high1 = function(x, na.rm = FALSE) { sum(x >= 30, na.rm = na.rm) }, # DWD Dauerregen
  d_high2 = function(x, na.rm = FALSE) { sum(x >= 50, na.rm = na.rm) } # DWD Ergiebiger Dauerregen
)
# call processing function
log <- srproc(
  yf,
  aggfuns = aggfuns,
  aggby = "yearmonth",
  path = paste0(data, "/external/processed/DWD/pr"),
  stub = "pr_hist",
  extract = list(postcode = p_postcode),
  cores = 3
)
# write log
writelog(
  log,
  indicator = "precipitation",
  scope = "historical",
  file = paste0(data, "/external/processed/DWD/log.csv"),
  gsubfilepath = data,
  colnames = TRUE
)


### presurvey, 14 days (including) survey day (28.10.2022 - 09.12.2022)

# define output units and corresponding files
yf <- data.frame(unit = "all")
yf$input <- paste0(data, "/external/raw/DWD/pr_sum_daily/pr_hyras_1_2022_v5-0_de.nc")
# specify aggregation functions
aggfuns <- list(
  mean = mean,
  min = min,
  max = max,
  d_low = function(x, na.rm = FALSE) { sum(x < 0.1, na.rm = na.rm) }, # no rain,
  d_high1 = function(x, na.rm = FALSE) { sum(x >= 30, na.rm = na.rm) }, # DWD Dauerregen
  d_high2 = function(x, na.rm = FALSE) { sum(x >= 50, na.rm = na.rm) } # DWD Ergiebiger Dauerregen
)
# call processing function
log <- srproc(
  yf,
  aggfuns = aggfuns,
  aggby = list("to", 14),
  forcetime = list(
    step = "days",
    timezone = "CET",
    period = list(start = "2022-10-28", end = "2022-12-09")
  ),
  path = paste0(data, "/external/processed/DWD/pr"),
  stub = "pr_presurvey",
  extract = list(postcode = p_postcode),
  timelong = TRUE
)
# write log
writelog(
  log,
  indicator = "precipitation",
  scope = "presurvey",
  file = paste0(data, "/external/processed/DWD/log.csv"),
  gsubfilepath = data,
  append = TRUE
)

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
# call processing function
log <- srproc(
  yf,
  forcetime = list(timezone = "CET"),
  path = paste0(data, "/external/processed/DWD/pr"),
  stub = "pr_survey",
  crs = crs,
  extract = list(postcode = p_postcode),
  timelong = TRUE
)
# write log
writelog(
  log,
  indicator = "precipitation",
  scope = "survey",
  file = paste0(data, "/external/processed/DWD/log.csv"),
  gsubfilepath = data,
  append = TRUE
)



### temperature ####################################################################################

### first aggregate to daily values

# define output units and corresponding files
yf <- data.frame(input = list.files(paste0(data, "/external/raw/DWD/temp_mean_hourly")))
yf <- data.frame(unit = gsub("(.+)(\\d{4})(\\d{6})(-.+)", "\\2", yf$input), yf)
yf$input <- paste0(data, "/external/raw/DWD/temp_mean_hourly/", yf$input)
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
  forcetime = list(timezone = "CET"),
  cdf = TRUE,
  path = paste0(data, "/external/temp/DWD/temp"),
  stub = "temp"
)


### temperature: historical monthly values

# define output units and corresponding files
yf <- data.frame(unit = 1995:2022)
yf$input <- paste0(
  data, "/external/temp/DWD/temp/temp_", yf$unit, "_grid.nc"
  )
yf$unit <- "all"
# specify aggregation functions: with reference to variable names stacked in sds
aggfuns <- list(
  mean = list(mean, "mean"),
  min = list(min, "min"),
  max = list(max, "max"),
  d_low = list(
    function(x, na.rm = FALSE) { sum(x < -10, na.rm = na.rm) }, # selbstdefiniert: < -10 Grad,
    "min"
  ),
  d_high1 = list(
    function(x, na.rm = FALSE) { sum(x > 32, na.rm = na.rm) }, # DWD starke Wärmebelastung
    "max"
  ),
  d_high2 = list(
    function(x, na.rm = FALSE) { sum(x > 38, na.rm = na.rm) }, # DWD extreme Wärmebelastung
    "max"
  )
)
# call processing function
log <- srproc(
  yf,
  aggfuns = aggfuns,
  aggby = "yearmonth",
  path = paste0(data, "/external/processed/DWD/temp"),
  stub = "temp_hist",
  extract = list(postcode = p_postcode),
  cores = 3
)
# write log
writelog(
  log,
  indicator = "temperature",
  scope = "historical",
  file = paste0(data, "/external/processed/DWD/log.csv"),
  gsubfilepath = data,
  append = TRUE
)


### presurvey, 14 days (including) survey day (28.10.2022 - 09.12.2022)
# if you ever attempt to count hot/cold days based on historic values, this would work:
# - stack daily values for all years, keep only survey period -> save data1
# - tapp by "doy", e.g. p20/p80 (do once per statistic) -> save data2
# - compare(data1, data2, "</>") -> save data3
# - roll data3 via sum function (e.g. counting days where temp < p20 30y daily temp)

# define output units and corresponding files
yf <- data.frame(unit = "all")
yf$input <- paste0(data, "/external/temp/DWD/temp/temp_2022_grid.nc")
# specify aggregation functions: with reference to variable names stacked in sds
aggfuns <- list(
  mean = list(mean, "mean"),
  min = list(min, "min"),
  max = list(max, "max")
)
# call processing function
log <- srproc(
  yf,
  aggfuns = aggfuns,
  aggby = list("to", 14),
  forcetime = list(
    step = "days",
    timezone = "CET",
    period = list(start = "2022-10-28", end = "2022-12-09")
  ),
  path = paste0(data, "/external/processed/DWD/temp"),
  stub = "temp_presurvey",
  extract = list(postcode = p_postcode),
  timelong = TRUE
)
# write log
writelog(
  log,
  indicator = "temperature",
  scope = "presurvey",
  file = paste0(data, "/external/processed/DWD/log.csv"),
  gsubfilepath = data,
  append = TRUE
)

### survey time 28.10.2022 - 09.12.2022 (hourly intervals -> no aggregation, just passthrough)

# define output units and corresponding files
files <- list.files(paste0(data, "/external/raw/DWD/temp_mean_hourly"))
files <- files[grep("tas_1hr_HOSTRADA-v1-0_BE_gn_2022(10|11|12).*\\.nc", files)]
files <- files[1]
yf <- data.frame(
  unit = rep("all", length(files)),
  input = paste0(paste0(data, "/external/raw/DWD/temp_mean_hourly/"), files)
)
# call processing function
log <- srproc(
  yf,
  forcetime = list(
    step = "hour",
    timezone = "CET",
    period = list(start = "2022-10-28", end = "2022-12-09")
  ),
  path = paste0(data, "/external/processed/DWD/temp"),
  stub = "temp_survey",
  cdf = FALSE,
  extract = list(postcode = p_postcode),
  timelong = TRUE
)
# write log
writelog(
  log,
  indicator = "temperature",
  scope = "survey",
  file = paste0(data, "/external/processed/DWD/log.csv"),
  gsubfilepath = data,
  append = TRUE
)

### testing area ###################################################################################

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
            LENGTHUNIT[\"metre\",1]],
    UNIT[\"metre\",1,
        AUTHORITY[\"EPSG\",\"9001\"]]
    ]"

w <- vect(paste0(data, "/external/raw/DWD/NUTS_RG_20M_2021_3035/NUTS_RG_20M_2021_3035.shp"))
t <- rast(paste0(data, "/external/raw/DWD/pr_sum_5min/2022/YW_2017.002_20221201.nc"), drivers="NETCDF")
t <- t[[1]]
crs(t) <- crs
t <- terra::project(t, crs(w))
r <- rast(paste0(data, "/external/raw/DWD/pr_sum_daily/pr_hyras_1_1995_v5-0_de.nc"))
r <- r[[1]]

jpeg(file = paste0(wd, "/docs/radolan_pr_coverage.jpg"), width = 800, height = 1000)
plot(is.na(t), alpha = 1, col = c("#ffffff", "#000000"), legend = FALSE, axes = FALSE, ext = ext(t) * 0.95)
plot(is.na(project(r, crs(w))), alpha = 0.5, add = TRUE, legend = FALSE)
plot(w, add = T)
title("Radolan 5min rain data coverage", line = 2.5)
dev.off()


t <- c(rast(yf[1,2]), rast(yf[2,2]), rast(yf[3,2]))
terra::writeRaster(t, file = "/home/max/Seafile/FoDiRa-SYR/survey/data/external/processed/DWD/temp/test.asc", overwrite = TRUE)


t <- rast(yf[1,2])
period <- c("2022-10-28", "2022-12-09", "forcedate")
t <- t[[as.Date(time(t)) >= period[[1]] & as.Date(time(t)) <= period[[2]]]]

# time comp extract terra vs. exactextractr
t <- rast(yf[1,2])
crs(t) <- crs
sf <- sf::st_as_sf(project(p_postcode, crs(t)))
df <- data.frame(postcode = sf[[1]])
tic()
e <- exactextractr::exact_extract(
  t,
  sf,
  fun = "mean",
  force_df = TRUE,
  progress = TRUE,
  append_cols = TRUE,
  stack_apply = TRUE
  )
sum(!is.na(e))
toc()

r <- rast(yf[1,2])
time(r) <- seq(
  from = as.POSIXlt(time(r)[1], tz = "CET"),
  to = as.POSIXlt(time(r)[length(time(r))] + 1, tz = "CET"),
  by = "hour"
)[1:length(time(r))]
time <- time(r)
period <- list(start = as.POSIXlt("2022-10-28", tz = "CET"), end = as.POSIXlt("2022-12-09", tz = "CET"))
time <- as.POSIXlt(time, tz = attr(period$start, "tzone"))
r <- r[[time >= period$start & time <= period$end]]
time(r)


t <- as.POSIXlt("2022-10-28")
t["years"]

for (i in 10:30) {
  r <- rast(paste0("C:/Users/max/Seafile/FoDiRa-SYR/survey/data/external/raw/DWD/pr_sum_5min/2022/YW_2017.002_202211", i, ".nc"))
  print(max(values(r), na.rm = TRUE))
}

r <- rast(paste0("C:/Users/max/Seafile/FoDiRa-SYR/survey/data/external/raw/DWD/YW_2017.002_20220101_0000.asc"))
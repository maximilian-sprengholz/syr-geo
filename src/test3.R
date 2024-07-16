### functions ######################################################################################

# aggregation function:
# 1. reads in a raster file
# 2. aggregates using passed vector of c(aggfunction, subset)
# 3. aggregates by passed index (either integer or one of the predefined time values, e.g. "month")
sragg <- function (file, aggfun, aggby, forcetime, period, cores) {
  # read
  r <- terra::rast(file)
  # force time format and zone; limiting to length(time(r)) ensures [start, end[ if output steps
  # more detailed than input time (e.g. ymd_h instead of ymd, and last datetime is then ymd 23:00)
  if (!is.null(forcetime)) {
    message("forcetime")
    time(r) <- seq(
      from = as.POSIXlt(time(r)[1], tz = forcetime$timezone),
      to = as.POSIXlt(time(r)[length(time(r))] + 1, tz = forcetime$timezone),
      by = forcetime$step
    )[1:length(time(r))]
  }
  # aggregation related checks and subsetting (rolling agg. extends period by 'window' ext(lo, hi))
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
    if (!is.null(period) & length(aggby) == 2) {
      n <- as.numeric(aggby[[2]])
      if (aggby[[1]] == "to") ext <- c(n-1, 0)
      if (aggby[[1]] == "around") ext <- c(ceiling((n-1)/2), floor((n-1)/2))
      if (aggby[[1]] == "from") ext <- c(0, n-1)
    }
  }
  # subset by period
  if (!is.null(period)) {
    tzone <- attr(period$start, "tzone")
    if (is.null(forcetime)) time <- as.POSIXlt(time, tz = tzone) else time <- time(r)
    r <- r[[time >= period$start - ext[[1]] & time <= period$end + ext[[2]]]]
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
      r <- terra::roll(r, n, aggfun[[1]], aggby[[1]], na.rm = FALSE, cores = cores)
      # trim rolling overhangs from period kept for aggregation
      if (!is.null(period)) {
        if (is.null(forcetime)) time <- as.POSIXlt(time, tz = tzone) else time <- time(r)
        r <- r[[time >= period$start & time <= period$end]]
      }
    }
  }
  # return
  return(r)
}

# stacking function: creates a new SpatRaster combining the layers of each aggregated SpatRaster
srstack <- function(aggfun, files, crs, aggby, forcetime, period, cores) {
  # aggregate each SpatRaster
  stack <- lapply(
    files,
    sragg,
    aggfun,
    aggby,
    forcetime,
    period,
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
    forcetime = NULL, # force time to specific step (days, hours, ...) and timezone
    period = NULL, # subset by period c(datelo, datehi); if rolling agg. time steps need to be aggby[[2]]
    cdf = FALSE, # write cdf?
    path = NULL, # out dir
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
  # check if forcetime has timezone and step period has two POSIXlt elements
  if (!is.null(forcetime)) {
    stopifnot(is.list(forcetime))
    stopifnot(!is.null(forcetime$step) & !is.null(forcetime$timezone))
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
      period = period,
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
            unit, outfile, geo = "grid", paste(varnames(stack), sep = ", "), paste(aggby, collapse = "")
          )
        )
      }
      # extract and save value for particular geometries as Stata file
      stack <- terra::rast(stack)
      for (geo in names(extract)) {
        print(paste0("Extracting values for ", geo, " and saving for Stata..."))
        outfile <- paste0(
          path, "/", 
          ifelse(is.null(stub), paste(unit, geo, sep = "_"), paste(stub, unit, geo, sep = "_")),
          ".dta"
        )
        # extract weighted mean of raster cells covered by polygons
        # this has to be done in chunks of layers sequentially if the object is big
        # there is some pre-chunking going on in extract if chunksize is big
        # for radolan data, 200 layers per chunk seem to work well
        # with 12GB available to GDAL and approproiate max_cells_in_memory
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
          time <- time(stack)
          if (timeInfo(stack)$step == "yearmonths") time <- as.Date.yearmon(time) # format exception
          values$time <- rep(time, length(unique(values[[geo]])))
        } else if (timeInfo(stack)$step == "yearmonths") {
          names <- colnames(values)
          names <- gsub("_\\d+$", "", names[2:length(names)])
          time <- gsub("\\s|-|-\\d{2}$", "", as.Date.yearmon(time(stack)))
          colnames(values) <- c(geo, paste(names, time, sep = "_"))
        }
        # save silently and log
        capture.output(sjlabelled::write_stata(values, outfile, version = 14), file = nullfile())
        log <- rbind(
          log, 
          data.frame(
            unit, outfile, geo, paste(varnames(stack), sep = ", "), paste(aggby, collapse = "")
          )
        )
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
chunks <- srproc(
  yf,
  forcetime = list(step = "hour", timezone = "CET"),
  period = list(start = as.POSIXlt("2022-10-28", tz = "CET"), end = as.POSIXlt("2022-12-09", tz = "CET")),
  path = paste0(data, "/external/processed/DWD/temp"),
  stub = "temp_survey",
  cdf = FALSE,
  extract = list(postcode = p_postcode),
  timelong = TRUE
)
# t <- rast(yf[1,2])
# sf <- sf::st_as_sf(project(p_postcode, crs(t)))
# values <- as.data.frame(p_postcode[, 1]) # geo ids
# chunks <- list(chunks)
# values <- data.frame(
#   values,
#   pblapply(
#     chunks,
#     exactextractr::exact_extract,
#     y = sf,
#     fun = "mean",
#     force_df = TRUE,
#     max_cells_in_memory = 9e+08,
#     progress = FALSE
#   )
# )
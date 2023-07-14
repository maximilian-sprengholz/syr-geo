#
# This file fetches INKAR data via API (pkg bonn):
# - ALL indicators available
#   - for 2020 or the next available year before (currently most recent year)
#   - for the smallest available level (should be Gemeinde or Kreis)
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

# CRAN
if (!require(pbapply)) install.packages("pbapply")
library(pbapply)

# github
if (!require(bonn)) remotes::install_github("sumtxt/bonn", force=TRUE)

### fetch which data is available ##################################################################

# geographies; use main ars entities for now smallest -> largest
geos_all <- get_geographies()
geos <- list("GEM", "GVB", "KRE", "RBZ")

# variables by theme x geo
df_vars <- NULL
for (geo in geos) {
  # get themes
  themes <- get_themes(geography = geo)
  # for each theme x geo: get indicators
  quietly <- pbapply(
    themes, 
    1, 
    function(row, geo) {
      vars <- get_variables(theme = row["ID"], geography = geo)
      vars$BereichID <- row["ID"]
      vars$Bereich <- row["Bereich"]
      vars$Unterbereich <- row["Unterbereich"]
      vars <- rename(vars, IndikatorID = Gruppe)
      vars$Raumbezug <- geo # not set in returns for Gemeinden
      df_vars <<- bind_rows(df_vars, vars)
      Sys.sleep(1)
      },
    geo = geo
    )    
}
df_vars <- df_vars %>% 
  select(IndikatorID, KurznamePlus, BereichID, Bereich, Unterbereich, Raumbezug)

# select what to fetch: smallest available entitity
# merge all metadata and save: easy lookup; use to determine which level is supposed to be merged
# also: save all returned IDs as safety measure, as metadata IDs are different from the indicator 
# IDs returned by get_*()
df_vars <- df_vars %>%
  arrange(BereichID, as.numeric(IndikatorID), factor(Raumbezug, levels = geos)) %>% 
  distinct(BereichID, IndikatorID, .keep_all = TRUE)
df_vars_metadata <- NULL
quietly <- pbsapply(
  df_vars$IndikatorID, 
  function(id) {
    metadata <- get_metadata(variable = id)
    rename(metadata, metadataID = ID)
    df_vars_metadata <<- bind_rows(df_vars_metadata, metadata)
    Sys.sleep(1)
    }
  )   
df_vars <- bind_cols(df_vars, df_vars_metadata)
df_vars$SyrGeoName <- tolower(gsub("\\s", "_", df_vars$Kurzname))
df_vars$FetchStatus <- 0
write_delim(df_vars, paste0(data, "/external/processed/INKAR/variable_index.csv"), delim = ";")


### fetch data (and track progess) #################################################################

# get ars file as base (ATM 2020 is most current and reference year!)
year <- 2020
df_ars <- read_csv(paste0(data, "/external/processed/ars/ars", year, ".csv"))
df_ars <- df_ars %>% 
  select(all_of(sort(c(paste0(tolower(geos), "_ars"), paste0(tolower(geos), "_name")))))

# fetch data; keep only most recent (for dynamics, additional requests are easy to do)
# merge to dataframe for each level
for (geo in geos) {
  # do if non-empty
  if (sum(df_vars$Raumbezug == geo) > 0) {
    # df with ars and name of geo entity
    arscol <- paste0(tolower(geo), "_ars")
    namecol <- paste0(tolower(geo), "_name")
    df <- df_ars %>% distinct(!!sym(arscol), !!sym(namecol))
    df[paste0(arscol, "_ref")] <- year # set ars ref (INKAR harmonized to most recent year)
    # get data (list of dfs is returned)
    df_vars[df_vars$Raumbezug == geo & df_vars$FetchStatus != 1, "FetchStatus"] <- unlist(pbapply(
      df_vars[df_vars$Raumbezug == geo & df_vars$FetchStatus != 1, ],
      1, 
      function(row, arscol) {
        tryCatch({
          # get data
          returns <- get_data(variable = row["IndikatorID"], geography = row["Raumbezug"])
          # keep most recent
          returns <- returns %>% 
            select(!c(Raumbezug, Indikator)) %>%
            arrange(Schlüssel, desc(Zeit)) %>% 
            distinct(Schlüssel, .keep_all = TRUE)
          # use consistent column names and indicator name derived from Kurzname
          name <- row["SyrGeoName"]
          colnames(returns) <- c(arscol, name, paste0(name, "_year"))
          # merge
          df <<- merge(df, returns, by = arscol, all.x = TRUE, all.y = FALSE)
          Sys.sleep(1)
          return(1)
          },
        error = function(e) {
          return(0)
          }
          )
        }, 
      arscol
      ))
    # save dataset
    write_delim(df, paste0(data, "/external/processed/INKAR/", tolower(geo), ".csv"), delim = ";")
    }
  }

# save progress to variable index
write_delim(df_vars, paste0(data, "/external/processed/INKAR/variable_index.csv"), delim = ";")

# total duration ~35 minutes
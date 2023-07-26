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
      Sys.sleep(0.5)
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
    Sys.sleep(0.5)
    }
  )   
df_vars <- bind_cols(df_vars, df_vars_metadata)

# rough doublette cleaning: exact (esp. with SDG stuff), typos, different geo levels
df_vars <- df_vars %>%
  arrange(Kurzname, Raumbezug) %>% 
  distinct(Kurzname, .keep_all = TRUE) %>% # GEM, GVB, KRE order alphabetical -> keeps smallest geo
  arrange(Name, Raumbezug) %>%
  distinct(Name, .keep_all = TRUE) %>%
  arrange(as.numeric(ID))
# create new variable names from "Bereich" and "ID" (whatever kind of ID that is...)
df_vars <- df_vars %>%
  mutate(bereich_short = tolower(case_when(
    Bereich == "Bauen und Wohnen" ~ "Bauen",
    Bereich == "Beschäftigung und Erwerbstätigkeit" ~ "Beschäftigung",
    Bereich == "Privateinkommen, Private Schulden" ~ "Einkommen",
    Bereich == "Flächennutzung und Umwelt" ~ "Umwelt",
    Bereich == "Medizinische und soziale Versorgung" ~ "Med Soz Versorgung",
    Bereich == "Öffentliche Finanzen" ~ "Öff Finanzen",
    Bereich == "Raumwirksame Mittel" ~ "Raumw Mittel",
    Bereich == "Verkehr und Erreichbarkeit" ~ "Verkehr",
    Bereich == "Zentrale Orte Monitoring" ~ "Orte Monitoring",
    Bereich == "SDG-Indikatoren für Kommunen" ~ "SDG",
    .default = Bereich
  )))
rep <- list(c("ä", "ae"), c("ö", "oe"), c("ü", "ue"), c("\\s", "_"))
for (r in rep) { df_vars$bereich_short <- gsub(r[1], r[2], df_vars$bereich_short) }
df_vars$variable <- paste(df_vars$bereich_short, df_vars$ID, sep = "_")

# other log stuff
df_vars$fetched <- 0
df_vars$year <- 2020
df_vars$year_max <- NA
df_vars$year_min <- NA
df_vars$year_modus <- NA
# order
df_vars <- df_vars %>%
  select(!c(bereich_short)) %>%
  select(variable, Name, Raumbezug, year, year_max, year_min, year_modus, colnames(.))
# write
write_delim(df_vars, paste0(data, "/external/processed/INKAR/variable_index.csv"), delim = ";")


### fetch data (and track progess) #################################################################

# get ars file as base (ATM 2020 is most current and reference year!)
year <- 2020
df_ars <- read_csv(paste0(data, "/external/processed/ars/ars", year, ".csv"))
df_ars <- df_ars %>% 
  select(all_of(c(paste0(tolower(geos), "_ars"), paste0(tolower(geos), "_name"), "pop")))

# fetch data; keep only most recent (for dynamics, additional requests are easy to do)
# merge to dataframe for each level
for (geo in geos) {
  # do if non-empty
  if (sum(df_vars$Raumbezug == geo) > 0) {
    # df with ars, name, and pop of geo entity
    arscol <- paste0(tolower(geo), "_ars")
    namecol <- paste0(tolower(geo), "_name")
    popcol <- paste0(tolower(geo), "_pop")
    if (file.exists(paste0(data, "/external/raw/INKAR/", tolower(geo), ".csv"))) {
      df <- read_delim(
        paste0(data, "/external/raw/INKAR/", tolower(geo), ".csv"), 
        delim = ";"
        )
    } else {
      # we need the pop count bc data is missing 
      df <- df_ars %>%
        mutate(pop = sum(pop, na.rm = TRUE), .by = all_of(c(arscol))) %>% 
        rename(!!sym(popcol) := pop) %>% 
        distinct(!!sym(arscol), !!sym(namecol), !!sym(popcol))
      df[paste0(arscol, "_ref")] <- year # set ars ref (INKAR harmonized to most recent year)
    }
    populated <- unlist(df[df[[popcol]] != 0, arscol]) # weird year values for non-populated areas
    # get data (list of dfs is returned)
    df_vars[df_vars$Raumbezug == geo & df_vars$fetched != 1, "fetched"] <- unlist(pbapply(
      df_vars[df_vars$Raumbezug == geo & df_vars$fetched != 1, ],
      1, 
      function(row, arscol) {
        tryCatch({
          # get data
          returns <- get_data(variable = row[["IndikatorID"]], geography = row[["Raumbezug"]])
          # keep most recent
          returns <- returns %>% 
            select(!c(Raumbezug, Indikator)) %>%
            mutate(Zeit = as.numeric(Zeit)) %>%
            arrange(Schlüssel, desc(Zeit)) %>%
            distinct(Schlüssel, .keep_all = TRUE)
          # time is not necessarily consistent! -> log
          years <- unlist(returns[returns[["Schlüssel"]] %in% populated, "Zeit"])
          uyears <- unique(years)
          uyears <- uyears[!is.na(uyears)]
          df_vars[df_vars$IndikatorID == row[["IndikatorID"]], "year_max"] <<- max(uyears)
          df_vars[df_vars$IndikatorID == row[["IndikatorID"]], "year_min"] <<- min(uyears)
          df_vars[df_vars$IndikatorID == row[["IndikatorID"]], 
            "year_modus"] <<- uyears[which.max(tabulate(match(years, uyears)))]
          # use consistent column names and indicator name derived from Kurzname
          name <- row[["variable"]]
          colnames(returns) <- c(arscol, name, paste0(name, "_year"))
          # merge
          df <<- merge(df, returns, by = arscol, all.x = TRUE, all.y = FALSE)
          Sys.sleep(0.5)
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
    write_delim(df, paste0(data, "/external/raw/INKAR/", tolower(geo), ".csv"), delim = ";")

    }
  }

# save progress to variable index
write_delim(df_vars, paste0(data, "/external/processed/INKAR/variable_index.csv"), delim = ";")
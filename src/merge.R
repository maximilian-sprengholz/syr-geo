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
library(terra)

### merge variable indices #########################################################################

df <- NULL
columns <- c(
  "variable", "desc", "geo", "year", "month", "source", "source_detail", "xwalk_time_weight", 
  "xwalk_geo_weight"
  )
for (source in c("Google", "BBA", "IVW", "BA", "Destatis", "INKAR")) {
  # read
  varindex <- read_delim(
    paste0(data, "/external/processed/", source, "/variable_index.csv"), 
    delim = ";"
    )
  # INKAR has some proprietary names, and non-xwalkable variables 
  if (source == "INKAR") {
    varindex <- varindex %>%
      mutate(
        source = "INKAR",
        geo = tolower(Raumbezug)
      ) %>%
      rename(
        desc = Name,
        source_detail = Quelle
        )
  }
  # append relevant columns
  df <- bind_rows(df, varindex)
}

# write
write_delim(
  df %>% select(all_of(columns)), 
  paste0(data, "/external/processed/variable_index.csv"), 
  delim = ";", 
  na = ""
  )


### merge all data #################################################################################

# use variable index for variables, weights, and ordering
varindex <- read_delim(paste0(data, "/external/processed/variable_index.csv"), delim = ";")

### merge by output geo: postcode, wohngebiet

# use google as base (is already in the right format)
dfs <- list(
  postcode = read_delim(paste0(data, "/external/processed/Google/postcode.csv"), delim = ";"),
  wohngebiet = read_delim(paste0(data, "/external/processed/Google/wohngebiet.csv"), delim = ";")
)

# xwalk and merge inputs
for (source in c("IVW", "BA", "Destatis", "INKAR")) {
  geos_in <- unique(unlist(varindex[varindex$source == source, "geo"]))
  for (geo_in in geos_in) {
    # read
    df <- read_delim(
      paste0(data, "/external/processed/", source, "/", geo_in, ".csv"), 
      delim = ";",
      show_col_types = FALSE
      )
    # weight and merge
    for (wref in c("abs", "rel")) {
      vars <- varindex[
        varindex$source == source 
          & varindex$geo == geo_in 
          & varindex$xwalk_geo_weight == paste0("area_w_", wref), 
        "variable"
        ]
      vars <- vars[!is.na(vars)]
      if (length(vars) > 0) {
        for (geo_out in c("postcode", "wohngebiet")) {
          if (geo_out == "postcode") groups <- geo_out else groups <- c("pid", "response_multisub_rank")
          dfs[[geo_out]] <- merge(
            dfs[[geo_out]],
            xwalk(
              df = df,
              geo = geo_in,
              to = geo_out,
              ars = paste0(geo_in, "_ars"),
              weight = "area",
              weightref = wref,
              variables = vars
              ),
            by = groups,
            all.x = TRUE,
            all.y = FALSE
            )
          }
        }
      }
    }
  }

# order and write
sapply(
  names(dfs), 
  function(geo) {
    write_delim(
      dfs[[geo]] %>% select(colnames(.), any_of(varindex$variable)), 
      paste0(data, "/syr_context_", geo, ".csv"), 
      delim = ";"
      )
    TRUE
    }
  )
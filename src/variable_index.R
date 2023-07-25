#
# This file:
# - merges the variable indices of different data sources to one big file

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


### merge variable indices #########################################################################

df <- NULL
columns <- c("variable", "desc", "geo", "year", "month", "source", "source_detail")
for (source in c("BA", "Destatis", "INKAR")) {
  # read
  varindex <- read_delim(
    paste0(data, "/external/processed/", source, "/variable_index.csv"), 
    delim = ";"
    )
  # INKAR has some proprietary names, and non-xwalkable variables 
  if (source == "INKAR") {
    varindex <- varindex %>%
      filter(!is.na(xwalk_time_weight)) %>%
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
  df <- bind_rows(df, varindex %>% select(any_of(columns)))
}

# write
write_delim(df, paste0(data, "/external/processed/variable_index.csv"), delim = ";", na = "")
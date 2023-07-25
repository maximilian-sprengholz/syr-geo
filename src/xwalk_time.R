#
# This file creates correspondence tables for geo crosswalks:
# - over time for the same units
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
library(tidyr)

# CRAN
if (!require(pbapply)) install.packages("pbapply")
library(pbapply)

### GV - ADMINISTRATIVE CHANGES 2020 - 2022 ########################################################

# INKAR correspondence tables refer to changes BETWEEN years y1 and y2 at 31.12.
# = equivalent to GV change tables WITHIN y2
# There seems to be some threshold to which changes are considered for INKAR:
# very small changes or simultaneous and counteracting changes are omitted
# (e.g. 20% of gem1 go to gem2 and approx. vice versa)

# target ars from 31.12.2022 GV
df_ars_2022 <- read_csv(paste0(data, "/external/processed/ars/ars2022.csv"))

### function creating ars correspondence table per year and ars lvl
process_gvchange <- function(year) {
  
  # set in and out ars x year for clarity
  year_in <- year - 1
  year_out <- year 
  ars_in <- paste0("gem_ars_full_", year_in)
  ars_out <- paste0("gem_ars_full_", year_out)
  
  # read and name
  fpath <- paste0(data, "/external/raw/Destatis/GV/3112", year_out, "_Aenderungen_GV.xlsx")
  gv <- read_excel(fpath, sheet = 2, skip = 4, col_names = FALSE)
  gv <- gv[, c(2, 3, 6, 7, 8, 9)]
  colnames(gv) <- c("lvl", ars_in, "type", "area_shared", "pop_shared", ars_out)
  gv$area_shared <- as.numeric(gv$area_shared)/1000000 # km2
  gv$pop_shared <- as.numeric(gv$pop_shared)
  
  # look only at changes for GEM; omit namechanges (these will be available from the GV per year)
  gv <- gv %>% filter(lvl == "Gemeinde" & type != "4") %>% select(-c(lvl, type))
  
  # merge to base ars to get full area/pop and names for in
  df_ars <- read_csv(paste0(data, "/external/processed/ars/ars", year_in, ".csv"))
  df_ars <- df_ars %>%
    select(!c(pop_m, pop_w, year)) %>%
    rename_with(~ paste0(.x, "_", year_in), !matches("^area.*|^pop.*")) %>%
    rename(area_in = area, pop_in = pop) # are area value consistent between GV and change files?
  df <- merge(df_ars, gv, by = ars_in, all.x = TRUE)
  df[is.na(df[[ars_out]]), ars_out] <- df[is.na(df[[ars_out]]), ars_in] # no change in ars if mi
  
  # merge base ars to get full area/pop and names for out
  df_ars <- read_csv(paste0(data, "/external/processed/ars/ars", year_out, ".csv"))
  df_ars <- df_ars %>%
    select(!c(area, pop, pop_m, pop_w, year)) %>%
    rename_with(~ paste0(.x, "_", year_out))
  df <- merge(df, df_ars, by = ars_out, all.x = TRUE)
  
  # generate weights
  # w_abs -> for absolute values, e.g. counts: reference is the in area/pop (n of x -> y)
  # w_rel -> for relative values, e.g. %; reference is the out area/pop (% of x -> % of y)
  df <- df %>% 
    mutate(
      # replace missing values (= no change to entity, full pop or area)
      area_shared = ifelse(is.na(area_shared), area_in, area_shared),
      pop_shared = ifelse(is.na(pop_shared), pop_in, pop_shared),
      ) %>%
    mutate(
      # calculate sum of area / pop for new entity (this is different from the out year values!)
      area_out = sum(area_shared, na.rm = TRUE),
      pop_out = sum(pop_shared, na.rm = TRUE),
      .by = all_of(ars_out)
      ) %>%
    mutate(
      area_w_abs = area_shared / area_in,
      area_w_rel = area_shared / area_out,
      pop_w_abs = ifelse(pop_in == 0, 1, pop_shared / pop_in),
      pop_w_rel = ifelse(pop_out == 0, 1, pop_shared / pop_out)
    )
  
  ### save in list of dfs, aggregate for gvb and kre (rbz and bl irrelevant, but possible)
  lvls <- c("bl", "rbz", "kre", "gvb", "gem")
  dfs <- list()
  
  # gem
  lvl <- "gem"
  dfs[[lvl]] <- df %>% 
    select(!matches(paste0(lvls[!lvls %in% lvl], "_.*", collapse = "|"))) %>%
    rename_with(~ paste0(.x, "_", year_in), matches(".*_w_.*"))
  
  # aggregation levels
  for (lvl in c("gvb", "kre")) {
    ars_in <- paste0(lvl, "_ars_", year_in)
    ars_out <- paste0(lvl, "_ars_", year_out)
    dfs[[lvl]] <- df %>%
      add_count(!!sym(paste0("gem_ars_full_", year_in)), name = "dupn") %>% # factor for gem dups
      mutate(
        area_in = sum(area_in / dupn),
        pop_in = sum(pop_in / dupn),
        .by = all_of(ars_in)
        ) %>%
      mutate(
        area_out = sum(area_shared),
        pop_out = sum(pop_shared),
        .by = all_of(ars_out)
        ) %>%
      mutate(
        area_shared = sum(area_shared),
        pop_shared = sum(pop_shared),
        .by = all_of(c(ars_in, ars_out))
        ) %>%
      distinct(!!sym(ars_in), !!sym(ars_out), .keep_all = TRUE) %>%
      mutate(
        area_w_abs = area_shared / area_in,
        area_w_rel = area_shared / area_out,
        pop_w_abs = ifelse(pop_in == 0, 1, pop_shared / pop_in),
        pop_w_rel = ifelse(pop_out == 0, 1, pop_shared / pop_out)
        ) %>%
      select(!matches(paste0(lvls[!lvls %in% lvl], "_.*", collapse = "|"))) %>%
      select(!c(dupn)) %>%
      rename_with(~ paste0(.x, "_", year_in), matches(".*_w_.*"))
    }

  # return
  return(dfs)
  }

merge_gvchange <- function(years, lvls) {

  # gather all based on first year changes
  dfs <- process_gvchange(years[1])

  # gather and merge info of all other yearly changes
  if (length(years) > 1) {
    for (year in years[2:length(years)]) {
      # fetch yearly changes
      df_current <- process_gvchange(year)
      # merge years by level
      for (lvl in lvls) {
          mergecol <- paste0(lvl, "_ars_", year-1)
          # merge
          dfs[[lvl]] <- merge(
            dfs[[lvl]] %>% 
              select(matches(paste0(mergecol, "|.*_w_.*|", paste0(".*_", years[1]-1)))),
            df_current[[lvl]] %>%
              select(matches(paste0(mergecol, "|.*_w_.*|", paste0(".*_", years[length(years)])))),
            by = mergecol, 
            all.x = TRUE,
            all.y = TRUE
            )
          dfs[[lvl]] <- dfs[[lvl]] %>% select(!matches(paste0(".*_", year-1)))
        }  
      }
    }
  # calculate weights over years; subset and order
  for (lvl in lvls) {  
    for (w in c("area_w_abs", "area_w_rel", "pop_w_abs", "pop_w_rel")) {
      dfs[[lvl]][[w]] <- apply(
        dfs[[lvl]] %>% select(matches(paste0(w, "_.*"))), 
        1, 
        function(x) { round(prod(x), 3) }
        )
    }
    dfs[[lvl]] <- dfs[[lvl]] %>% 
      select(!matches(paste(".*_w_.*_\\d+"))) %>%
      select(matches(paste0(years[1] - 1)), matches("_w_"), matches(paste0(years[length(years)])))
  }

  # write
  lapply(
    names(dfs), 
    function(lvl) {
      write_delim(
        dfs[[lvl]],
        paste0(
          data, "/external/processed/ars/xwalk_", 
          lvl, "_", years[1]-1, "_", years[length(years)], ".csv"
          ), 
        delim = ";"
        )
      TRUE
      }
    )

    # return
    return(dfs)

  }

# run
# CHANGES over given years:
# changes for 2022 occur between 31.12.2021 and 31.12.2022 => xwalk 2021 data -> 2022 data
dfs <- merge_gvchange(2021:2022, c("gem", "gvb", "kre"))
dfs <- merge_gvchange(2022, c("gem", "gvb", "kre"))

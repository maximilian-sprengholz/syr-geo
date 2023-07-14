#
# This file fetches Destatis data (pkg restatis) for selected indicators
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
library(dotenv)
library(httr2)

# CRAN
if (!require(pbapply)) install.packages("pbapply")
library(pbapply)
if (!require(restatis)) install.packages("restatis")
library(restatis)
# gen_auth_save() # do once manually, only works with libssl.so.1.1; needs to be in shared env

# github
if (!require(ags)) remotes::install_github("sumtxt/ags", force=TRUE)
library(ags)

# AUTH
load_dot_env(file = paste0(wd, "/src/.env"))
genesis_user <- Sys.getenv("GENESIS_USER")
genesis_pass <- Sys.getenv("GENESIS_PASS")


### population by kreis ############################################################################
# https://www-genesis.destatis.de/genesis//online?operation=table&code=12411-0015
# This information is used to split the aggregated info from the Ausländerstatistik for several
# Kreise
code <- "12411-0015"
metadata <- gen_metadata_tab(code)
years <- 1998:2022
returns <- gen_table(
  code,
  regionalvariable = "KREISE",
  startyear = 1998,
  endyear = 2022,
  language = "de"
  )
# keep relevant columns, label, save
df_pop <- returns %>% 
  select(Zeit, `1_Auspraegung_Code`, BEVSTD__Bevoelkerungsstand__Anzahl)
colnames(df_pop) <- c("zeit", "kre_ars", "pop")
df_pop$pop <- as.numeric(df_pop$pop)
# save
write_delim(df_pop, paste0(data, "/external/raw/Destatis/Bevölkerung/kre.csv"), delim = ";")


### foreigners by nationality per kreis ############################################################
# https://www-genesis.destatis.de/genesis//online?operation=table&code=12521-0041

code <- "12521-0041"
metadata <- gen_metadata_tab(code)

# fetch by year & bundesland to keep tables under the limit (otherwise throws errors of no result)
# bayern has too many kreise, needs to be split further
years <- 1998:2022
kre_groups <- c(
  paste0("0", 1:8, "*"),
  paste0("09", 1:7, "*"), # Bayern: 091* - 097*
  paste0(10:16, "*")
  )
pbsapply(
  years,
  function(year) {
    tryCatch({
      df <- NULL
      for (group in kre_groups) {
        returns <- gen_table(
          code,
          regionalvariable = "KREISE",
          regionalkey = group,
          startyear = year,
          endyear = year,
          classifyingvariable1 = "GES",
          classifyingvariable2 = "STAAG6",
          language = "de"
          )
        returns$`1_Auspraegung_Code` <- as.character(returns$`1_Auspraegung_Code`) # prevent doubles
        df <- bind_rows(df, returns)
        Sys.sleep(1)
        }
      # save by year to keep manageable, process in second step
      write_delim(
        df, 
        paste0(data, "/external/raw/Destatis/Ausländerstatistik/kre_", year, ".csv"),
        delim = ";"
        )
      return(TRUE)
      },
    error = function(e) {
      return(e)
      })
    }
  )
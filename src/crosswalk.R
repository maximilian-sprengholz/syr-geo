#
# This file performs geo crosswalks:
# - over time for the same units
# - between different geo units for the same time
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

# CRAN
if (!require(pbapply)) install.packages("pbapply")
library(pbapply)

# github
if (!require(ags)) remotes::install_github("sumtxt/ags", force=TRUE)
library(ags)


### XXX ############################################################################################

data(btw_sn)
df <- tibble(btw_sn)
btw_sn_ags20 <- xwalk_ags(
  data=btw_sn, 
  ags="district", 
  time="year", 
  xwalk="xd20"
  )
btw_sn_ags20
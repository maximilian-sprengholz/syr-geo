#
# This file merges Destatis data (fetched via API) for selected indicators
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
library(readxl)

# CRAN
if (!require(pbapply)) install.packages("pbapply")
library(pbapply)

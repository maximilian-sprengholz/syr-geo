#
# This file fetches DWD data:
# - precipitation: daily (1995-2022), 5min (2022)
# - temperature: hourly (1995-2022)

### config (relative to working dir syr-geo!)
source("src/_config.R")

### packages

# conda managed
library(rvest)
library(stringr)


### precipitation ##################################################################################

### daily
# get file links from url
url <- "https://opendata.dwd.de/climate_environment/CDC/grids_germany/daily/hyras_de/precipitation/"
webpage <- read_html(url)
files <- webpage %>%
  html_nodes("a") %>%
  html_attr("href")
files <- files[grep("pr_hyras_1_(199[5-9]|200[0-9]|201[0-9]|2020|2021|2022)_v5-0_de.nc", files)]
# download
for (f in files) {
  download.file(
    paste0(url, f), 
    paste0(data, "/external/raw/DWD/precip_sum_daily/", f)
  )
}

### 5min (just a single file for 2022)
# download
f <- "YW2017.002_2022_netcdf.tar.gz"
outfile <- paste0(data, "/external/raw/DWD/precip_sum_5min/", f)
download.file(
  paste0(
    "https://opendata.dwd.de/climate_environment/CDC/grids_germany/5_minutes/radolan/reproc/2017_002/netCDF/2022/",
    f
  ), 
  outfile
)
# extract and delete archive
untar(outfile, exdir = paste0(data, "/external/raw/DWD/precip_sum_5min/"))
file.remove(outfile)


### temperature ####################################################################################

# get file links from url
url <- "https://opendata.dwd.de/climate_environment/CDC/grids_germany/hourly/hostrada/air_temperature_mean/"
webpage <- read_html(url)
files <- webpage %>%
  html_nodes("a") %>%
  html_attr("href")
files <- files[grep("tas_1hr_HOSTRADA-v1-0_BE_gn_(199[0-9]|200[0-9]|201[0-9]|2020|2021|2022).*\\.nc", files)]
# download
for (f in files) {
  download.file(
    paste0(url, f), 
    paste0(data, "/external/raw/DWD/temp_mean_hourly/", f)
  )
}
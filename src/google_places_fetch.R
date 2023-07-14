#
# This file handles Google Places API calls to fetch:
# - turkish/arabic restaurants/take-outs
# - turkish/arabic supermarkets
# - mosques
# in Germany (generally everything, but sequentially by postcode)
#

### config (relative to working dir syr-geo!)
source("src/_config.R")

### packages

# conda managed
library(dplyr)
library(readr)
library(stringr)
library(tibble)
library(terra)
library(sf)
library(lwgeom)
library(jsonlite)
library(dotenv)

# CRAN only
if (!require(googleway)) install.packages("googleway")
library(googleway)
if (!require(pbapply)) install.packages("pbapply")
library(pbapply)


### api key
load_dot_env(file = paste0(wd, "/src/.env"))
#apikey <- Sys.getenv("GOOGLE_PLACES") # Max
apikey <- Sys.getenv("GOOGLE_PLACES2") # Christian
assign("has_internet_via_proxy", TRUE, environment(curl::has_internet)) # circumvent hu net error

### data fetching loop
"
Fetch function which uses the arguments from the postcode geometry collection:

- should return a success code to the collection and save out the results
- do the results binding later based on the saved results, so in-between errors are no problem

"
fetch_places <- function(row, query_list) {
  
  # do the error handling at the top level
  # return success value in the end

  success <- tryCatch(
    
    {
    # create empty dataframe to which to append results from queries
    df <- data.frame()

    # repeat for all queries in vector
    for (query in query_list$queries) {

      continue <- TRUE
      page_token <- NULL
      if (query == "") query <- NULL

      # using a page token, we can get the next 20 results per query (all other params ignored)
      while (continue == TRUE) {

        # API request
        data <- google_places(
          # no query needed for mosques, include postcode in query for best results
          search_string = query,
          language = "de",
          location = c(st_coordinates(row$bcircle_center)[2], st_coordinates(row$bcircle_center)[1]),
          radius = row$bcircle_radius,
          place_type = query_list$type,
          key = apikey,
          page_token = page_token
          )

        # check status
        if (!data$status %in% c("OK", "ZERO_RESULTS")) {
          # error
          stop(paste("API Error:", data$status))
        } else if (data$status == "ZERO_RESULTS") {
          # no results
          break
        } else { 
          # check if postcode in results after filling query column (to distinguish queries later)
          df_res <- as.data.frame(data$results)
          if ("formatted_address" %in% colnames(df_res)) {
            df_res$postcode <- sapply(df_res$formatted_address, function(x) str_match(x, "\\d{5}"))
            # check if at least one fitting postcode in last 10 results
            if (sum(df_res[11:20, "postcode"] == row$plz, na.rm = TRUE) == 0) continue <- FALSE
            # subset to fitting postcodes (or missing postcodes for manual cleaning)
            df_res <- df_res %>% filter(postcode == row$plz | is.na(postcode))
            }
          # if results remain, save query and append
          if (nrow(df_res) == 0) break
          df_res$query <- query
          df <- bind_rows(df, df_res)

          # check if requesting next page is worthwhile
          if ("next_page_token" %in% names(data) & continue == TRUE) {
            # continue with delay: tokens become valid after a short time
            page_token <- data$next_page_token
            Sys.sleep(2)
          } else {
            continue <- FALSE
          }
        }
      }
    }

    # write df to file (will be merged later)
    if (nrow(df) > 0) {
      saveRDS(df, file = paste0(data, "/external/raw/Google/", paste(row$plz, query_list$type, sep = "_"), ".rds"))
      return("non-empty")
    } else {
      return("empty")
    }
  },
  error = function(e) {
      message(paste("Error in", row$plz, ":", e))
      return("error")
  }
  )
}

### generate smallest encompassing circle around postcode from shapefiles
# from these circles, we can get the center point and radius to be passed on to the API request
df_geo <- tryCatch(
  {
  df <- readRDS(paste0(data, "/external/raw/Google/postcode_fetching_status.rds"))
  },
  warning = function(e) {
    df <- vect(paste0(data, "/external/raw/Shapefiles/plz/plz-5stellig.shp"))
    df <- sf::st_as_sf(df)
    df <- df %>%
      rowwise() %>%
      mutate(
        bcircle = st_minimum_bounding_circle(geometry),
        bcircle_center = st_centroid(bcircle),
        bcircle_radius = sort(
          as.vector(st_distance(bcircle_center, st_cast(bcircle, to = "POINT"), by_element = TRUE))
          , decreasing = TRUE
          )[1]
      )
  }
)

### fetch

# use named list as input
query_input <- list(
  restaurants = list(
    queries = c("türkisch", "arabisch"),
    type = "restaurant"
    ),
  supermarkets = list(
    queries = c("türkisch", "arabisch"),
    type = "supermarket"
    ),
  mosques = list(
    queries = c(""),
    type = "mosque"
    )
  )

# fetch by postcode and save status (in case of error, we can just run what's missing)
# Hamburg test:
# df_geo_rest <- df_geo %>% filter(!str_detect(note, "Hamburg"))
# df_geo <- df_geo %>% filter(str_detect(note, "Hamburg"))
for (input in query_input) {
  # create status column if non-existent
  statuscol <- paste0(input$type, "_fetch")
  if (!statuscol %in% colnames(df_geo)) df_geo[statuscol] <- NA
  # fetch info for all postcodes where info is missing (either untried or error)
  print(paste("Fetching:", input$type))
  df_geo[(df_geo[statuscol] == "error" | is.na(df_geo[statuscol])), statuscol] <- pbapply(
    df_geo[(df_geo[statuscol] == "error" | is.na(df_geo[statuscol])), ], 
    1, 
    fetch_places, 
    input
    )
}
# df_geo <- bind_rows(df_geo, df_geo_rest)
saveRDS(df_geo, file = paste0(data, "/external/raw/Google/postcode_fetching_status.rds"))

### merge
for (type in c("restaurant", "supermarket", "mosque")) {
  df <- data.frame()
  statuscol <- paste0(type, "_fetch")
  nonempty <- df_geo %>% filter(!!sym(statuscol) == "non-empty")
  for (postcode in nonempty$plz) {
    # for all postcodes: look at the files we got and bind them together
    part <- readRDS(paste0(data, "/external/raw/Google/", postcode, "_", type, ".rds"))
    df <- bind_rows(df, part)
  }
  # save without (exact) duplicates (only relevant for mosques which had no prior postcode check)
  saveRDS(distinct(df), file = paste0(data, "/external/raw/Google/", type, "s.rds"))
}
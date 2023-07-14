#
# This file:
# - cleans the Google Places data for supermarkets, restaurants, and mosques automatically
# - exports a file with cases which might be duplicates or wrong results for manual cleaning
# - subsets the final data based on the manual coding
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
sf_use_s2(FALSE)
library(units)
library(tictoc)

# CRAN only
if (!require(pbapply)) install.packages("pbapply")
library(pbapply)
if (!require(smoothr)) install.packages("smoothr")
library(smoothr)
if (!require(tidyterra)) install.packages("tidyterra")
library(tidyterra)


### prep Google data ###############################################################################

# timing start
tic()

### mosques

x <- "mosques"
df_google <- read_rds(paste0(data, "/external/raw/Google/", x, ".rds"))

# operational; no duplicates
df_google <- df_google %>% 
  filter(business_status == "OPERATIONAL") %>%
  distinct(place_id, .keep_all = TRUE)

# get coordinates
df_google$lon <- df_google$geometry$location$lng
df_google$lat <- df_google$geometry$location$lat
df_google <- df_google %>% select(-c(geometry))
print(nrow(df_google))

# rough duplicate cleaning seems OK, most duplicates are mosques + society and some stale results)
# of the duplicated, we always keep the (first) entry where name contains "mosque" (if any)
# keep one per lon x lat x address
df_google <- df_google %>% 
  mutate(mname = ifelse(str_detect(name, regex("moschee|mosque", ignore_case = TRUE)), TRUE, FALSE))
df_google <- df_google %>%
  mutate(lon_round = sprintf("%.1f", lon)) %>%
  mutate(lat_round = sprintf("%.1f", lat)) %>%
  group_by(lon_round, lat_round, vicinity) %>%
  arrange(desc(mname), .by_group = TRUE) %>% # arranging so mosque in name is first (if present)
  mutate(duprown = row_number()) %>%
  ungroup %>%
  filter(duprown == 1)
# keep one per lon x lat (4-digit precision); 
df_google <- df_google %>%
  mutate(lon_round = sprintf("%.4f", lon)) %>%
  mutate(lat_round = sprintf("%.4f", lat)) %>%
  group_by(lon_round, lat_round) %>%
  arrange(desc(mname), .by_group = TRUE) %>% # arranging so mosque in name is first (if present)
  mutate(duprown = row_number()) %>%
  ungroup %>%
  filter(duprown == 1)

# count duplicates on lon x lat (3-digit precision) to allow for manual cleaning
df_google <- df_google %>%
  mutate(lon_round = sprintf("%.3f", lon)) %>%
  mutate(lat_round = sprintf("%.3f", lat)) %>%
  add_count(lon_round, lat_round, name = "dupn") %>%
  group_by(lon_round, lat_round) %>%
  arrange(desc(mname), .by_group = TRUE) %>% # arranging so mosque in name is first (if present)
  mutate(dupid = cur_group_id()) %>%
  mutate(duprown = row_number()) %>%
  ungroup %>%
  arrange(desc(dupn), dupid, duprown)

# create spatvector
df_google <- sf::st_as_sf(df_google, coords = c("lon", "lat"), crs = st_crs(4326))

# get postcode; drop those without postcode match (= non-German territory)
p <- vect(paste0(data, "/external/raw/Shapefiles/plz/plz-5stellig.shp"))
df_google$postcode <- pbsapply(
  df_google$geometry,
  function(x, p) {
    mask <- relate(vect(x), p, "intersects")
    p$plz[mask][1]
  },
  p)
df_google <- df_google %>% filter(!is.na(postcode))

# save dataset for manual cleaning of remaining duplicates
write_delim(
  df_google %>% 
    filter(dupn > 1) %>%
    select(place_id, name, vicinity, postcode, dupid, dupn, duprown) %>%
    mutate(drop = NA, dropcomment = NA),
  paste0(data, "/external/processed/Google/", x, "_manualcheck.csv"),
  delim = ";"
  )

# import manually cleaned dataset: Julia
df_manualcheck <- read_delim(
  paste0(data, "/external/processed/Google/", x, "_manualcheck_jw.csv"),
  delim = ";"
  )
df_google <- merge(
    df_google, 
    df_manualcheck %>% select(place_id, drop), 
    by = "place_id", all.x = TRUE, all.y = FALSE
    )
df_google <- df_google %>% filter(is.na(drop)) %>% select(!c(drop))

# write cleaned dataset
saveRDS(df_google, file = paste0(data, "/external/processed/Google/", x, ".rds"))


### supermarkets

x <- "supermarkets"
df_google <- read_rds(paste0(data, "/external/raw/Google/", x, ".rds"))

# operational, no duplicates
df_google <- df_google %>% 
  filter(business_status == "OPERATIONAL") %>% 
  distinct(place_id, .keep_all = TRUE)

# drop those outside Germany
neighbors <- "schweiz|österreich|tschechien|polen|niederlande|frankreich|luxemburg"
df_google <- df_google %>% 
  filter(!str_detect(formatted_address, regex(neighbors, ignore_case = TRUE)))

# drop standard discounters / supermarkets
drop <- c(
  "adix", "aldi", "alnatura", "automat", "combi", "cap", "diska", "e[\\s-]center", "e[\\s-]aktiv", 
  "edeka", "family frisch", "feneberg", "getr[aeä]*nke[-]*markt", "hit\\smarkt", "inkoop", 
  "kaufland", "konsum", "lidl", "maga[zs]in", "markant[\\s-]*markt", "marktkauf", "mix\\smarkt", 
  "nahkauf", "nah\\s&\\sgut", "nah\\sund\\sgut", "nahkauf", "naturmarkt", "natursupermarkt", 
  "netto", "norma", "np[\\s-]markt", "prima", "rewe", "spar", "tchibo", "tegut", "v-markt", 
  "v-mini", "weihnacht"
  )
df_google <- df_google %>% 
  filter(
    !str_detect(name, regex(paste(paste0("\\b", drop, "\\b"), collapse = "|"), ignore_case = TRUE))
    & !str_detect(name, regex("^supermarkt$|^discounter$", ignore_case = TRUE))
    & !str_detect(name, regex("asia[\\S]*", ignore_case = TRUE))
    )

# get coordinates
df_google$lon <- df_google$geometry$location$lng
df_google$lat <- df_google$geometry$location$lat
df_google <- df_google %>% select(-c(geometry))

# create dataset for manual cleaning; mark duplicates by address and rough lon lat
df_google <- df_google %>%
  mutate(lon_round = sprintf("%.1f", lon)) %>%
  mutate(lat_round = sprintf("%.1f", lat)) %>%
  add_count(lon_round, lat_round, formatted_address, name = "dupn") %>%
  group_by(lon_round, lat_round, formatted_address) %>%
  mutate(dupid = cur_group_id()) %>%
  mutate(duprown = row_number()) %>%
  ungroup %>%
  arrange(desc(dupn), dupid, duprown)

# save dataset for manual cleaning of remaining duplicates
write_delim(
  df_google %>% 
    select(place_id, name, formatted_address, dupid, dupn, duprown) %>%
    mutate(drop = NA, dropcomment = NA),
  paste0(data, "/external/processed/Google/", x, "_manualcheck.csv"),
  delim = ";"
  )

# create spatvector
df_google <- sf::st_as_sf(df_google, coords = c("lon", "lat"), crs = st_crs(4326))

# # import manually cleaned dataset: XX
# df_manualcheck <- read_delim(
#   paste0(data, "/external/processed/Google/", x, "_manualcheck_XX.csv"),
#   delim = ";"
#   )
# df_google <- merge(
#     df_google, 
#     df_mosques_manualcheck %>% select(place_id, drop), 
#     by = "place_id", all.x = TRUE, all.y = FALSE
#     )
# df_google <- df_google %>% filter(is.na(drop)) %>% select(!c(drop))

# # import manually cleaned dataset: XX
# df_manualcheck <- read_delim(
#   paste0(data, "/external/processed/Google/", x, "_manualcheck_XX.csv"),
#   delim = ";"
#   )
# df_google <- merge(
#     df_google, 
#     df_manualcheck %>% select(place_id, drop), 
#     by = "place_id", all.x = TRUE, all.y = FALSE
#     )
# df_google <- df_google %>% filter(is.na(drop)) %>% select(!c(drop))

# write cleaned dataset
saveRDS(df_google, file = paste0(data, "/external/processed/Google/", x, ".rds"))


### restaurants

x <- "restaurants"
df_google <- read_rds(paste0(data, "/external/raw/Google/", x, ".rds"))

# operational, no duplicates
df_google <- df_google %>% 
  filter(business_status == "OPERATIONAL") %>% 
  distinct(place_id, .keep_all = TRUE) %>%
  distinct(name, formatted_address, .keep_all = TRUE)

# drop those outside Germany
neighbors <- "schweiz|österreich|tschechien|polen|niederlande|frankreich|luxemburg"
df_google <- df_google %>% 
  filter(!str_detect(formatted_address, regex(neighbors, ignore_case = TRUE)))

# drop standard discounters / supermarkets
drop <- c(
  "subway", "dr.\\wok", "indisch[es]*", "griech[\\S]*", "asia[\\S]*"
  )
df_google <- df_google %>% 
  filter(
    !str_detect(name, regex(paste(paste0("\\b", drop, "\\b"), collapse = "|"), ignore_case = TRUE))
    )

# get coordinates
df_google$lon <- df_google$geometry$location$lng
df_google$lat <- df_google$geometry$location$lat
df_google <- df_google %>% select(-c(geometry))

# create dataset for manual cleaning; mark duplicates by address and rough lon lat
df_google <- df_google %>%
  mutate(lon_round = sprintf("%.1f", lon)) %>%
  mutate(lat_round = sprintf("%.1f", lat)) %>%
  add_count(lon_round, lat_round, formatted_address, name = "dupn") %>%
  group_by(lon_round, lat_round, formatted_address) %>%
  mutate(dupid = cur_group_id()) %>%
  mutate(duprown = row_number()) %>%
  ungroup %>%
  arrange(desc(dupn), dupid, duprown)

# save dataset for manual cleaning of remaining duplicates
write_delim(
  df_google %>%
    filter(dupn > 1) %>%
    select(place_id, name, formatted_address, dupid, dupn, duprown) %>%
    mutate(drop = NA, dropcomment = NA),
  paste0(data, "/external/processed/Google/", x, "_manualcheck.csv"),
  delim = ";"
  )

# create spatvector
df_google <- sf::st_as_sf(df_google, coords = c("lon", "lat"), crs = st_crs(4326))

# # import manually cleaned dataset: XX
# df_manualcheck <- read_delim(
#   paste0(data, "/external/processed/Google/", x, "_manualcheck_XX.csv"),
#   delim = ";"
#   )
# df_google <- merge(
#     df_google, 
#     df_mosques_manualcheck %>% select(place_id, drop), 
#     by = "place_id", all.x = TRUE, all.y = FALSE
#     )
# df_google <- df_google %>% filter(is.na(drop)) %>% select(!c(drop))

# # import manually cleaned dataset: XX
# df_manualcheck <- read_delim(
#   paste0(data, "/external/processed/Google/", x, "_manualcheck_XX.csv"),
#   delim = ";"
#   )
# df_google <- merge(
#     df_google, 
#     df_manualcheck %>% select(place_id, drop), 
#     by = "place_id", all.x = TRUE, all.y = FALSE
#     )
# df_google <- df_google %>% filter(is.na(drop)) %>% select(!c(drop))

# write cleaned dataset
saveRDS(df_google, file = paste0(data, "/external/processed/Google/", x, ".rds"))

# timing end
toc()

### compare to data from jonas? ####################################################################

# organized by group the establishments caters to (country of origin)
# what to do with cases not directly assignable? e.g. Iran; Afghanistan

# # groceries
# df_g <- read_rds(paste0(data_jonas, "/groceries.rds"))
# turkish <- c("Türkei")
# arabic <- c("Ägypten", "Algerien", "Libyen", "Marokko", "Tunesien", "Irak", "Jemen", "Jordanien", "Libanon", "Syrien", "Afghanistan")
# df_g <- df_g %>% filter(group %in% c(turkish, arabic)) %>% group_by(ID) %>% slice_head()
# df$guess6c <- sapply(df$wohngebiet_wkt, sum_intersections, points = df_g[c("geometry")])
# table(df %>% filter(wohngebiet_plausible == TRUE) %>% select(guess6c))

# # mosques
# # what does it mean when name ($Moschee) is NA? seems mostly valid after cursory checking
# # lon lat are interchanged in the df!
# df_m <- read_rds(paste0(data_jonas, "/mosques_lists.rds"))
# colnames(df_m) <- c("religion", "country", "adr", "Moschee", "lat", "lon")
# df_m <- df_m %>% select(c("religion", "country", "adr", "Moschee", "lon", "lat"))
# df$guess8c <- sapply(df$wohngebiet_wkt, sum_intersections, points = df_m[c("lon", "lat")])
# table(df %>% filter(wohngebiet_plausible == TRUE) %>% select(guess8c))
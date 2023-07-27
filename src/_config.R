### user management

# get user = username on machine
user = Sys.info()[7]

# paths
paths = list(
  christianhunkler = list(
    wd = "/Users/christianhunkler/Library/CloudStorage/Dropbox/FoDiRa_Arbeit/SyrGit/syr-geo",
    data = "/Users/christianhunkler/Seafile/FoDiRa-SYR/survey/data"
    ),
  claum = list(
    wd = "C:Users/claum/Documents/HU Migration und Geschlecht/FoDiRa/ShowingYourReligion/SYR 22 Datensatz + Code/syr-geo",
    data = "C:/Users/claum/Documents/Seafile/FoDiRa-SYR/survey/data"
    ),
  jmwei = list(
    wd = "C:/Dokumente/Documents/MZES/Survey/syr-geo",
    data = "C:/Users/jmwei/Seafile/FoDiRa-SYR/survey/data"
    ),
  Lenovo = list(
    wd = "C:/Users/Lenovo/Desktop/Arbeit/Datensatz_SYR/syr-geo",
    data = "C:/Users/Lenovo/Seafile/FoDiRa-SYR/survey/data"
    ),
  max = list(
    wd = "/home/max/Seafile/Projects/syr-geo",
    data = "/home/max/Seafile/FoDiRa-SYR/survey/data",
    data_jonas = "/home/max/Seafile/Projects/guestimates/Data"
    )
  )
for (key in names(paths[[user]])) {
  do.call("<-", list(key, unlist(paths[[user]][key])))
}

# viewer options
if (user == "max") {
  options(browser = 'brave-browser')
  }


### functions

# Crosswalk by
# - time within geo: 2020 / 2021 -> 2022 (for 1998 - 2019 -> 2020 use the `ags` package)
# - by geo for 2022: gem / gvb / kre -> postcode / wohngebiet
# Does a single conversion per call, returns the converted df

#
# Allow to do it in one step? Or: just allow to fetch data from what is available?
# 

xwalk <- function(
    df,
    geo, # gem; gvb; kre
    to, # 2022; postcode; wohngebiet
    ars, # column with ars
    arsref = NA, # 2020; 2021; 2022 - only relevant if to == 2022
    years = NA, # only relevant if long data by year WITH THE SAME ARSREF
    weight, # area; pop
    weightref, # abs; rel
    variables # vector of column names
    ) {
  
  # numeric checks
  if (!is.na(as.numeric(to))) to <- as.numeric(to)
  if (!is.na(as.numeric(arsref))) arsref <- as.numeric(arsref)

  # new geo id: ars; postcode; pid x response_multisub_rank
  # set aggregation groups
  if (to == 2022) {
    # by time
    geonew <- paste0(geo, "_ars_", ifelse(nchar(df[1, ars]) == 12, "full_", ""), 2022) # ars vs. ags
    namenew <- paste0(geo, "_name_2022")
    ctable <- paste0(data, "/external/processed/ars/xwalk_", tolower(geo), "_")
    ctable <- paste0(ctable, ifelse(arsref == 2022, 2021, arsref), "_2022.csv")
    groups <- c(geonew, namenew)
  } else {
    # by geo for 2022
    arsref <- 2022
    if (weight == "pop") print("Weighting by pop not possible. Weighting by area instead.")
    weight <- "area" # only area weights possible
    if (to == "wohngebiet") geonew <- c("pid", "response_multisub_rank") else geonew <- "postcode"
    ctable <- paste0(data, "/external/processed/ars/xwalk_", tolower(geo), "_", to, ".csv")
    groups <- geonew
  }
  if (!is.na(years)) groups <- c(groups, years)

  # 2022 to 2022: no xwalk but merge info to have same output when looping over years including 2022
  if (arsref == 2022 & to == 2022) {
    # just merge ars info
    df_xwalk <- read_delim(ctable, delim = ";", show_col_types = FALSE)
    df_xwalk <- df_xwalk %>% distinct(!!sym(geonew), !!sym(namenew))
    df <- merge(df, df_xwalk, by.x = ars, by.y = geonew, all.x = TRUE)
    df <- df %>% rename(!!sym(geonew) := !!sym(ars))
  } else {
    ### xwalk 
    # read correspondence table
    df_xwalk <- read_delim(ctable, delim = ";", show_col_types = FALSE)
    # merge to passed df
    df <- merge(df, df_xwalk, by.x = ars, by.y = paste0(geo, "_ars_", arsref), all.x = TRUE)
    # weight data; rounded values show imprecision!
    w <- paste0(weight, "_w_", weightref)
    df <- df %>%
      mutate(across(where(is.numeric) & any_of(variables), ~ .x * .data[[w]])) %>%
      group_by(pick(all_of(groups))) %>%
      summarize(across(where(is.numeric) & any_of(variables), ~ round(sum(.x, na.rm = TRUE), 3)))
    }
  
  # rename, set ars ref, order columns
  if (to == 2022) {
    df <- df %>%
      mutate(!!sym(paste0(ars, "_ref")) := 2022) %>%
      select(any_of(c(geonew, paste0(ars, "_ref"), groups[!groups %in% geonew])), colnames(.)) %>%
      rename(
        !!sym(ars) := !!sym(geonew),
        !!sym(paste0(ars, "_name")) := !!sym(namenew)
        )
  } else {
    df <- df %>% select(all_of(groups), colnames(.))
  }

  # return
  return(df)
}

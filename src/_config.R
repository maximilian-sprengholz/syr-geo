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

# crosswalk by time 
# 2020 -> 2022 or 2021 -> 2022
# for 1998 - 2019 -> 2020 use the `ags` package
xwalk22 <- function(
    df,
    geo,
    arscol,
    arsref,
    yearcol = NA,
    weight,
    weightref,
    variables
    ) {

  # new ars (check if full ars = 12 digits or ags for gem) and name
  arsnew <- paste0(geo, "_ars_", ifelse(nchar(df[1, arscol]) == 12, "full_", ""), 2022)
  namenew <- paste0(geo, "_name_2022")
  # either xwalk or (if arsref = 2022) merge names and set reference
  if (arsref != 2022) {    
    # read correspondence table
    df_xwalk <- read_delim(
      paste0(data, "/external/processed/ars/xwalk_", tolower(geo), "_", arsref, "_2022.csv"),
      delim = ";",
      show_col_types = FALSE
      )
    # merge to passed df
    df <- merge(df, df_xwalk, by.x = arscol, by.y = paste0(geo, "_ars_", arsref), all.x = TRUE)
    # grouping
    if (!is.na(yearcol)) groups <- c(arsnew, namenew, yearcol) else groups <- c(arsnew, namenew)
    # weight data
    w <- paste0(weight, "_w_", weightref)
    df <- df %>%
      mutate(across(where(is.numeric) & any_of(variables), ~ .x * .data[[w]])) %>%
      group_by_at(groups) %>%
      summarize(across(where(is.numeric) & any_of(variables), ~ sum(.x, na.rm = TRUE))) %>%
      rename(!!sym(arscol) := !!sym(arsnew))
    if (weightref == "abs") {
      df <- df %>% mutate(across(where(is.numeric) & any_of(variables), ~ round(.x))) # round to int
      }
  } else {
    # just merge ars info
    df_xwalk <- read_delim(
      paste0(data, "/external/processed/ars/xwalk_", tolower(geo), "_2021_2022.csv"),
      delim = ";",
      show_col_types = FALSE
      )
    df_xwalk <- df_xwalk %>% distinct(!!sym(arsnew), !!sym(namenew))
    df <- merge(df, df_xwalk, by.x = arscol, by.y = arsnew, all.x = TRUE)
  }
  # rename, set ars ref, order columns
  df <- df %>%
    rename(!!sym(paste0(arscol, "_name")) := !!sym(namenew)) %>%
    mutate(!!sym(paste0(arscol, "_ref")) := 2022) %>%
    select(any_of(groups), colnames(.))
  # return
  return(df)
}

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
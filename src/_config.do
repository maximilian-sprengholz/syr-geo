//
// user management
//

local claudia_wd "C:/Users/claum/Documents/Seafile/FoDiRa-SYR/survey/data"
local claudia_data "C:/Users/claum/Documents/Seafile/FoDiRa-SYR/survey/data"
local max_wd "/home/max/Seafile/Projects/syr-geo"
local max_data "/home/max/Seafile/FoDiRa-SYR/survey/data"

foreach name in claudia max {
    cap cd ``name'_wd' // tries to open the directory
    if (_rc == 170) continue // tries next one if error
        else global user = "`name'" // sets if no error
}

global wd = "`${user}_wd'"
global data = "`${user}_data'"
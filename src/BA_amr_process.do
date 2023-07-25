// config
clear
run "src/_config.do"


//
// data
//

// import ars base file and keep one level per kreis; keep as tempfile
import delimited "$data/external/processed/ars/ars2022.csv", stringcols(_all) clear
keep kre_*
bys kre_ars: gen n = _n
keep if n == 1
drop n
gen kre_ars_ref = 2022
tempfile kreise
save `kreise'

//
// loop over Kreise
//

levelsof kre_ars, local(arslvls)
foreach arslvl in `arslvls' {
    
    //
    // Part 1: Variablen Anzahl Arbeitslose
    //
    
    import excel "$data/external/raw/BA/amr/amr-`arslvl'-0-202211-xlsx.xlsx" ///
        , sheet("Eckwerte") allstring clear
    
    drop (C E-L)
    drop A-B // nur in diesem fall, sonst ist A die relevante Spalte
    keep in 15/26
    rename D arbeitslose
    
    // suffixes for reshape
    cap drop towide
    gen str towide = "_insg"
    replace towide = "_m" if _n == 2
    replace towide = "_f" if _n == 3
    replace towide = "_15_24" if _n == 4
    replace towide = "1_d" if _n == 5
    replace towide = "_50_64" if _n == 6
    replace towide = "2_d" if _n == 7
    replace towide = "3_d" if _n == 8
    replace towide = "4_d" if _n == 9
    replace towide = "5_d" if _n == 10
    replace towide = "6_d" if _n == 11
    replace towide = "_auslaender" if _n == 12
    
    // merge & reshape id
    gen str kre_ars = "`arslvl'"
    
    // reshape
    reshape wide arbeitslose, i(kre_ars) j(towide) string
    drop *_d
    destring arbeitslose*, replace
    
    // save
    tempfile p1
    save `p1' 
    
    //
    // Part 2: 
    //
    
    // Variablen Arbeitslosenquoten
    import excel "$data/external/raw/BA/amr/amr-`arslvl'-0-202211-xlsx.xlsx" ///
        , sheet("Eckwerte") allstring clear
    
    drop (A-C E-L)
    keep in 42/49
    rename D arbeitslosenquote
    cap drop id
    
    // suffixes for reshape
    cap drop towide
    gen str towide = "_insg"
    replace towide = "_m" if _n == 2
    replace towide = "_f" if _n == 3
    replace towide = "_15_24" if _n == 4
    replace towide = "1_d" if _n == 5
    replace towide = "_50_64" if _n == 6
    replace towide = "2_d" if _n == 7
    replace towide = "_auslaender" if _n == 8
    
    // merge & reshape id
    gen str kre_ars = "`arslvl'"
    
    // reshape
    reshape wide arbeitslosenquote, i(kre_ars) j(towide) string
    drop *_d
    destring arbeitslosenquote*, replace
    
    // save
    tempfile p2
    save `p2'
    
    //
    // Part 3: Variable Arbeitsstellen
    //
    
    import excel "$data/external/raw/BA/amr/amr-`arslvl'-0-202211-xlsx.xlsx" ///
        , sheet("Eckwerte") allstring clear

    keep in 64
    drop (A-C E-L)
    rename D arbeitsstellen
    destring arbeitsstellen, replace
    
    // merge id
    gen str kre_ars = "`arslvl'"
    
    // save
    tempfile p3
    save `p3'
    
    //
    // Merge Parts (better to start with part 1 to keep order in dataset)
    //
    use `p1', clear
    forvalues i = 2/3 {
        merge 1:1 kre_ars using `p`i''
        drop _merge
    }
    
    //
    // Variablen Erwerbspersonen
    //
    
    local suffixes insg m f 15_24 50_64 auslaender 
    foreach suffix in `suffixes' {
        gen erwerbspersonen_`suffix' ///
            = round(((arbeitslose_`suffix' * 100) / arbeitslosenquote_`suffix'))
    }
    order erwerb*
    
    //
    // Variablen umbenennen, Labels etc. 
    //

    // do in loop!
    // compound quotes say: leave inner double quotes as is! `" "inner element" "second" "'
    // has the advantage that you can specify an empty string: we do not want "insg"
    local vars arbeitslose arbeitslosenquote erwerbspersonen
    local suffixes insg m f 15_24 50_64 auslaender 
    local suffixnames `" "" "Männer" "Frauen" "Alter 15-24 J" "Alter 50-64 J" "Ausländer" "'
    
    // loop over varname stubs
    foreach var in `vars' {
        // get proper name for variable: in this case, just the name with a capital first letter
        local varname = strproper("`var'")
        // loop over suffixes, but count the current word no. to match it with`suffixnames'
        local suffixcnt = 0
        foreach suffix in `suffixes' {
            // label var
            local ++suffixcnt // for each round in the loop, this count is incremented by 1
            local suffixname : word `suffixcnt' of `suffixnames' // extracts the nth word
            local label = strrtrim("`varname' `suffixname'") // add in case empty
            lab var `var'_`suffix' "`label'"
        }
        // after everything is done, get rid of "insg" suffix
        rename `var'_insg `var'
    }
    
    // Bestand is extra
    lab var arbeitsstellen "Arbeitsstellen Bestand"

    // temporarily save data
    save "$data/external/temp/ba_amr_kre`arslvl'.dta", replace
}

//
// Merge all Kreise together
//

use `kreise', clear
levelsof kre_ars, local(arslvls)
foreach arslvl in `arslvls' {
    merge 1:1 kre_ars using "$data/external/temp/ba_amr_kre`arslvl'.dta", nogen update replace
}
order _all, alphabetic
order kre* erwerb* arbeitslose arbeitslose_* arbeitslosenq*
save `kreise', replace

// write out variable index
ds kre*, not
local vars "`r(varlist)'"
local nvars : word count `vars'
foreach var in `vars' {
    local label : variable label `var'
    local labels `" `labels' "`label'" "'
}
clear
set obs `nvars'
gen str variable = ""
gen str desc = ""
forvalues i = 1/`nvars' {
    local var : word `i' of `vars'
    local label : word `i' of `labels'
    replace variable = "`var'" if _n == `i'
    replace desc = "`label'" if _n == `i'
}
gen str geo = "kre"
gen year = 2022
gen month = 11
gen str source = "BA"
gen str source_detail = "Arbeitsmarktreport"
tempfile varindex
save `varindex'
local file "$data/external/processed/BA/variable_index.csv"
capture confirm file `file'
if (!_rc) {
    import delimited using `file', clear delimiter(";")
    append using `varindex'
    export delimited using `file', delimiter(";") replace quote
}
else {
    export delimited using `file', delimiter(";") replace quote
}

// write new file or merge to existing
// possibly
local file "$data/external/processed/BA/kre.csv"
capture confirm file `file'
if (!_rc) {
    import delimited using `file', clear delimiter(";") stringcols(1)
    merge 1:1 kre_ars using `kreise', nogen update replace
}
else {
    use `kreise', clear
}
export delimited using `file', delimiter(";") replace quote

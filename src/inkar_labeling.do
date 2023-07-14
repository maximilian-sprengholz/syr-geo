// config
clear
run "src/_config.do"

//Merge 2021 Regierungsbezirke:

import delimited "$data/external/raw/INKAR/INKAR_rb_2021.csv", clear

drop raumeinheit aggregat

**Bonusaufgabe: rename + label Variablen
rename kennziffer rb_ars
rename wohnungsnahe_grundversorgung_hau nahe_hausarzt
rename wohnungsnahe_grundversorgung_apo nahe_apotheke
rename wohnungsnahe_grundversorgung_sup nahe_supermarkt
rename erreichbarkeit_von_autobahnen nahe_autobahn
rename erreichbarkeit_von_flughäfen nahe_flug
rename erreichbarkeit_von_icecicebahnhö nahe_ice
rename erreichbarkeit_von_oberzentren nahe_oberzentren
rename erreichbarkeit_von_mittelzentren nahe_mittelzentren
rename nahversorgung_supermärkte_durchs nahe_average_supermarkt 
rename nahversorgung_supermärkte_anteil bev_radius_supermarkt
rename nahversorgung_apotheken_durchsch nahe_average_apotheke
rename nahversorgung_apotheken_anteil_d bev_radius_apotheke
rename nahversorgung_grundschulen_durch nahe_average_grundschule
rename nahversorgung_grundschulen_antei bev_radius_grundschule
rename pkw_elektro_bev 
rename pkw_hybrid_insgesamt
rename pkw_pluginhybrid_phev
rename pkw_benzin
rename pkw_gas
rename pkw_diesel
rename ladepunkte_je_100000_einwohner ladepunkte_einwohner
la var ladepunkte_einwohner "Ladepunkte Elektro-PKW pro 10.000 Einwohner"
rename ladepunkte_je_100_elektrofahrzeu ladepunkte_pkw
la var ladepunkte_pkw "Ladepunkte pro 100 Elektrofahrzeuge"
la var wahlbeteiligung "Wahlbeteiligung in Prozent"
rename stimmenanteile_cducsu stimme_cdu
la var stimme_cdu "Stimmenanteile CDU/CSU in Prozent"
rename stimmenanteile_spd stimme_spd
la var stimme_spd "Stimmenanteile SPD in Prozent"
rename stimmenanteile_grüne stimme_grüne
la var stimme_grüne "Stimmenanteile B90/Grünen in Prozent"
rename stimmenanteile_fdp stimme_fdp
la var stimme_fdp "Stimmenanteile FDP in Prozent"
rename stimmenanteile_sonstige_parteien stimme_sonst
la var stimme_sonst "Stimmenanteile sonstige Parteien in Prozent"
rename stimmenanteile_die_linke stimme_linke
la var stimme_linke "Stimmenanteile Die Linke in Prozent"
rename stimmenanteile_afd stimme_afd
la var stimme_afd "Stimmenanteile AFD in Prozent"

tempfile inkar // damit wir nicht schreiben und spaeter loeschen
save `inkar', replace

**Merge 2021 RB:

use "$data/external/processed/rb_2021.dta", clear

merge 1:1 rb_ars using `inkar'
drop _merge 

save "$data/external/processed/rb_2021.dta", replace

//Merge 2020 Regierungsbezirke

import delimited "$data/external/raw/INKAR/INKAR_rb_2020.csv", clear

drop raumeinheit aggregat

**Bonusaufgabe: rename + label Variablen

rename kennziffer rb_ars
rename schlafgelegenheiten_in_beherberg schlaf_gelegenheit
la var schlaf_gelegenheit "Schlafgelegenheiten in Beherbergungsbetrieben je 1.000 Einwohner"
rename gästeübernachtungen_in_beherberg gaeste_uebernachtung
la var gaeste_uebernachtung "Gästeübernachtungen in Beherbergungsbetrieben je Einwohner"
rename ausländische_gäste_in_beherbergu gaeste_ausland
la var gaeste_ausland "Anteil Gästeübernachtungen aus dem Ausland in %"
rename verweildauer_in_beherbergungsbet gaeste_verweildauer
la var gaeste_verweildauer "Durchschnittliche Zahl der Übernachtungen in Beherbergungsbetrieben"
rename kleinstbetriebe kleinst_bet
la var kleinst_bet "Anteil der Niederlassungen mit bis unter 10 SV-Beschäftigten an den Niederlassungen insgesamt in %"
rename kleinbetriebe klein_bet
la var klein_bet "Anteil der Niederlassungen mit 10 bis unter 50 SV-Beschäftigten an den Niederlassungen insgesamt in %"
rename mittlere_unternehmen mittel_bet
la var mittel_bet "Anteil der Niederlassungen mit 50 bis unter 250 SV-Beschäftigten an den Niederlassungen insgesamt in %"
rename großunternehmen gross_bet
la var gross_bet "Anteil der Niederlassungen mit mehr als 250 SV-Beschäftigten an den Niederlassungen insgesamt in %"
rename bruttoinlandsprodukt_je_einwohne bip_einwohner
la var bip_einwohner "Bruttoinlandsprodukt in 1.000 € je Einwohner"
rename bruttoinlandsprodukt_je_erwerbst bip_erwerb
la var bip_erwerb "Bruttoinlandsprodukt in 1.000 € je Erwerbstätigen"
rename bruttowertschöpfung_je_erwerbstä bws_erwerb
la var bws_erwerb "Bruttowertschöpfung insgesamt in 1.000 € je Erwerbstätigen"
rename v15 bws_primaer
la var bws_primaer "Bruttowertschöpfung im Primären Sektor in 1.000 € je Erwerbstätigen im Primären Sektor"
rename v16 bws_sekundaer
la var bws_sekundaer "Bruttowertschöpfung im Sekundären Sektor in 1.000 € je Erwerbstätigen im Sekundären Sektor"
rename v17 bws_tertiaer
la var bws_tertiaer "Bruttowertschöpfung im Tertiären Sektor in 1.000 € je Erwerbstätigen im Tertiären Sektor"
rename anteil_bruttowertschöpfung_primä anteil_bws_primaer
la var anteil_bws_primaer "Anteil Bruttowertschöpfung im Primären Sektor an der Bruttowertschöpfung in %"
rename anteil_bruttowertschöpfung_sekun anteil_bws_sekundaer
la var anteil_bws_sekundaer "Anteil Bruttowertschöpfung im Sekundären Sektor an der Bruttowertschöpfung in %"
rename anteil_bruttowertschöpfung_terti anteil_bws_tertiaer
la var anteil_bws_tertiaer "Anteil Bruttowertschöpfung im Tertiären Sektor an der Bruttowertschöpfung in %"
rename unternehmensinsolvenzen insolvenz_bet
la var insolvenz_bet "Beantragte Unternehmensinsolvenzverfahren je 1.000 Betriebe"
rename unternehmensinsolvenzen_gläubige 




**Do-file in Bearbeitung**




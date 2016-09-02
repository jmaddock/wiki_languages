// stata batch command: StataSE -b do filename
//setup variables -- only needs to run once. ignore - already done w/ .dta file
// pass .csv file as first command line arg
// pass output .dta file as second command line arg

// import file
import delimited using `1'
// get language tabs
tabulate lang
// encode string variable to categorical
encode lang, generate(lang2) 
// save file
save `2', replace

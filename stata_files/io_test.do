// stata batch command: StataSE -b do filename
// import files from command line
// pass .dta file as first command line argument
// pass output dir as second command line argument

use `1'
local output_dir = "`2'"

// change time column to years
gen talk_age_year = tds_1 / (60 * 60 * 24 * 365)
gen article_age_year = tds_0 / (60 * 60 * 24 * 365)

// create basic model using only talk vars
// drop single outlier for en
nbreg len_1 num_editors_1 talk_age_year if len_1 < 50000

// output the coeficient estimates with standard errors and confidence intervals
matrix output = r(table)
local output_path = "`output_dir'" + "talk_only_model.tsv"
estout m(output) using `output_path', replace label

// get the coeficient percent estimates and store them to a matrix
listcoef
matrix output = r(table)
listcoef, percent
matrix output = output,r(table)
local output_path = "`output_dir'" + "talk_only_model_percents.tsv"
estout m(output) using `output_path', replace label

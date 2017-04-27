// StataSE -b do regression_table.do <input.dta> <output.tex>
// add language -> ib5.language for expanded dummy vars

import delimited using `1'
local output_dir = "`2'"

// change time column to years
gen talk_age = tds_1 / (60 * 60 * 24 * 365)
gen article_age = tds_0 / (60 * 60 * 24 * 365)

// get language tabs
tabulate lang
// encode string variable to categorical
encode lang, generate(lang2)

// generate log vars
gen log_no_revert_len_0 = ln(no_revert_len_0)
gen log_len_1 = ln(len_1)

// create full model using talk, article, and language vars
// drop single outlier for en

eststo: nbreg reverts_0 log_len_1 talk_age_year log_no_revert_len_0 article_age_year if len_1 < 50000, robust
eststo: nbreg reverts_0 log_len_1 talk_age_year log_no_revert_len_0 article_age_year ib5.lang2 if len_1 < 50000, robust

// output the coeficient estimates with standard errors and confidence intervals
local output_path = "`output_dir'"
esttab using `output_path', replace label

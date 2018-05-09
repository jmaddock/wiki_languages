// StataSE -b do regression_table.do <input.dta> <output.tex>
// add language -> ib5.language for expanded dummy vars

import delimited using `1'
local output_dir = "`2'"

// change time column to years
gen talk_age_year = tds_1 / (60 * 60 * 24 * 365)
gen article_age_year = tds_0 / (60 * 60 * 24 * 365)

// create all models
// drop single outlier for en
eststo: nbreg len_1 num_editors_1 talk_age_year len_0 article_age_year ib2.lang2 if len_1 < 50000, robust
eststo: nbreg len_1 num_editors_1 talk_age_year len_0 article_age_year ib2.lang2##ib(freq).en_category_title2 if len_1 < 50000, robust

// output the coeficient estimates with standard errors and confidence intervals
local output_path = "`output_dir'"
esttab using `output_path', replace label

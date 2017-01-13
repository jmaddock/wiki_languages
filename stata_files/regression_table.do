// StataSE -b do regression_table.do <input.dta> <output.tex>
// add language -> ib5.language for expanded dummy vars

use `1'
local output_dir = "`2'"

// change time column to years
gen talk_age = tds_1 / (60 * 60 * 24 * 365)
gen article_age = tds_0 / (60 * 60 * 24 * 365)

// rename variables
rename len_1 talk_edits
rename len_0 article_edits
rename num_editors_1 talk_authors
rename num_editors_0 article_authors
rename lang2 language

// create all models
// drop single outlier for en
eststo: nbreg talk_edits talk_authors talk_age article_edits article_age if talk_edits < 50000, robust
eststo: nbreg talk_edits talk_authors talk_age article_edits article_age ib5.language if talk_edits < 50000, robust

// output the coeficient estimates with standard errors and confidence intervals
local output_path = "`output_dir'"
esttab using `output_path', replace label

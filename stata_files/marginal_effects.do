// stata batch command: StataSE -b do filename
// import files from command line
// pass .dta file as first command line argument
// pass output dir as second command line argument

use `1'
local output_dir = "`2'"

// change time column to years
gen talk_age_year = tds_1 / (60 * 60 * 24 * 365)
gen article_age_year = tds_0 / (60 * 60 * 24 * 365)

nbreg len_1 num_editors_1 talk_age_year len_0 article_age_year ib5.lang2 if len_1 < 50000, robust

margins, at((p50) num_editors_1 (p50) talk_age_year (p50) len_0 (p50) article_age_year lang2=(5,7,10,17))
matrix output = r(table)

margins, at((p75) num_editors_1 (p75) talk_age_year (p75) len_0 (p57) article_age_year lang2=(5,7,10,17))
matrix output = (output \ r(table))

margins, at((p90) num_editors_1 (p90) talk_age_year (p90) len_0 (p90) article_age_year lang2=(5,7,10,17))
matrix output = (output \ r(table))

margins, at((p95) num_editors_1 (p95) talk_age_year (p95) len_0 (p95) article_age_year lang2=(5,7,10,17))
matrix output = (output \ r(table))

margins, at((p97) num_editors_1 (p97) talk_age_year (p97) len_0 (p97) article_age_year lang2=(5,7,10,17))
matrix output = (output \ r(table))

margins, at((p99) num_editors_1 (p99) talk_age_year (p99) len_0 (p99) article_age_year lang2=(5,7,10,17))
matrix output = (output \ r(table))

estout m(output) using `2', replace

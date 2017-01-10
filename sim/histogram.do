import delimited using `1'
local output_dir = "`2'"

drop unnamed0 model_id

foreach var of varlist * {
histogram `var'
local output_path = "`output_dir'" + "`var'" + ".pdf"
graph export `output_path', replace
}

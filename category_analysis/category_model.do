// StataSE -b do model_simulated_data.do <input.dta> <path_to_output_dir/file_prefix>
// import files from command line
// pass .dta file as first command line argument
//use "/Users/klogg/research_data/wiki_language_data/stata_files/combined_edit_ratios_no_title_drop1.dta"
import delimited using `1'
local output_dir = "`2'"

// change time column to years
gen talk_age_year = tds_1 / (60 * 60 * 24 * 365)
gen article_age_year = tds_0 / (60 * 60 * 24 * 365)

// get language tabs
tabulate lang
// encode string variable to categorical
encode lang, generate(lang2)

// get category tabs
tabulate en_category_title
// encode string variable to categorical
encode en_category_title, generate(en_category_title2)

// create full model using talk, article, and language vars
// drop single outlier for en
nbreg len_1 num_editors_1 talk_age_year len_0 article_age_year ib(freq).lang2 ib(freq).en_category_title2 if len_1 < 50000, robust

// output the coeficient estimates with standard errors and confidence intervals
matrix output = r(table)
local output_path = "`output_dir'" + "talk_article_lang_model.tsv"
estout m(output) using `output_path', replace label

// get the language labels
eststo
local output_path = "`output_dir'" + "talk_article_lang_model_lablels.tsv"
estout using `output_path', replace label

// get the coeficient percent estimates and store them to a matrix
listcoef
matrix output = r(table)
listcoef, percent
matrix output = output,r(table)
local output_path = "`output_dir'" + "talk_article_lang_model_percents.tsv"
estout m(output) using `output_path', replace label

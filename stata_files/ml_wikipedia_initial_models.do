// import files
use "/Users/klogg/research_data/wiki_language_data/stata_files/combined_edit_ratios_no_title_drop1.dta"

// change time column to years
gen talk_age_year = tds_1 / (60 * 60 * 24 * 365)
gen article_age_year = tds_0 / (60 * 60 * 24 * 365)

// create basic model using only talk vars
// drop single outlier for en
nbreg len_1 num_editors_1 talk_age_year if len_1 < 50000

// output the coeficient estimates with standard errors and confidence intervals
matrix output = r(table)
estout m(output) using ~/research_data/wiki_language_data/stata_files/talk_only_model.tsv, label

// get the coeficient percent estimates and store them to a matrix
listcoef
matrix output = r(table)
listcoef, percent
matrix output = output,r(table)
estout m(output) using ~/research_data/wiki_language_data/stata_files/talk_only_model_percents.tsv, label

// create second model using talk and article vars
// drop single outlier for en
nbreg len_1 num_editors_1 talk_age_year len_0 article_age_year if len_1 < 50000

// output the coeficient estimates with standard errors and confidence intervals
matrix output = r(table)
estout m(output) using ~/research_data/wiki_language_data/stata_files/talk_article_model.tsv, label

// get the coeficient percent estimates and store them to a matrix1
listcoef
matrix output = r(table)
listcoef, percent
matrix output = output,r(table)
estout m(output) using ~/research_data/wiki_language_data/stata_files/talk_article_model_percents.tsv, label

// create full model using talk, article, and language vars
// drop single outlier for en
nbreg len_1 num_editors_1 talk_age_year len_0 article_age_year ib5.lang2 if len_1 < 50000

// output the coeficient estimates with standard errors and confidence intervals
matrix output = r(table)
estout m(output) using ~/research_data/wiki_language_data/stata_files/talk_article_lang_model.tsv, label

// get the language labels
eststo
estout using ~/research_data/wiki_language_data/stata_files/talk_article_lang_model_lablels.tsv, label

// get the coeficient percent estimates and store them to a matrix
listcoef
matrix output = r(table)
listcoef, percent
matrix output = output,r(table)
estout m(output) using ~/research_data/wiki_language_data/stata_files/talk_article_lang_model_percents.tsv, label
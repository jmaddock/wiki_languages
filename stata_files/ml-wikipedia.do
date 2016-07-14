//setup variables -- only needs to run once. ignore - already done w/ .dta file
//tabulate lang //get language tabs
//encode lang, generate(lang2) //encode string variable to categorical


//////
//
// FILE I/O 
//
//////
use "/Users/klogg/research_data/wiki_language_data/stata_files/combined_edit_ratios_no_title_drop1.dta"


//////
//
// NEGATIVE BINOMIAL MODELS
//
//////

//simple negative binomial model without language
nbreg len_1 num_editors_1 if len_1 < 50000 //use len_1 restriction to restric outlier  if to restrict outlier

// negative binomial model with language variable included
nbreg len_1 num_editors_1 i.lang2 if len_1 < 50000, baselevels //or use allbaselevels if you have interactions and want to see everything

// USE ME
// neg binomial w/ EN (baselevel #5) as the excluded category
//
nbreg len_1 num_editors_1 ib5.lang2 if len_1 < 50000, baselevels //allbaselevels


//////
//
// ZERO TRUNCATED MODELS
//
// WARNING: Will take very long time to converge (if so). 
// ll(0) is the truncation level, here we use ll(1) since we only have 2+
//
//////
tnbreg len_1 num_editors_1 ib5.lang2 if len_1 < 50000, ll(1)


//////
//
// Do plotting and post-estimation here
//
//////

// use Long & Freese postestimation commands, they are better

//plot CIs
coefplot, drop(_cons num_editors_1) //simple plot of coeff CIs

//calculate margins - very simple dydx -- also look at Freese et al. for better postestimation commands
//this will be very slow, use nose to keep from estimating SEs
//margins, dydx(*) atmeans
margins, dydx(*) nose
marginsplot, horizontal xline(0) yscale(reverse) recast(scatter)


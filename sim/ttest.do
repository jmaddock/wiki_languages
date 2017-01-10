import delimited using `1'
local output_dir = "`2'"

drop unnamed0 model_id

ttest len_11lang2=.1609383
matrix output = (r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_12lang2=.0671202
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_13lang2=.4359249
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_14lang2=.3176441
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_16lang2=.0628584
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_17lang2=.482562
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_18lang2=.4355494
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_19lang2=-.066614
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_110lang2=.5226168
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_111lang2=.1534393
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_112lang2=.4592675
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_113lang2=.1323114
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_114lang2=.2914342
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_115lang2=.2404746
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_116lang2=-.0342542
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_117lang2=-.2649063
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_118lang2=.2061375
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_119lang2=.3641495
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_120lang2=.319574
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_121lang2=-.1123513
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_122lang2=.0149884
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_123lang2=.2732585
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))
ttest len_124lang2=.0796922
matrix output = (output \ r(level),r(sd_1),r(se),r(p_u),r(p_l),r(p),r(t),r(df_t),r(mu_1),r(N_1))

matrix colnames output = level standard_deviation standard_error upper_p_value lower_p_value p_value t_statistic degrees_of_freedom population_mean sample_size

estout m(output) using `2', replace

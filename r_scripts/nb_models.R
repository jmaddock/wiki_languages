require(foreign)
require(ggplot2)
require(MASS)
require(quantreg)

## read csv from file
args <- commandArgs(trailingOnly = TRUE)
print(args[1])
dat <- read.csv(args[1])

## factor language varriable into dummies
dat <- within(dat, lang <- factor(lang))
## choose which dummy to use as reference
dat <- within(dat, lang <- relevel(lang, ref = 'de'))

## negative binomial models
## basic model with only talk level vars
print("*** negative binomial models ***")
print("base model")
summary(nb1) <- glm.nb(len_1 ~ num_editors_1 + tds_1, data = dat, maxit = 100)
## added article controls
print("## article level controls (number of editors)")
summary(nb2) <- glm.nb(len_1 ~ tds_1 + num_editors_1 + num_editors_0 + tds_1, data = dat, maxit = 100)
print("## article level controls (number of edits)")
summary(nb3) <- glm.nb(len_1 ~ tds_1 + num_editors_1 + len_0 + tds_1, data = dat, maxit = 100)
print("## article level controls (all)")
summary(nb4) <- glm.nb(len_1 ~ tds_1 + num_editors_1 + len_0 + num_editors_0 + tds_1, data = dat, maxit = 100)
## added langs
print("## full model (number of editors)")
summary(nb5) <- glm.nb(len_1 ~ tds_1 + num_editors_1 + num_editors_0 + tds_1 + lang, data = dat, maxit = 100)
print("## full model (number of edits)")
summary(nb6) <- glm.nb(len_1 ~ tds_1 + num_editors_1 + len_0 + tds_1 + lang, data = dat, maxit = 100)
print("## full model (all)")
summary(nb7) <- glm.nb(len_1 ~ tds_1 + num_editors_1 + len_0 + num_editors_0 + tds_1 + lang, data = dat, maxit = 100)
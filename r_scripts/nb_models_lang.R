require(foreign)
require(MASS)

## useage editors, edits, full
## read csv from file
args <- commandArgs(trailingOnly = TRUE)
print(args[1])
dat <- read.csv(args[1])

## factor language varriable into dummy
dat <- within(dat, lang <- factor(lang))
print("*** negative binomial models ***")

## choose which dummy to use as reference
dat <- within(dat, lang <- relevel(lang, ref = 'de'))    
## added langs
if ("editors" %in% args) {
    print("## full model (number of editors)")
    print(summary(nb5 <- glm.nb(len_1 ~ tds_1 + num_editors_1 + num_editors_0 + tds_0 + lang, data = dat, maxit = 100)))
}
if ("edits" %in% args) {
    print("## full model (number of edits)")
    print(summary(nb6 <- glm.nb(len_1 ~ tds_1 + num_editors_1 + len_0 + tds_0 + lang, data = dat, maxit = 100)))
}

if ("all" %in% args) {
    print("## full model (all)")
    print(summary(nb7 <- glm.nb(len_1 ~ tds_1 + num_editors_1 + len_0 + num_editors_0 + tds_0 + lang, data = dat, maxit = 100)))
}

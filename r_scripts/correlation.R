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
dat <- within(dat, lang <- relevel(lang, ref = "de"))

# get correlation matrix
cor_header <- c("tds_1","num_editors_1","len_0","num_editors_0","tds_0")
cor(dat[cor_header])


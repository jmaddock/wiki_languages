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

# get correlation matrix
cor_header<- c('len_1','tds_1','num_editors_1','len_0','num_editors_0','tds_1')
cor(dat[cor_header])

# plot districutions
#ggplot(dat, aes(len_1, fill = lang)) +
#    geom_histogram(binwidth=1) +
#    facet_grid(lang ~ ., margins=TRUE, scales="free")

# for each language show mean and sd
#with(dat, tapply(len_1, lang, function(x) {
#  sprintf("M (SD) = %1.2f (%1.2f)", mean(x), sd(x))
#}))

## negative binomial models
## basic model with only talk level vars
print('*** negative binomial models ***')
nb1 <- glm.nb(len_1 ~ num_editors_1 + tds_1, data = dat, maxit = 100)
## added article controls
nb2 <- glm.nb(len_1 ~ tds_1 + num_editors_1 + len_0 + num_editors_0 + tds_1, data = dat, maxit = 100)
## added langs
nb3 <- glm.nb(len_1 ~ tds_1 + num_editors_1 + len_0 + num_editors_0 + tds_1 + lang, data = dat, maxit = 100)

## anova basic and article models
anova(nb1,nb2,nb3)
## anova article and lang models
anova(nb2,nb3)

print('*** liniar models ***')
## basic model with only talk level vars
summary(lm1 <- lm(len_1 ~ num_editors_1 + tds_1, data = dat))
## added article controls
summary(lm2 <- lm(len_1 ~ tds_1 + num_editors_1 + len_0 + num_editors_0 + tds_1, data = dat))
## added langs
summary(lm3 <- lm(len_1 ~ tds_1 + num_editors_1 + len_0 + num_editors_0 + tds_1 + lang, data = dat))

anova(lm1,lm2)
anova(lm2,lm3)

print('*** quantile models ***')
## basic model with only talk level vars
summary(rq1 <- rq(len_1 ~ num_editors_1 + tds_1, data = dat, tau = .5))
## added article controls
summary(rq2 <- rq(len_1 ~ tds_1 + num_editors_1 + len_0 + num_editors_0 + tds_1, data = dat, tau = .5))
## added langs
summary(rq3 <- rq(len_1 ~ tds_1 + num_editors_1 + len_0 + num_editors_0 + tds_1 + lang, data = dat, tau = .5))

anova(rq1,rq2)
anova(rq2,rq3)

## TODO: CONSTRUCT MODELS USING LINEAR REGRESSION AND QUANTILE REGRESSION
## TODO: QQ PLOTS



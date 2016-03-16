require(foreign)
require(ggplot2)
require(MASS)
require(quantreg)

# read csv from file
dat = read.csv('../db/combined/combined_merged_edit_counts_1000.csv')
# factor language varriable into dummies
dat <- within(dat, lang <- factor(lang))
# choose which dummy to use as reference
dat <- within(dat, lang <- relevel(lang, ref = 'de'))
# plot districutions
#ggplot(dat, aes(len_1, fill = lang)) +
#    geom_histogram(binwidth=1) +
#    facet_grid(lang ~ ., margins=TRUE, scales="free")

# for each language show mean and sd
with(dat, tapply(len_1, lang, function(x) {
  sprintf("M (SD) = %1.2f (%1.2f)", mean(x), sd(x))
}))

# create models
#summary(m1 <- glm.nb(len_1 ~ num_editors_1 + tds_1 + lang, data = dat))
#summary(m2 <- glm.nb(len_1 ~ num_editors_1 + tds_1, data = dat))
summary(nb5 <- glm.nb(len_1 ~ tds_1 + len_0, data = dat))
summary(nb4 <- glm.nb(len_1 ~ tds_1 + len_0 + num_editors_0, data = dat))
summary(nb3 <- glm.nb(len_1 ~ tds_1 + lang + len_0 + num_editors_0, data = dat))

anova(nb5,nb4)
anova(nb5,nb3)

summary(lm5 <- lm(len_1 ~ tds_1 + len_0, data = dat))
summary(lm4 <- lm(len_1 ~ tds_1 + len_0 + num_editors_0, data = dat))
summary(lm3 <- lm(len_1 ~ tds_1 + lang + len_0 + num_editors_0, data = dat))

anova(lm5,lm4)
anova(lm5,lm3)

summary(qr5 <- rq(len_1 ~ tds_1 + len_0, data=dat, tau = 0.5, method = 'pfn'))
summary(qr4 <- rq(len_1 ~ tds_1 + len_0 + num_editors_0, data=dat, tau = 0.5, method = 'pfn'))
summary(qr3 <- rq(len_1 ~ tds_1 + len_0 + lang + num_editors_0, data=dat, tau = 0.5, method = 'pfn'))

## TODO: CONSTRUCT MODELS USING LINEAR REGRESSION AND QUANTILE REGRESSION
## TODO: QQ PLOTS



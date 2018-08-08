# Glasses.com N52W Demand Forecast

# 01 - Load Packages, Global Variables & Options --------------------------

ptm <- proc.time()

rm(list = ls(all.names = TRUE))

options(scipen = 999)

options(googleAuthR.scopes.selected =
          "https://www.googleapis.com/auth/cloud-platform")

project <- "ecomm-197702"
zone <- "us-east4-a"
account_key <- "gcs-key.json"
bucket <- "ecomm_lux"

Sys.setenv(
  GCS_AUTH_FILE = account_key,
  GCS_DEFAULT_PROJECT_ID = project,
  GCS_DEFAULT_ZONE = zone,
  GCS_DEFAULT_BUCKET = bucket
)

library(tidyverse)
library(readxl)
library(bigrquery)
library(googleCloudStorageR)
library(lubridate)
library(stringr)

gcs_auth()

# 02 - Import Data --------------------------------------------------------

gcs_get_object("dpm_hist_fcst.csv",
               saveToDisk = "dpm_hist_fcst.csv",
               overwrite = TRUE)

l52w_sls_dpm <- read_csv(
  "dpm_hist_fcst.csv"#,
#  col_types = cols(
#    `Comm Unconsumed Fcst` = col_number(),
#    `Stat Unconsumed Fcst` = col_number()
#  ),
#  locale = locale(encoding = "ISO-8859-1")
)

sql <- "SELECT   f_yr_week, id
FROM     [ecomm-197702:ecomm.time_dim]
GROUP BY f_yr_week, id"

hist_wk <- query_exec(sql, project = project)


# 03 - Transform Data -----------------------------------------------------

l52w_sls_dpm <- l52w_sls_dpm %>%
  select(-X1)

names(l52w_sls_dpm)[1] <- "DPT"
names(l52w_sls_dpm)[2] <- "F_YR"
names(l52w_sls_dpm)[3] <- "F_WK"
names(l52w_sls_dpm)[4] <- "CL_NME"
names(l52w_sls_dpm)[5] <- "SKU_NBR"
names(l52w_sls_dpm)[6] <- "SLS_U"
names(l52w_sls_dpm)[7] <- "FCST_DPM"

names(hist_wk)[1] <- "F_YR_WK"
names(hist_wk)[2] <- "HIST_WK"

l52w_sls_dpm$F_WK <- str_pad(l52w_sls_dpm$F_WK,
                             width = 2,
                             side = "left",
                             pad = "0")

l52w_sls_dpm <- transform(l52w_sls_dpm, F_YR_WK = paste0(F_YR, F_WK))

l52w_sls_dpm$F_YR <- as.numeric(as.character(l52w_sls_dpm$F_YR))
l52w_sls_dpm$F_WK <- as.numeric(as.character(l52w_sls_dpm$F_WK))
l52w_sls_dpm$F_YR_WK <-
  as.numeric(as.character(l52w_sls_dpm$F_YR_WK))
l52w_sls_dpm$SKU_NBR <-
  as.numeric(as.character(l52w_sls_dpm$SKU_NBR))

l52w_sls <- l52w_sls_dpm %>%
  select(F_YR, F_WK, CL_NME, DPT, SKU_NBR, SLS_U, F_YR_WK)

l52w_sls <- left_join(l52w_sls, hist_wk, by = "F_YR_WK")

l52w_sls <- l52w_sls %>%
  filter(HIST_WK <= 52 & HIST_WK >= 1)

dpm_fcst <- l52w_sls_dpm %>%
  select(F_YR, F_WK, CL_NME, DPT, SKU_NBR, FCST_DPM, F_YR_WK)

dpm_fcst <- left_join(dpm_fcst, hist_wk, by = "F_YR_WK")

dpm_fcst <- dpm_fcst %>%
  filter(is.na(HIST_WK))

l52w_sls_select <-
  select(l52w_sls, F_YR, F_WK, CL_NME, DPT, SKU_NBR,
         SLS_U, F_YR_WK)

# Filter new variable to not show negative sales
#l52w_sls_select <- filter(l52w_sls_select, l52w_sls_select$SLS_U > 0)

# Create new variable summarising sales units by DPT/CL/WK
l52w_sls_select_class <- l52w_sls_select %>%
  group_by(DPT, CL_NME, F_WK) %>%
  summarise(sum(SLS_U))

# Create new column for DPT/CL
l52w_sls_select_class <- transform(l52w_sls_select_class, DPT_CL =
                                     paste0(DPT, CL_NME))

# Create new variable summarising sales units by DPT/CL
total_class_sls <- l52w_sls_select_class %>%
  group_by(DPT_CL) %>%
  summarise(sum(sum.SLS_U.))

# Join last two variables to bring in "total_class_sls" by "DPT_CL"
l52w_sls_select_class <-
  left_join(l52w_sls_select_class, total_class_sls,
            by = "DPT_CL")

# Calculate INDEX for each DPT/CL
l52w_sls_select_class <- mutate(
  l52w_sls_select_class,
  INDEX =
    l52w_sls_select_class$sum.SLS_U. /
    ((1 / 52) *
       l52w_sls_select_class$`sum(sum.SLS_U.)`)
)

# Rename summary sum columns
l52w_sls_select_class <- rename(l52w_sls_select_class,
                                SUM_SLS = sum.SLS_U.,
                                SUM_TTL_SLS = `sum(sum.SLS_U.)`)

# Assign Historical Week # to Fiscal Week #'s
hist_wks <- l52w_sls %>%
  select(F_YR_WK) %>%
  distinct(F_YR_WK)

h_wk_vector <- c(52:1)
h_wk_vector <- as.vector(h_wk_vector)

hist_wks$HIST_WK <- h_wk_vector

l52w_sls_select <-
  left_join(l52w_sls_select, hist_wks, by = "F_YR_WK")

# Filter variable to only show last 17 weeks of sales
l17w_sls <- filter(l52w_sls_select,
                   l52w_sls_select$HIST_WK < 18)

# Summarise sales units by SKU
l17w_sls <- l17w_sls %>%
  group_by(F_YR_WK, F_YR, F_WK, CL_NME, DPT, SKU_NBR) %>%
  summarise(sum(SLS_U))

# Create column combining DPT/CL/WK
l52w_sls_select_class <-
  transform(l52w_sls_select_class, DPT_CL_WK =
              paste0(DPT_CL, F_WK))

# Create variable containing INDEX by DPT/CL/WK
index <- select(l52w_sls_select_class, DPT_CL_WK, INDEX)

# Create column combining DPT/CL/WK
l17w_sls <-
  transform(l17w_sls, DPT_CL_WK = paste0(DPT, CL_NME, F_WK))

# Lookup "INDEX" into l17w_sls variable from index variable
l17w_sls <- left_join(l17w_sls, index,
                      by = "DPT_CL_WK")

# Sort in ascending order
l17w_sls <- arrange(l17w_sls, DPT, CL_NME, SKU_NBR, F_YR, F_WK)

# Add extra F_WK Column
l17w_sls <- l17w_sls %>%
  mutate(F_YR_WK_2 = F_YR_WK + 1000000)

# Filter l13w_sls by NA
l17w_sls <- l17w_sls %>%
  filter(!is.na(SKU_NBR))

l17w_sls_cast <-
  l17w_sls %>% select(F_YR_WK, SKU_NBR, sum.SLS_U.) %>%
  spread(F_YR_WK, sum.SLS_U., fill = 0)

l17w_sls_cast_2 <-
  l17w_sls %>% select(F_YR_WK_2, SKU_NBR, INDEX) %>%
  spread(F_YR_WK_2, INDEX, fill = 0.1)

# Bind data frames and remove duplicate columns

l17w_sls_cast <- bind_cols(l17w_sls_cast, l17w_sls_cast_2)
l17w_sls_cast <-
  l17w_sls_cast[,!duplicated(colnames(l17w_sls_cast))]

l17w_sls_cast <- l17w_sls_cast %>%
  select(-SKU_NBR1)

# Rename column headers

names(l17w_sls_cast)[2:18] <-
  c(
    "SLS_1",
    "SLS_2",
    "SLS_3",
    "SLS_4",
    "SLS_5",
    "SLS_6",
    "SLS_7",
    "SLS_8",
    "SLS_9",
    "SLS_10",
    "SLS_11",
    "SLS_12",
    "SLS_13",
    "SLS_14",
    "SLS_15",
    "SLS_16",
    "SLS_17"
  )

names(l17w_sls_cast)[19:35] <-
  c(
    "IND_1",
    "IND_2",
    "IND_3",
    "IND_4",
    "IND_5",
    "IND_6",
    "IND_7",
    "IND_8",
    "IND_9",
    "IND_10",
    "IND_11",
    "IND_12",
    "IND_13",
    "IND_14",
    "IND_15",
    "IND_16",
    "IND_17"
  )

# 04 - Model --------------------------------------------------------------

# Holt Winters Damped Trend Multiplicative Seasonlity Model

# Train & Optimize Model

iter_df <- l17w_sls_cast[FALSE, ]
new_df <- data.frame()

hwdtms <- function(a = 0.1,
                   g = 0,
                   p = 0) {
  lvl_1 <- iter_df$SLS_1 / iter_df$IND_1
  trnd_1 <- 0
  lvl_2 <- a * iter_df$SLS_2 / iter_df$IND_2 +
    (1 - a) * (lvl_1 + trnd_1)
  trnd_2 <- g * (lvl_2 - lvl_1) + (1 - g) * trnd_1
  lvl_3 <- a * iter_df$SLS_3 / iter_df$IND_3 +
    (1 - a) * (lvl_2 + trnd_2)
  trnd_3 <- g * (lvl_3 - lvl_2) + (1 - g) * trnd_2
  lvl_4 <- a * iter_df$SLS_4 / iter_df$IND_4 +
    (1 - a) * (lvl_3 + trnd_3)
  trnd_4 <- g * (lvl_4 - lvl_3) + (1 - g) * trnd_3
  lvl_5 <- a * iter_df$SLS_5 / iter_df$IND_5 +
    (1 - a) * (lvl_4 + trnd_4)
  trnd_5 <- g * (lvl_5 - lvl_4) + (1 - g) * trnd_4
  lvl_6 <- a * iter_df$SLS_6 / iter_df$IND_6 +
    (1 - a) * (lvl_5 + trnd_5)
  trnd_6 <- g * (lvl_6 - lvl_5) + (1 - g) * trnd_5
  lvl_7 <- a * iter_df$SLS_7 / iter_df$IND_7 +
    (1 - a) * (lvl_6 + trnd_6)
  trnd_7 <- g * (lvl_7 - lvl_6) + (1 - g) * trnd_6
  lvl_8 <- a * iter_df$SLS_8 / iter_df$IND_8 +
    (1 - a) * (lvl_7 + trnd_7)
  trnd_8 <- g * (lvl_8 - lvl_7) + (1 - g) * trnd_7
  lvl_9 <- a * iter_df$SLS_9 / iter_df$IND_9 +
    (1 - a) * (lvl_8 + trnd_8)
  trnd_9 <- g * (lvl_9 - lvl_8) + (1 - g) * trnd_8
  lvl_10 <- a * iter_df$SLS_10 / iter_df$IND_10 +
    (1 - a) * (lvl_9 + trnd_9)
  trnd_10 <- g * (lvl_10 - lvl_9) + (1 - g) * trnd_9
  lvl_11 <- a * iter_df$SLS_11 / iter_df$IND_11 +
    (1 - a) * (lvl_10 + trnd_10)
  trnd_11 <- g * (lvl_11 - lvl_10) + (1 - g) * trnd_10
  lvl_12 <- a * iter_df$SLS_12 / iter_df$IND_12 +
    (1 - a) * (lvl_11 + trnd_11)
  trnd_12 <- g * (lvl_12 - lvl_11) + (1 - g) * trnd_11
  lvl_13 <- a * iter_df$SLS_13 / iter_df$IND_13 +
    (1 - a) * (lvl_12 + trnd_12)
  trnd_13 <- g * (lvl_13 - lvl_12) + (1 - g) * trnd_12
  trnd_14 <- trnd_13
  trnd_15 <- trnd_13
  trnd_16 <- trnd_13
  trnd_17 <- trnd_13
  lvl_14 <- lvl_13 + p * trnd_14
  lvl_15 <- lvl_13 + (p + p ^ 2) * trnd_15
  lvl_16 <- lvl_13 + (p + p ^ 2 + p ^ 3) * trnd_16
  lvl_17 <- lvl_13 + (p + p ^ 2 + p ^ 3 + p ^ 4) * trnd_17
  ftest_1 <- lvl_14 * iter_df$IND_14
  ftest_2 <- lvl_15 * iter_df$IND_15
  ftest_3 <- lvl_16 * iter_df$IND_16
  ftest_4 <- lvl_17 * iter_df$IND_17
  ftest_sum <- ftest_1 + ftest_2 + ftest_3 + ftest_4
  act_sum <- iter_df$SLS_14 + iter_df$SLS_15 +
    iter_df$SLS_16 + iter_df$SLS_17
  abs_e <- abs(ftest_sum - act_sum)
  sign_error <- (ftest_sum - act_sum)
  ape <- abs_e / act_sum * 100
  wape <- ape * act_sum
  ape[is.infinite(ape)] <- 0
  wape[is.nan(wape)] <- 0
  wmape <- sum(wape) / sum(act_sum)
  bias <- sum(sign_error) / sum(act_sum)
  return(abs_e)
}

# Optimize Model

for (i in 1:nrow(l17w_sls_cast)) {
  iter_df <- l17w_sls_cast[i, ]
  parameters <- optim(
    c(0.1),
    hwdtms,
    method = "L-BFGS-B",
    lower = c(0.1),
    upper = c(0.75)
  )
  iter_df$ALPHA <- parameters$par[1]
  iter_df$GAMMA <- parameters$par[2]
  iter_df$PHI <- parameters$par[3]
  iter_df$ABS_E <- parameters$value[1]
  new_df <- rbind(new_df, iter_df)
  print(i)
}

new_df$ALPHA <- round(new_df$ALPHA, digits = 3)
new_df$GAMMA <- round(new_df$GAMMA, digits = 3)
new_df$PHI <- round(new_df$PHI, digits = 3)
new_df$ABS_E <- round(new_df$ABS_E, digits = 3)

new_df$GAMMA[is.na(new_df$GAMMA)] <- 0
new_df$PHI[is.na(new_df$PHI)] <- 0

sum(new_df$ABS_E)

new_df <- new_df %>%
  mutate("LVL_1" = SLS_1 / IND_1) %>%
  mutate("TRND_1" = 0) %>%
  mutate("LVL_2" = ALPHA * SLS_2 / IND_2 + (1 - ALPHA) * (LVL_1 + TRND_1)) %>%
  mutate("TRND_2" = GAMMA * (LVL_2 - LVL_1) + (1 - GAMMA) * TRND_1) %>%
  mutate("LVL_3" = ALPHA * SLS_3 / IND_3 + (1 - ALPHA) * (LVL_2 + TRND_2)) %>%
  mutate("TRND_3" = GAMMA * (LVL_3 - LVL_2) + (1 - GAMMA) * TRND_2) %>%
  mutate("LVL_4" = ALPHA * SLS_4 / IND_4 + (1 - ALPHA) * (LVL_3 + TRND_3)) %>%
  mutate("TRND_4" = GAMMA * (LVL_4 - LVL_3) + (1 - GAMMA) * TRND_3) %>%
  mutate("LVL_5" = ALPHA * SLS_5 / IND_5 + (1 - ALPHA) * (LVL_4 + TRND_4)) %>%
  mutate("TRND_5" = GAMMA * (LVL_5 - LVL_4) + (1 - GAMMA) * TRND_4) %>%
  mutate("LVL_6" = ALPHA * SLS_6 / IND_6 + (1 - ALPHA) * (LVL_5 + TRND_5)) %>%
  mutate("TRND_6" = GAMMA * (LVL_6 - LVL_5) + (1 - GAMMA) * TRND_5) %>%
  mutate("LVL_7" = ALPHA * SLS_7 / IND_7 + (1 - ALPHA) * (LVL_6 + TRND_6)) %>%
  mutate("TRND_7" = GAMMA * (LVL_7 - LVL_6) + (1 - GAMMA) * TRND_6) %>%
  mutate("LVL_8" = ALPHA * SLS_8 / IND_8 + (1 - ALPHA) * (LVL_7 + TRND_7)) %>%
  mutate("TRND_8" = GAMMA * (LVL_8 - LVL_7) + (1 - GAMMA) * TRND_7) %>%
  mutate("LVL_9" = ALPHA * SLS_9 / IND_9 + (1 - ALPHA) * (LVL_8 + TRND_8)) %>%
  mutate("TRND_9" = GAMMA * (LVL_9 - LVL_8) + (1 - GAMMA) * TRND_8) %>%
  mutate("LVL_10" = ALPHA * SLS_10 / IND_10 + (1 - ALPHA) * (LVL_9 + TRND_9))   %>%
  mutate("TRND_10" = GAMMA * (LVL_10 - LVL_9) + (1 - GAMMA) * TRND_9) %>%
  mutate("LVL_11" = ALPHA * SLS_11 / IND_11 + (1 - ALPHA) * (LVL_10 + TRND_10)) %>%
  mutate("TRND_11" = GAMMA * (LVL_11 - LVL_10) + (1 - GAMMA) * TRND_10) %>%
  mutate("LVL_12" = ALPHA * SLS_12 / IND_12 + (1 - ALPHA) * (LVL_11 + TRND_11)) %>%
  mutate("TRND_12" = GAMMA * (LVL_12 - LVL_11) + (1 - GAMMA) * TRND_11) %>%
  mutate("LVL_13" = ALPHA * SLS_13 / IND_13 + (1 - ALPHA) * (LVL_12 + TRND_12)) %>%
  mutate("TRND_13" = GAMMA * (LVL_13 - LVL_12) + (1 - GAMMA) * TRND_12) %>%
  mutate("LVL_14" = ALPHA * SLS_14 / IND_14 + (1 - ALPHA) * (LVL_13 + TRND_13)) %>%
  mutate("TRND_14" = GAMMA * (LVL_14 - LVL_13) + (1 - GAMMA) * TRND_13) %>%
  mutate("LVL_15" = ALPHA * SLS_15 / IND_15 + (1 - ALPHA) * (LVL_14 + TRND_14)) %>%
  mutate("TRND_15" = GAMMA * (LVL_15 - LVL_14) + (1 - GAMMA) * TRND_14) %>%
  mutate("LVL_16" = ALPHA * SLS_16 / IND_16 + (1 - ALPHA) * (LVL_15 + TRND_15)) %>%
  mutate("TRND_16" = GAMMA * (LVL_16 - LVL_15) + (1 - GAMMA) * TRND_15) %>%
  mutate("LVL_17" = ALPHA * SLS_17 / IND_17 + (1 - ALPHA) * (LVL_16 + TRND_16)) %>%
  mutate("TRND_17" = GAMMA * (LVL_17 - LVL_16) + (1 - GAMMA) * TRND_16)

fcst_level <- new_df$LVL_17
fcst_level <- as.data.frame(fcst_level)

fcst_index <-
  l52w_sls_select_class %>% select(DPT_CL, F_WK, INDEX) %>%
  spread(F_WK, INDEX, fill = 0.1)

fcst_skus <- l17w_sls_cast %>%
  select(SKU_NBR)

fcst <- bind_cols(fcst_skus, fcst_level)
fcst$fcst_level <- round(fcst$fcst_level, digits = 2)

# Create column combining DPT/CL
l52w_sls_select <- transform(l52w_sls_select, DPT_CL =
                               paste0(DPT, CL_NME))

fcst_dpt_cl <- l52w_sls_select %>%
  select(SKU_NBR, DPT_CL)

fcst_dpt_cl <- unique(fcst_dpt_cl)

fcst <- left_join(fcst, fcst_dpt_cl, by = "SKU_NBR")

fcst <- left_join(fcst, fcst_index, by = "DPT_CL")

fcst <- fcst %>%
  gather(
    `1`,
    `2`,
    `3`,
    `4`,
    `5`,
    `6`,
    `7`,
    `8`,
    `9`,
    `10`,
    `11`,
    `12`,
    `13`,
    `14`,
    `15`,
    `16`,
    `17`,
    `18`,
    `19`,
    `20`,
    `21`,
    `22`,
    `23`,
    `24`,
    `25`,
    `26`,
    `27`,
    `28`,
    `29`,
    `30`,
    `31`,
    `32`,
    `33`,
    `34`,
    `35`,
    `36`,
    `37`,
    `38`,
    `39`,
    `40`,
    `41`,
    `42`,
    `43`,
    `44`,
    `45`,
    `46`,
    `47`,
    `48`,
    `49`,
    `50`,
    `51`,
    `52`,
    key = "WK",
    value = "INDEX"
  )

fcst <- arrange(fcst, SKU_NBR)

fcst <- fcst %>%
  mutate("FCST" = fcst_level * INDEX)

fcst_wks <- l52w_sls_select %>%
  select(F_WK, F_YR_WK, HIST_WK)

fcst_wks <- unique(fcst_wks)
fcst_wks <- mutate(fcst_wks, "FCST_YR_WK" = F_YR_WK + 100)
fcst_wks$F_YR_WK <- NULL
fcst_wks$HIST_WK <- NULL
fcst_wks <- rename(fcst_wks, WK = F_WK)
fcst$WK <- as.numeric(as.character(fcst$WK))
fcst <- left_join(fcst, fcst_wks, by = "WK")
fcst <- fcst %>%
  mutate("FCST_LK_WK" = min(FCST_YR_WK))
fcst$FCST <- round(fcst$FCST, digits = 3)

dpt_class <- select(l52w_sls_select_class, DPT_CL, DPT, CL_NME)
dpt_class <- unique(dpt_class)

fcst <- left_join(fcst, dpt_class, by = "DPT_CL")
fcst <-
  select(fcst,
         DPT,
         CL_NME,
         SKU_NBR,
         FCST_YR_WK,
         FCST,
         fcst_level,
         FCST_LK_WK)
fcst <- arrange(fcst, DPT, SKU_NBR, FCST_YR_WK)


l52w_sls <- left_join(l52w_sls, hist_wks, by = "F_YR_WK")

hist_l52w_sls <- l52w_sls %>%
  select(SKU_NBR, SLS_U, HIST_WK.x) %>%
  group_by(SKU_NBR, HIST_WK.x) %>%
  summarise(sum(SLS_U)) %>%
  rename(SLS = `sum(SLS_U)`) %>%
  arrange(SKU_NBR, HIST_WK.x) %>%
  spread(HIST_WK.x, SLS, fill = 0)

hist_l52w_sls <- as.data.frame(hist_l52w_sls)

hist_l52w_sls <- hist_l52w_sls %>%
  mutate("lw_sls" = rowSums(.[2])) %>%
  mutate("l04w_sls" = rowSums(.[2:5])) %>%
  mutate("l13w_sls" = rowSums(.[2:14])) %>%
  mutate("l26w_sls" = rowSums(.[2:27])) %>%
  mutate("l52w_sls" = rowSums(.[2:53])) %>%
  mutate("brand" = "GLSCOM") %>%
  select(brand, SKU_NBR, lw_sls, l04w_sls, l13w_sls, l26w_sls, l52w_sls) %>%
  filter(l52w_sls > 0)

names(hist_l52w_sls)[2] <- "upc"

curr_week <- hist_wk %>%
  filter(HIST_WK == 52) %>%
  select(F_YR_WK)



yr_wk_lock <- as_vector(curr_week[1] + 100)

# 05 - Summarize and Export --------------------------------------------

fcst_sku_loc <- fcst %>%
  spread(FCST_YR_WK, FCST, fill = 0)

names(fcst_sku_loc)[6:57] <-
  c(
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "10",
    "11",
    "12",
    "13",
    "14",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
    "30",
    "31",
    "32",
    "33",
    "34",
    "35",
    "36",
    "37",
    "38",
    "39",
    "40",
    "41",
    "42",
    "43",
    "44",
    "45",
    "46",
    "47",
    "48",
    "49",
    "50",
    "51",
    "52"
  )

fcst_sku_loc <- fcst_sku_loc %>%
  mutate("brand" = "GLSCOM") %>%
  mutate("yr_wk_lock" = yr_wk_lock) %>%
  mutate("tw_fcst" = fcst_sku_loc$`1`) %>%
  mutate("n04w_fcst" = fcst_sku_loc$`1` + fcst_sku_loc$`2` + fcst_sku_loc$`3` +
           fcst_sku_loc$`4`) %>%
  mutate("fcst_5" = fcst_sku_loc$`5` + 0) %>%
  mutate(
    "n6w_fcst" = fcst_sku_loc$`1` + fcst_sku_loc$`2` + fcst_sku_loc$`3` +
      fcst_sku_loc$`4` + fcst_sku_loc$`5` + fcst_sku_loc$`6`
  ) %>%
  mutate(
    "n13w_fcst" = fcst_sku_loc$`1` + fcst_sku_loc$`2` + fcst_sku_loc$`3` +
      fcst_sku_loc$`4` + fcst_sku_loc$`5` + fcst_sku_loc$`6` +
      fcst_sku_loc$`7` + fcst_sku_loc$`8` + fcst_sku_loc$`9` +
      fcst_sku_loc$`10` + fcst_sku_loc$`11` + fcst_sku_loc$`12` +
      fcst_sku_loc$`13`
  ) %>%
  mutate(
    "n26w_fcst" = fcst_sku_loc$`1` + fcst_sku_loc$`2` + fcst_sku_loc$`3` +
      fcst_sku_loc$`4` + fcst_sku_loc$`5` + fcst_sku_loc$`6` +
      fcst_sku_loc$`7` + fcst_sku_loc$`8` + fcst_sku_loc$`9` +
      fcst_sku_loc$`10` + fcst_sku_loc$`11` + fcst_sku_loc$`12` +
      fcst_sku_loc$`13` + fcst_sku_loc$`14` + fcst_sku_loc$`15` +
      fcst_sku_loc$`16` + fcst_sku_loc$`17` + fcst_sku_loc$`18` +
      fcst_sku_loc$`19` + fcst_sku_loc$`20` + fcst_sku_loc$`21` +
      fcst_sku_loc$`22` + fcst_sku_loc$`23` + fcst_sku_loc$`24` +
      fcst_sku_loc$`25` + fcst_sku_loc$`26`
  ) %>%
  mutate("n52w_fcst" = rowSums(.[6:57]))

colnames(fcst_sku_loc)[3] <- "upc"

write_csv(fcst_sku_loc, "gl-fcst-sku-loc.csv")
write_csv(fcst, "fcst-flat.csv")

fcst_isaiah <- fcst_sku_loc %>%
  select(brand,
         yr_wk_lock,
         upc,
         tw_fcst,
         n04w_fcst,
         n13w_fcst,
         n26w_fcst,
         n52w_fcst) %>%
  filter(n52w_fcst > 0)

fcst_isaiah$n52w_fcst[is.na(fcst_isaiah$n52w_fcst)] <- 0
fcst_isaiah$n26w_fcst[is.na(fcst_isaiah$n26w_fcst)] <- 0
fcst_isaiah$n13w_fcst[is.na(fcst_isaiah$n13w_fcst)] <- 0
fcst_isaiah$n04w_fcst[is.na(fcst_isaiah$n04w_fcst)] <- 0

insert_upload_job(
  project = project,
  dataset = "ecomm",
  table = "fcst_locked_isaiah",
  fcst_isaiah,
  billing = project,
  write_disposition = "WRITE_APPEND"
)

dpm_fcst_lock <- dpm_fcst %>%
  select(F_YR_WK, SKU_NBR, FCST_DPM) %>%
  group_by(F_YR_WK, SKU_NBR) %>%
  summarise(FCST = sum(FCST_DPM)) %>%
  spread(F_YR_WK, FCST, fill = 0) %>%
  mutate("brand" = "GLSCOM") %>%
  mutate("tw_fcst" = rowSums(.[2])) %>%
  mutate("n04w_fcst" = rowSums(.[2:5])) %>%
  mutate("n13w_fcst" = rowSums(.[2:14])) %>%
  mutate("yr_wk_lock" = yr_wk_lock) %>%
  select(brand, yr_wk_lock, SKU_NBR, tw_fcst, n04w_fcst, n13w_fcst) %>%
  filter(n13w_fcst > 0)

names(dpm_fcst_lock)[3] <- "upc"

insert_upload_job(
  project = project,
  dataset = "ecomm",
  table = "fcst_locked_dpm",
  dpm_fcst_lock,
  billing = project,
  write_disposition = "WRITE_APPEND"
)

insert_upload_job(
  project = project,
  dataset = "ecomm",
  table = "sales_lxw",
  hist_l52w_sls,
  billing = project,
  write_disposition = "WRITE_TRUNCATE"
)

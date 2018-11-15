## Prop Table for LC.com Forecast

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
library(scales)
library(forecast)

gcs_auth()



# Import Data -------------------------------------------------------------

hist_dmd <- gcs_get_object("lc_to_dmd.csv", bucket = "ecomm_lux")
hist_dmd <- hist_dmd %>%
  select(-X1)

sql <- "SELECT * FROM [ecomm-197702:ecomm.time_dim]"
time_dim <- query_exec(sql, project = project)

x <- min(time_dim$f_yr_week) + 200

# Transform Data ----------------------------------------------------------

hist_dmd$Week <- str_pad(hist_dmd$Week, width = 2, side = "left", pad = "0")

hist_dmd_top <- hist_dmd %>%
  select(`Ecomm Platform`, Year, Week, Collection, `Gross Pieces`) %>%
  filter(`Ecomm Platform` == "LensCrafter.com") %>%
  filter(!is.na(Collection)) %>%
  transform(YR_WK = paste0(Year, Week)) %>%
  select(YR_WK, Gross.Pieces) %>%
  group_by(YR_WK) %>%
  summarise(DMD = sum(Gross.Pieces)) %>%
  filter(YR_WK != x)

hist_dmd_top$YR_WK <- as.character(hist_dmd_top$YR_WK)

YR_WK <- c("201701", "201702", "201703", "201704", "201705", "201706", "201707",
           "201708", "201709", "201710", "201711")

DMD <- c(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

df <- data.frame(YR_WK, DMD)

hist_dmd_top_all <- bind_rows(df, hist_dmd_top)


# Model Data --------------------------------------------------------------

demand <- hist_dmd_top_all %>%
  select(DMD)

y <- ts(demand, frequency = 52, start = c(2017,1))

fcst <- y %>%
  Arima(order = c(0,1,1), seasonal = c(0, 1, 2)) %>%
  forecast(h = 26)

fcstdf <- as.data.frame(fcst)

p <- y %>%
  Arima(order = c(0, 1, 1), seasonal = c(0, 1, 2)) %>%
  forecast(h = 26) %>%
  autoplot(ylab = "Units", xlab = "Year")

p <- p + labs(title = "Next 26 Week\nDemand Forecast")
p <- p + scale_x_continuous(breaks = c(2017, 2018, 2019))

p

ggsave("p-lc.png", plot = p, h = 9/2, w = 16/2, type = "cairo-png")

# UPC Proportion Table ----------------------------------------------------

lc_fcst_flat <- read_csv("lc-fcst-flat.csv")

lc_fcst_spread <- lc_fcst_flat %>%
  select(FCST_YR_WK, SKU_NBR, FCST) %>%
  spread(FCST_YR_WK, FCST, fill = 0)

newdf <- lc_fcst_spread %>%
  remove_rownames %>%
  column_to_rownames(var="SKU_NBR")

matrix <- data.matrix(newdf)

pt <- prop.table(matrix, margin = 2)

ptdf <- as.data.frame(pt)

ptdf <- ptdf %>%
  add_rownames(var = "UPC")

ptdf_gather <- ptdf %>%
  gather(c(2:53), key = "F_WK", value = "PROP") %>%
  arrange(UPC, F_WK)

f_wk <- as.data.frame(ptdf_gather$F_WK) %>%
  distinct() %>%
  slice(1:26) %>%
  rename_at(1, ~"F_WK")

fcstdf <- bind_cols(f_wk, fcstdf)

fcst_top <- fcstdf %>%
  select(F_WK, `Point Forecast`) %>%
  rename_at(2, ~"FCST_TOP")

ptdf_gather <- left_join(ptdf_gather, fcst_top, by = "F_WK")

ptdf_gather <- ptdf_gather %>%
  filter(!is.na(FCST_TOP)) %>%
  mutate("FCST" = FCST_TOP * PROP)

n26w_fcst <- ptdf_gather %>%
  select(UPC, F_WK, FCST) %>%
  spread(key = F_WK, value = FCST, fill = 0)

fcst_nxw <- n26w_fcst %>%
  mutate("TW_FCST" = rowSums(.[2])) %>%
  mutate("N04W_FCST" = rowSums(.[2:5])) %>%
  mutate("N13W_FCST" = rowSums(.[2:14])) %>%
  mutate("N26W_FCST" = rowSums(.[2:27])) %>%
  select(UPC, TW_FCST, N04W_FCST, N13W_FCST, N26W_FCST)

# Stock KPI Report

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

stock <- gcs_get_object("stock.csv")
qaf <- gcs_get_object("qaf.csv")
transit <- gcs_get_object("transit.csv")
zupc <- gcs_get_object("zupc_ago.csv")

sql <- "SELECT * FROM [ecomm-197702:ecomm.time_dim]"
time_dim <- query_exec(sql, project = project)

x <- min(time_dim$f_yr_week) + 200

sql <- "SELECT * FROM [ecomm-197702:ecomm.sales_lxw]"
sales_lxw <- query_exec(sql, project = project)

sql <-
  paste("SELECT * FROM [ecomm-197702:ecomm.fcst_locked_isaiah] WHERE yr_wk_lock = ",
        x ,
        "",
        sep = "")
fcst_isaiah <- query_exec(sql, project = project)

sql <-
  paste("SELECT * FROM [ecomm-197702:ecomm.fcst_locked_dpm] WHERE yr_wk_lock = ",
        x ,
        "",
        sep = "")
fcst_dpm <- query_exec(sql, project = project)

sql <- "SELECT * FROM [ecomm-197702:ecomm.cc_dim]"
cc <- query_exec(sql, project = project)


# Transform Data ----------------------------------------------------------

detail <- qaf %>%
  select(
    `DATA INSERIMENTO`,
    `DATA CONS`,
    DATESTCODE,
    BRAND,
    SKU,
    MODELLO,
    CALIBRO,
    COLORE,
    `QUANTITA'`
  ) %>%
  filter(DATESTCODE == 8460)

sku <- zupc %>%
  select(Material, Dim.value2, Dim.value1, `Grid value`, `Primary UPC`) %>%
  unite(SKU, Material, Dim.value2, Dim.value1, sep = "")

detail <- left_join(detail, sku, by = "SKU")

sku2 <- zupc %>%
  select(`Primary UPC`, Material, `Grid value`) %>%
  unite(SKU2, Material, `Grid value`, sep = "", remove = TRUE)

stock_avail <- stock %>%
  select(`EAN/UPC`, Available, Allocated, Unrestricted, `Qual-Insp`)

names(stock_avail)[1] <- "UPC"
names(detail)[11] <- "UPC"
names(sales_lxw)[2] <- "UPC"
names(cc)[1] <- "UPC"
names(fcst_isaiah)[3] <- "UPC"
names(sku2)[1] <- "UPC"

detail <- left_join(detail, stock_avail, by = "UPC")

detail <- left_join(detail, sku2, by = "UPC")

transit <- transit %>%
  select(MATNR, J_3ASIZE, ZZSHIPCLOSE, ZZTRNQTY) %>%
  filter(ZZSHIPCLOSE == "00.00.0000")

in_transit <- transit %>%
  unite(SKU2, MATNR, J_3ASIZE, sep = "", remove = FALSE) %>%
  group_by(SKU2) %>%
  summarise(TRANSIT = sum(ZZTRNQTY))

detail <- left_join(detail, in_transit, by = "SKU2")
detail$TRANSIT[is.na(detail$TRANSIT)] <- 0

detail <- detail %>%
  select(-SKU2)

sales_lxw <- sales_lxw %>%
  select(UPC, l52w_sls, l26w_sls, l13w_sls, l04w_sls, lw_sls)

fcst_isa <- fcst_isaiah %>%
  select(UPC, tw_fcst, n04w_fcst, n13w_fcst)

detail <- left_join(detail, cc, by = "UPC")
detail <- left_join(detail, sales_lxw, by = "UPC")
detail <- left_join(detail, fcst_isa, by = "UPC")

detail$cc[is.na(detail$cc)] <- "E"

detail <- detail %>%
  mutate("MIN" = if_else(detail$cc == "A", 8,
                 if_else(detail$cc == "B", 7,
                 if_else(detail$cc == "C", 6,
                 if_else(detail$cc == "D", 3,
                 if_else(detail$cc == "E", 3, 3)))))) %>%
  mutate("TGT_WOS" = detail$n13w_fcst / 13 * 6)

detail[is.na(detail)] <- 0

detail <- detail %>%
  mutate("QAF_REV" = pmax(detail$MIN, detail$TGT_WOS))

detail$QAF_REV <- ceiling(detail$QAF_REV)

detail <- detail[!duplicated(detail$UPC), ]

names(detail) <- toupper(names(detail))

# Export ------------------------------------------------------------------

write_csv(detail, "qaf_detail.csv")

sum(detail$QAF_REV)

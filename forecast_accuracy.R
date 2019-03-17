# Forecast Accuracy

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

gcs_auth()


# 02 - Import Data --------------------------------------------------------

detail <- read_csv("qaf_detail.csv")

sql <- "SELECT   f_yr_week, id
FROM     [ecomm-197702:ecomm.time_dim]
GROUP BY f_yr_week, id"

hist_wk <- query_exec(sql, project = project)

lag01 <- max(hist_wk$f_yr_week)

y <- hist_wk %>%
  filter(hist_wk$id == 4)

lag04 <- y$f_yr_week

z <- hist_wk %>%
  filter(hist_wk$id == 13)

lag13 <- z$f_yr_week


# Isaiah Forecast

sql <-
  paste(
    "SELECT upc, n13w_fcst FROM [ecomm-197702:ecomm.fcst_locked_isaiah] WHERE yr_wk_lock = ",
    lag13 ,
    " AND brand = 'GLSCOM'",
    sep = ""
  )

lag13_fcst_isaiah <- query_exec(sql, project = project)
names(lag13_fcst_isaiah)[1] <- c("EAN_UPC")
names(lag13_fcst_isaiah)[2] <- c("L13W_ISA")

sql <-
  paste(
    "SELECT upc, n04w_fcst FROM [ecomm-197702:ecomm.fcst_locked_isaiah] WHERE yr_wk_lock = ",
    lag04 ,
    " AND brand = 'GLSCOM'",
    sep = ""
  )

lag04_fcst_isaiah <- query_exec(sql, project = project)
names(lag04_fcst_isaiah)[1] <- c("EAN_UPC")
names(lag04_fcst_isaiah)[2] <- c("L04W_ISA")

sql <-
  paste(
    "SELECT upc, tw_fcst FROM [ecomm-197702:ecomm.fcst_locked_isaiah] WHERE yr_wk_lock = ",
    lag01 ,
    " AND brand = 'GLSCOM'",
    sep = ""
  )

lag01_fcst_isaiah <- query_exec(sql, project = project)
names(lag01_fcst_isaiah)[1] <- c("EAN_UPC")
names(lag01_fcst_isaiah)[2] <- c("L01W_ISA")

# DPM Forecast

sql <-
  paste(
    "SELECT upc, n13w_fcst FROM [ecomm-197702:ecomm.fcst_locked_dpm] WHERE yr_wk_lock = ",
    lag13 ,
    " AND brand = 'GLSCOM'",
    sep = ""
  )

lag13_fcst_dpm <- query_exec(sql, project = project)
names(lag13_fcst_dpm)[1] <- c("EAN_UPC")
names(lag13_fcst_dpm)[2] <- c("L13W_DPM")

sql <-
  paste(
    "SELECT upc, n04w_fcst FROM [ecomm-197702:ecomm.fcst_locked_dpm] WHERE yr_wk_lock = ",
    lag04 ,
    " AND brand = 'GLSCOM'",
    sep = ""
  )

lag04_fcst_dpm <- query_exec(sql, project = project)
names(lag04_fcst_dpm)[1] <- c("EAN_UPC")
names(lag04_fcst_dpm)[2] <- c("L04W_DPM")

sql <-
  paste(
    "SELECT upc, tw_fcst FROM [ecomm-197702:ecomm.fcst_locked_dpm] WHERE yr_wk_lock = ",
    lag01 ,
    " AND brand = 'GLSCOM'",
    sep = ""
  )

lag01_fcst_dpm <- query_exec(sql, project = project)
names(lag01_fcst_dpm)[1] <- c("EAN_UPC")
names(lag01_fcst_dpm)[2] <- c("L01W_DPM")




detail <- detail %>%
  select(
    BRAND,
    MODELLO,
    `GRID VALUE`,
    UPC,
    CC,
    AVAILABLE,
    ALLOCATED,
    `QUAL-INSP`,
    TRANSIT,
    L13W_SLS,
    L04W_SLS,
    LW_SLS,
    TW_FCST,
    N04W_FCST,
    N13W_FCST,
    QAF_REV,
    `QUANTITA'`,
    OOS,
    OOS_WITH_TRANSIT,
    POOS,
    REL
  ) %>%
  filter(UPC != 0)

detail$TW_FCST <- round(detail$TW_FCST, digits = 2)
detail$N04W_FCST <- round(detail$N04W_FCST, digits = 2)
detail$N13W_FCST <- round(detail$N13W_FCST, digits = 2)

names(detail)[1] <- c("BRAND")
names(detail)[2] <- c("MODEL")
names(detail)[3] <- c("GRID_VALUE")
names(detail)[4] <- c("EAN_UPC")
names(detail)[5] <- c("CONTR_CODE")
names(detail)[6] <- c("ATP_QTY")
names(detail)[7] <- c("ALLOCATED")
names(detail)[8] <- c("QUAL_INSP")
names(detail)[9] <- c("TRANSIT")
names(detail)[16] <- c("QAF_CALC")
names(detail)[17] <- c("QAF_CURR")
names(detail)[19] <- c("OOS_ITR")

detail <- detail %>%
  arrange(desc(L13W_SLS))

detail <- detail %>%
  select(BRAND, MODEL, GRID_VALUE, EAN_UPC, CONTR_CODE, L13W_SLS, L04W_SLS, LW_SLS)

detail <- left_join(detail, lag13_fcst_isaiah, by = "EAN_UPC")
detail <- left_join(detail, lag04_fcst_isaiah, by = "EAN_UPC")
detail <- left_join(detail, lag01_fcst_isaiah, by = "EAN_UPC")
detail <- left_join(detail, lag13_fcst_dpm, by = "EAN_UPC")
detail <- left_join(detail, lag04_fcst_dpm, by = "EAN_UPC")
detail <- left_join(detail, lag01_fcst_dpm, by = "EAN_UPC")

detail[is.na(detail)] <- 0

detail <- detail %>%
  mutate("L13W_ISA_ABS" = abs(L13W_SLS - L13W_ISA)) %>%
  mutate("L04W_ISA_ABS" = abs(L04W_SLS - L04W_ISA)) %>%
  mutate("L01W_ISA_ABS" = abs(LW_SLS   - L01W_ISA)) %>%
  mutate("L13W_DPM_ABS" = abs(L13W_SLS - L13W_DPM)) %>%
  mutate("L04W_DPM_ABS" = abs(L04W_SLS - L04W_DPM)) %>%
  mutate("L01W_DPM_ABS" = abs(LW_SLS   - L01W_DPM))

detail <- detail %>%
  mutate("L13W_ISA_APE" = L13W_ISA_ABS / L13W_SLS * 100) %>%
  mutate("L04W_ISA_APE" = L04W_ISA_ABS / L04W_SLS * 100) %>%
  mutate("L01W_ISA_APE" = L01W_ISA_ABS / LW_SLS * 100) %>%
  mutate("L13W_DPM_APE" = L13W_DPM_ABS / L13W_SLS * 100) %>%
  mutate("L04W_DPM_APE" = L04W_DPM_ABS / L04W_SLS * 100) %>%
  mutate("L01W_DPM_APE" = L01W_DPM_ABS / LW_SLS * 100)

detail$L13W_ISA_APE[which(detail$L13W_ISA_APE == Inf)] <- 100
detail$L04W_ISA_APE[which(detail$L04W_ISA_APE == Inf)] <- 100
detail$L01W_ISA_APE[which(detail$L01W_ISA_APE == Inf)] <- 100
detail$L13W_DPM_APE[which(detail$L13W_DPM_APE == Inf)] <- 100
detail$L04W_DPM_APE[which(detail$L04W_DPM_APE == Inf)] <- 100
detail$L01W_DPM_APE[which(detail$L01W_DPM_APE == Inf)] <- 100

detail$L13W_ISA_APE[is.nan(detail$L13W_ISA_APE)] <- 100
detail$L04W_ISA_APE[is.nan(detail$L04W_ISA_APE)] <- 100
detail$L01W_ISA_APE[is.nan(detail$L01W_ISA_APE)] <- 100
detail$L13W_DPM_APE[is.nan(detail$L13W_DPM_APE)] <- 100
detail$L04W_DPM_APE[is.nan(detail$L04W_DPM_APE)] <- 100
detail$L01W_DPM_APE[is.nan(detail$L01W_DPM_APE)] <- 100

detail <- detail %>%
  mutate("L13W_ISA_WAPE" = L13W_ISA_APE * L13W_SLS) %>%
  mutate("L04W_ISA_WAPE" = L04W_ISA_APE * L04W_SLS) %>%
  mutate("L01W_ISA_WAPE" = L01W_ISA_APE * LW_SLS) %>%
  mutate("L13W_DPM_WAPE" = L13W_DPM_APE * L13W_SLS) %>%
  mutate("L04W_DPM_WAPE" = L04W_DPM_APE * L04W_SLS) %>%
  mutate("L01W_DPM_WAPE" = L01W_DPM_APE * LW_SLS)

detail <- detail %>%
  mutate("UPC_CNT" = 1)

detail <- distinct(detail, EAN_UPC, .keep_all = TRUE)

# 04 - Export Data --------------------------------------------------------

write_excel_csv(detail, "fcst_accy_gl.csv")

gcs_upload("fcst_accy_gl.csv", gcs_global_bucket(bucket), type = 'text/csv')

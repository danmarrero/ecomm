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
library(scales)

gcs_auth()

# 02 - Import Data --------------------------------------------------------

stock <- gcs_get_object("stock.csv", bucket = "ecomm_lux")
all_stock <- gcs_get_object("all_stock.csv", bucket = "ecomm_lux")
sap_us03_gl <- gcs_get_object("sap_us03_gl.csv", bucket = "ecomm_lux")
qaf <- gcs_get_object("qaf.csv", bucket = "ecomm_lux")
transit <- gcs_get_object("transit.csv", bucket = "ecomm_lux")
zupc <- gcs_get_object("zupc_ago.csv", bucket = "ecomm_lux")
hist_oos <- gcs_get_object("hist_oos.csv", bucket = "ecomm_lux")
brand_hist_oos <- gcs_get_object("brand_hist_oos.csv", bucket = "ecomm_lux")
release <- gcs_get_object("release.csv", bucket = "ecomm_lux")
lifecycle <- gcs_get_object("sap_lifecycle_gl.csv", bucket = "ecomm_lux")
calendar <- gcs_get_object("sap_calendar_gl.csv", bucket = "ecomm_lux")

write_csv(lifecycle, "lifecycle.csv")

lifecycle_gl <-read_delim("lifecycle.csv", 
                          ",",
                          escape_double = FALSE,
                          locale = locale(encoding = "ISO-8859-1"), 
                          na = "empty",
                          trim_ws = TRUE)

sql <- "SELECT * FROM [ecomm-197702:ecomm.time_dim]"
time_dim <- query_exec(sql, project = project)

x <- min(time_dim$f_yr_week) + 200

sql <- "SELECT * FROM [ecomm-197702:ecomm.sales_lxw] WHERE brand = 'GLSCOM'"
sales_lxw <- query_exec(sql, project = project)

sql <-
  paste("SELECT * FROM [ecomm-197702:ecomm.fcst_locked_isaiah] WHERE yr_wk_lock = ",
        x ,
        " AND brand = 'GLSCOM'",
        sep = "")
fcst_isaiah <- query_exec(sql, project = project)

#sql <-
#  paste("SELECT * FROM [ecomm-197702:ecomm.fcst_locked_dpm] WHERE yr_wk_lock = ",
#        x ,
#        "",
#        sep = "")
#fcst_dpm <- query_exec(sql, project = project)

sql <- "SELECT * FROM [ecomm-197702:ecomm.cc_dim] WHERE brand = 'GLSCOM'"
cc <- query_exec(sql, project = project)

#hist_oos <- read_csv("hist_oos.csv")

# Transform Data ----------------------------------------------------------



cc <- cc %>%
  select(-brand)

#sales_lxw <- sales_lxw %>%
#  select(-brand)

sap_us03_gl <- sap_us03_gl %>%
  select(-X1)

hist_oos <- hist_oos %>%
  select(-X1)

hist_oos <- hist_oos %>%
  filter(DATE != Sys.Date())

brand_hist_oos <- brand_hist_oos %>%
  filter(DATE != Sys.Date())

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
  ) #%>%
  #filter(DATESTCODE == "008460")

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

detail$UPC <- as.numeric(as.character(detail$UPC))
sku2$UPC <- as.numeric(as.character(sku2$UPC))

detail <- left_join(detail, stock_avail, by = "UPC")
detail <- left_join(detail, sku2, by = "UPC")

transit <- transit %>%
  select(Material, `Grid Value`, `Closure Date`, `Transit Qty`) %>%
  filter(`Closure Date` == "00.00.0000")

in_transit <- transit %>%
  unite(SKU2, Material, `Grid Value`, sep = "", remove = FALSE) %>%
  group_by(SKU2) %>%
  summarise(TRANSIT = sum(`Transit Qty`))

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

release <- release %>%
  filter(BRAND == "GL") %>%
  select(-X1, -ARTICLE, -BRAND)

detail <- left_join(detail, release, by = "UPC")

detail <- detail %>%
  filter(!is.na(UPC))

#oo_stock_new$Stock_Avail[oo_stock_new$Stock_Avail < 0] <- 0

detail$cc[detail$REL == "NPI"] <- "NPI"

#detail <- detail %>%
#  mutate(cc_new = if_else(detail$REL =))

detail <- detail %>%
  mutate("MIN" = if_else(detail$cc == "A", 7,
                 if_else(detail$cc == "B", 5,
                 if_else(detail$cc == "C", 5,
                 if_else(detail$cc == "D", 3,
                 if_else(detail$cc == "E", 3,
                 if_else(detail$cc == "ADV", 8,
                 if_else(detail$cc == "NPI", 8, 3)))))))) %>%
  mutate("TGT_WOS" = detail$n13w_fcst / 13 * 7)

detail[is.na(detail)] <- 0

detail <- detail %>%
  mutate("QAF_REV" = pmax(detail$MIN, detail$TGT_WOS))

detail$QAF_REV <- ceiling(detail$QAF_REV)

detail <- detail[!duplicated(detail$UPC), ]

names(detail) <- toupper(names(detail))

detail[is.na(detail)] <- 0

lifecycle_gl <- lifecycle_gl %>%
  select(Material, `Grid value`, `Flag collection`) %>%
  unite(MatGrV, Material, `Grid value`, sep = "", remove = TRUE)

detail <- detail %>%
  unite(MatGrV, MODELLO, `GRID VALUE`, sep = "", remove = FALSE)

detail <- left_join(detail, lifecycle_gl, by = "MatGrV")

names(detail)[31] <- c("ZZFCAM")

# Flag Calculations -------------------------------------------------------

detail <- detail %>%
  mutate("OOS" = if_else(AVAILABLE <= 2, 1, 0)) %>%
  mutate("OOS_WITH_TRANSIT" = if_else(AVAILABLE + TRANSIT + `QUAL-INSP` > 2,
                                      0, 1)) %>%
  mutate("POOS" = if_else(OOS == 0 & AVAILABLE + TRANSIT + `QUAL-INSP` < N04W_FCST,
                                    1, 0)) %>%
  mutate("UPC_CNT" = 1)

detail <- left_join(detail, sap_us03_gl, by = "UPC")

detail[is.na(detail)] <- 0

oos_summary <- detail %>%
  select(CC, UPC_CNT, OOS, OOS_WITH_TRANSIT, POOS) %>%
  group_by(CC) %>%
  summarise(sum(UPC_CNT), sum(OOS), sum(OOS_WITH_TRANSIT), sum(POOS))

names(oos_summary)[2:5] <- c("UPC_CNT", "OOS", "OOS_WITH_TRANSIT", "POOS_CNT")

oos_summary <- oos_summary %>%
  mutate("ATP" = OOS / UPC_CNT) %>%
  mutate("ITR" = OOS_WITH_TRANSIT / UPC_CNT) %>%
  mutate("POOS" = POOS_CNT / UPC_CNT)

oos_summary$ATP <-round(oos_summary$ATP, digits = 3)
oos_summary$ITR <- round(oos_summary$ITR, digits = 3)
oos_summary$POOS <- round(oos_summary$POOS, digits = 3)

oos_total <- oos_summary %>%
  filter(CC != "NPI")

oos_total <- oos_total %>%
  mutate(TTL_UPC = sum(UPC_CNT)) %>%
  mutate(TTL_OOS = sum(OOS)) %>%
  mutate(TTL_ITR = sum(OOS_WITH_TRANSIT)) %>%
  mutate(TTL_POOS = sum(POOS_CNT))

oos_total <- oos_total %>%
  mutate(TTL_OOS_PCT = TTL_OOS / TTL_UPC) %>%
  mutate(TTL_ITR_PCT = TTL_ITR / TTL_UPC) %>%
  mutate(TTL_POOS_PCT = TTL_POOS / TTL_UPC) %>%
  mutate(CC = "Grand Total")

oos_total$TTL_OOS_PCT <- round(oos_total$TTL_OOS_PCT, digits = 3)
oos_total$TTL_ITR_PCT <- round(oos_total$TTL_ITR_PCT, digits = 3)
oos_total$TTL_POOS_PCT <- round(oos_total$TTL_POOS_PCT, digits = 3)

oos_total <- oos_total %>%
  select(CC, TTL_UPC, TTL_OOS, TTL_ITR, TTL_POOS, TTL_OOS_PCT, TTL_ITR_PCT,
         TTL_POOS_PCT) %>%
  distinct()

names(oos_total)[2:8] <- c("UPC_CNT", "OOS", "OOS_WITH_TRANSIT", "POOS_CNT",
                           "ATP", "ITR", "POOS")

oos_summary <- bind_rows(oos_summary, oos_total)


brand_summary <- detail %>%
  filter(REL != "NPI") %>%
  select(BRAND, UPC_CNT, OOS) %>%
  dplyr::group_by(BRAND) %>%
  summarise(sum(UPC_CNT), sum(OOS))

names(brand_summary)[2:3] <- c("UPC_CNT", "OOS")

brand_summary <- brand_summary %>%
  mutate("ATP" = OOS / UPC_CNT)

brand_summary$ATP <- round(brand_summary$ATP, digits = 3)

top_5_brands <- detail %>%
  select(BRAND, L13W_SLS) %>%
  group_by(BRAND) %>%
  summarise(L13W = sum(L13W_SLS)) %>%
  arrange(desc(L13W)) %>%
  mutate(RANK = row_number())

brand_summary <- left_join(brand_summary, top_5_brands, by = "BRAND")

brand_summary <- brand_summary %>%
  filter(RANK < 6) %>%
  select(BRAND, CC, ATP, RANK) %>%
  arrange(RANK)


npi_summary <- detail %>%
  filter(REL == "NPI") %>%
  select(BRAND, UPC_CNT, OOS) %>%
  dplyr::group_by(BRAND) %>%
  summarise(sum(UPC_CNT), sum(OOS))

names(npi_summary)[2:3] <- c("UPC_CNT", "OOS")

npi_summary <- npi_summary %>%
  mutate("ATP" = OOS / UPC_CNT)

npi_summary$ATP <- round(npi_summary$ATP, digits = 3)

npi_summary <- left_join(npi_summary, top_5_brands, by = "BRAND")

npi_summary <- npi_summary %>%
  filter(RANK < 6) %>%
  select(BRAND, CC, ATP, RANK) %>%
  arrange(RANK)
  


# Historical OOS Summary --------------------------------------------------

oos_today <- oos_summary %>%
  select(CC, ATP) %>%
  mutate(DATE = Sys.Date()) %>%
  select(DATE, CC, ATP)

oos_total_today <- oos_summary %>%
  select(UPC_CNT, OOS) %>% 
  mutate(GROUP = 1) %>%
  group_by(GROUP, TTL_UPC = sum(UPC_CNT), TTL_OOS = sum(OOS))

oos_total_today <- oos_total_today %>%
  select(TTL_UPC, TTL_OOS) %>%
  distinct()

oos_total_today <- oos_total_today %>%
  mutate(TTL_OOS_PCT = TTL_OOS / TTL_UPC) %>%
  mutate(DATE = Sys.Date()) %>%
  mutate(CC = "Grand Total")

oos_total_today <-  as.data.frame(oos_total_today)

oos_total_today <- oos_total_today %>%
  select(DATE, CC, TTL_OOS_PCT)

oos_total_today$TTL_OOS_PCT <- round(oos_total_today$TTL_OOS_PCT, digits = 3)

names(oos_total_today)[3] <- "ATP OOS %"
names(oos_today)[3] <- "ATP OOS %"

#oos_today <- bind_rows(oos_today, oos_total_today)

hist_oos <- bind_rows(hist_oos, oos_today)

hist_oos <- hist_oos %>%
  select(DATE, CC, `ATP OOS %`) %>%
  distinct()

brand_oos_today <- brand_summary %>%
  select(BRAND, ATP) %>%
  mutate(DATE = Sys.Date()) %>%
  select(DATE, BRAND, ATP)

brand_hist_oos <- bind_rows(brand_hist_oos, brand_oos_today)


# Export ------------------------------------------------------------------

detail_gl <- detail %>%
  select(
    BRAND,
    MODELLO,
    `GRID VALUE`,
    UPC,
    CC,
    ZZFCAM,
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
    REL,
    US03
  ) %>%
  filter(UPC != 0)

detail_gl$TW_FCST <- round(detail$TW_FCST, digits = 2)
detail_gl$N04W_FCST <- round(detail$N04W_FCST, digits = 2)
detail_gl$N13W_FCST <- round(detail$N13W_FCST, digits = 2)

names(detail_gl)[1] <- c("BRAND")
names(detail_gl)[2] <- c("MODEL")
names(detail_gl)[3] <- c("GRID_VALUE")
names(detail_gl)[4] <- c("EAN_UPC")
names(detail_gl)[5] <- c("CONTR_CODE")
names(detail_gl)[6] <- c("ZZFCAM")
names(detail_gl)[7] <- c("ATP_QTY")
names(detail_gl)[8] <- c("ALLOCATED")
names(detail_gl)[9] <- c("QUAL_INSP")
names(detail_gl)[10] <- c("TRANSIT")
names(detail_gl)[17] <- c("QAF_CALC")
names(detail_gl)[18] <- c("QAF_CURR")
names(detail_gl)[20] <- c("OOS_ITR")

detail_gl <- detail_gl %>%
  arrange(desc(L13W_SLS)) %>%
  mutate(UPC_CNT = 1)

detail_gl <- detail_gl %>%
  mutate(RELEASE = if_else(detail_gl$CONTR_CODE == "NPI", "NPI", "Carryover"))

write_excel_csv(detail_gl, "detail_gl.csv")
write_csv(detail, "qaf_detail.csv")
write_csv(oos_summary, "oos_summary.csv")
write_csv(brand_summary, "brand_summary.csv")
write_csv(npi_summary, "npi_summary.csv")
write_csv(hist_oos, "hist_oos.csv")
write_csv(brand_hist_oos, "brand_hist_oos.csv")
write_csv(lifecycle_gl, "lifecycle_gl.csv")
write_csv(all_stock, "all_stock.csv")

gcs_upload("hist_oos.csv", gcs_global_bucket(bucket))
gcs_upload("brand_hist_oos.csv", gcs_global_bucket(bucket))
gcs_upload("detail_gl.csv", gcs_global_bucket(bucket), type = 'text/csv')

sum(detail$QAF_REV)






#oos_summary <- read_csv("oos_summary.csv")

#oos_summary <- oos_summary %>%
#  select(CC, ATP, ITR)

#oos_summary <- oos_summary %>%
#  gather(ATP, ITR, key = "TYPE", value = "OOS")


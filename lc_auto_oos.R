## Lenscrafters.com
## Planning Report

## Load libraries & options-----------------------------------------------------

ptm <- proc.time()

rm(list = ls(all.names = TRUE))

options(scipen = 999)
#options(httr_oob_default=FALSE)

#library(googleAuthR)

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
library(RCurl)
library(stringi)
library(mailR)
library(openxlsx)
library(gridExtra)
library(xtable)

gcs_auth()

#googleAuthR::gar_gce_auth()


## Import data------------------------------------------------------------------

campaign_lc <-
  gcs_get_object("campaign_lc.csv", bucket = "ecomm_lux")

demand_lc <- gcs_get_object("demand_lc.csv", bucket = "ecomm_lux")

assortment_lc <-
  gcs_get_object("assortment_lc.csv", bucket = "ecomm_lux")

sap_lc_ms_wr60 <-
  gcs_get_object("sap_lc_ms_wr60.csv", bucket = "ecomm_lux")

trans_ship_008600 <-
  gcs_get_object("trans_ship_008600.csv", bucket = "ecomm_lux")

zupc_ago_8843 <-
  gcs_get_object("zupc_ago_8843.csv", bucket = "ecomm_lux")

project <- "ecomm-197702"
sql <- "SELECT * FROM [ecomm-197702:ecomm.time_dim]"
time_dim <- query_exec(sql, project = project)

time_dim <- time_dim %>%
  select(-date,-f_day,-f_day_nbr,-f_week,-f_month,-f_quarter) %>%
  distinct()

campaign_lc <- campaign_lc %>% select(-X1)
demand_lc <- demand_lc %>% select(-X1)
assortment_lc <- assortment_lc %>% select(-X1)
sap_lc_ms_wr60 <- sap_lc_ms_wr60 %>% select(-X1)
trans_ship_008600 <- trans_ship_008600 %>% select(-X1)
zupc_ago_8843 <- zupc_ago_8843 %>% select(-X1)

userpwd <- "sghbrand:mqiQp2o6kxtko"

url <-  "https://sftp.luxotticaretail.com/LCInventoryReport/"

filenames <-
  getURL(
    url,
    userpwd = userpwd,
    ftp.use.epsv = FALSE,
    dirlistonly = TRUE
  )

filenames <- strsplit(filenames, "\r\n*")
filenames <- unlist(filenames)

f <- stringi::stri_extract_all_regex(filenames, '(?<=").*?(?=")')

fv <- as.data.frame(f[[3]])

lr <- tail(fv, 1)

char <- as.character(lr$`f[[3]]`)

url2 <- paste("https://sftp.luxotticaretail.com",
              char ,
              "",
              sep = "")

x <- getURL(url2, userpwd = userpwd)

df <- read.csv(text = x, header = FALSE)

df2 <- df %>%
  filter(df$V4 != 0)

df <- df %>%
  mutate("DateTime" = char)

oldnames = c("V1", "V2", "V3", "V4", "V5", "V6")
newnames = c("UPC",
             "ARTICLE",
             "ATP_VALUE",
             "ATP_TOTAL",
             "ATP_9901",
             "ATP_8843")

df <- df %>%
  rename_at(vars(oldnames), ~ newnames) %>%
  select(-UPC)

## Transform & Summarise Data---------------------------------------------------

demand_lc <- demand_lc %>%
  select(F_YR_WK, UPC, Retail.Units) %>%
  filter(!is.na(UPC)) %>%
  filter(UPC != 99)

colnames(demand_lc)[1] <- "f_yr_week"
colnames(demand_lc)[3] <- "Retail Units"
demand_lc$f_yr_week <- as.character(demand_lc$f_yr_week)
time_dim$f_yr_week <- as.character(time_dim$f_yr_week)
demand_lc <- left_join(demand_lc, time_dim, by = "f_yr_week")

demand_l52w <- demand_lc %>%
  select(UPC, `Retail Units`, l52w_ty_flag) %>%
  filter(l52w_ty_flag == 1) %>%
  group_by(UPC) %>%
  summarise(L52W_SLS = sum(`Retail Units`))

demand_l26w <- demand_lc %>%
  select(UPC, `Retail Units`, l26w_ty_flag) %>%
  filter(l26w_ty_flag == 1) %>%
  group_by(UPC) %>%
  summarise(L26W_SLS = sum(`Retail Units`))

demand_l13w <- demand_lc %>%
  select(UPC, `Retail Units`, l13w_ty_flag) %>%
  filter(l13w_ty_flag == 1) %>%
  group_by(UPC) %>%
  summarise(L13W_SLS = sum(`Retail Units`))

demand_l04w <- demand_lc %>%
  select(UPC, `Retail Units`, l04w_ty_flag) %>%
  filter(l04w_ty_flag == 1) %>%
  group_by(UPC) %>%
  summarise(L04W_SLS = sum(`Retail Units`))

demand_ytd <- demand_lc %>%
  select(UPC, `Retail Units`, ytd_ty_flag) %>%
  filter(ytd_ty_flag == 1) %>%
  group_by(UPC) %>%
  summarise(YTD_SLS = sum(`Retail Units`))

demand_qtd <- demand_lc %>%
  select(UPC, `Retail Units`, qtd_ty_flag) %>%
  filter(qtd_ty_flag == 1) %>%
  group_by(UPC) %>%
  summarise(QTD_SLS = sum(`Retail Units`))

demand_mtd <- demand_lc %>%
  select(UPC, `Retail Units`, mtd_ty_flag) %>%
  filter(mtd_ty_flag == 1) %>%
  group_by(UPC) %>%
  summarise(MTD_SLS = sum(`Retail Units`))

demand_wtd <- demand_lc %>%
  select(UPC, `Retail Units`, wtd_ty_flag) %>%
  filter(wtd_ty_flag == 1) %>%
  group_by(UPC) %>%
  summarise(LW_SLS = sum(`Retail Units`))

demand_l52w$UPC <- as.numeric(demand_l52w$UPC)
demand_l26w$UPC <- as.numeric(demand_l26w$UPC)
demand_l13w$UPC <- as.numeric(demand_l13w$UPC)
demand_l04w$UPC <- as.numeric(demand_l04w$UPC)

demand_ytd$UPC <- as.numeric(demand_ytd$UPC)
demand_qtd$UPC <- as.numeric(demand_qtd$UPC)
demand_mtd$UPC <- as.numeric(demand_mtd$UPC)
demand_wtd$UPC <- as.numeric(demand_wtd$UPC)

supply_lc <- assortment_lc

supply_lc <- left_join(supply_lc, zupc_ago_8843, by = "UPC")

supply_lc <- supply_lc %>%
  select(UPC, ARTICLE, CORE_BRAND, Brand, Model, Grid, Color, Size, `Model Description`,
         CORE_OPH_SUN_ACCESSORIES, `Frame Color`, `Lens Color`, NPI, MODEL_STOCK)

colnames(supply_lc)[2] <- "ARTICLE"
colnames(supply_lc)[3] <- "Core Brand"
colnames(supply_lc)[10] <- "Product Type"
colnames(supply_lc)[14] <- "Model Stock"


campaign_lc <- campaign_lc %>%
  select(UPC, CAMPAIGN, EDM)

supply_lc <- left_join(supply_lc, campaign_lc, by = "UPC")

supply_lc[is.na(supply_lc)] <- 0

supply_lc <- supply_lc %>%
  mutate("RELEASE" = if_else(supply_lc$NPI == 1, "NPI", "Carryover")) %>%
  mutate("UPC_CNT" = 1)

supply_lc <- left_join(supply_lc, demand_l52w, by = "UPC")
supply_lc <- left_join(supply_lc, demand_l26w, by = "UPC")
supply_lc <- left_join(supply_lc, demand_l13w, by = "UPC")
supply_lc <- left_join(supply_lc, demand_l04w, by = "UPC")
supply_lc <- left_join(supply_lc, demand_ytd, by = "UPC")
supply_lc <- left_join(supply_lc, demand_qtd, by = "UPC")
supply_lc <- left_join(supply_lc, demand_mtd, by = "UPC")
supply_lc <- left_join(supply_lc, demand_wtd, by = "UPC")

supply_lc[is.na(supply_lc)] <- 0

supply_lc <-
  left_join(supply_lc, df, by = "ARTICLE")

supply_lc <- supply_lc %>%
  unique()

supply_lc <- supply_lc[!duplicated(supply_lc$UPC),]

supply_lc <- supply_lc %>%
  mutate(ModelGrid = paste0(Model, Grid))

supply_lc <- supply_lc %>%
  arrange(desc(L13W_SLS))

trans_ship_008600 <- trans_ship_008600 %>%
  select(Material, `Grid Value`, Complete, `Transit Qty`) %>%
  filter(is.na(Complete))

trans_ship_008600 <- trans_ship_008600 %>%
  mutate(ModelGrid = paste0(Material, `Grid Value`)) %>%
  select(ModelGrid, `Transit Qty`)

trans_ship <- trans_ship_008600 %>%
  group_by(ModelGrid) %>%
  summarise(`Transit 9901` = sum(`Transit Qty`))

supply_lc <- left_join(supply_lc, trans_ship, by = "ModelGrid")

supply_lc[is.na(supply_lc)] <- 0

supply_lc <- supply_lc %>%
  select(-DateTime, everything())

supply_lc <- supply_lc %>%
  select(-ModelGrid)

dt <- Sys.time()

xlsx <- paste("LC Inventory Report ", dt , ".xlsx", sep = "")

write.xlsx(supply_lc, xlsx, asTable = FALSE, colWidths = "auto")

# OOS Flag Calculations ---------------------------------------------------

oos_summary <- supply_lc %>%
  select(RELEASE, UPC_CNT, ATP_9901, ATP_8843, ATP_TOTAL) %>%
  mutate("OOS_9901" = if_else(ATP_9901 == 0, 1, 0)) %>%
  mutate("OOS_8843" = if_else(ATP_8843 == 0, 1, 0)) %>%
  mutate("OOS_TOTAL" = if_else(ATP_TOTAL == 0, 1, 0))

oos_summary[is.na(oos_summary)] <- 0

oos_summary <- oos_summary %>%
  group_by(RELEASE) %>%
  summarise(
    UPC = sum(UPC_CNT),
    `9901_OOS` = sum(OOS_9901),
    `8843_OOS` = sum(OOS_8843),
    TOTAL_OOS = sum(OOS_TOTAL)
  )

oos_summary_total <- oos_summary %>%
  mutate("Release" = "Total") %>%
  select(Release, UPC, `9901_OOS`, `8843_OOS`, TOTAL_OOS)

oos_summary_total <- oos_summary_total %>%
  group_by(Release) %>%
  summarise(
    UPC = sum(UPC),
    `9901_OOS` = sum(`9901_OOS`),
    `8843_OOS` = sum(`8843_OOS`),
    TOTAL_OOS = sum(TOTAL_OOS)
  )

oos_summary_total <- oos_summary_total %>%
  mutate("9901" = `9901_OOS` / UPC) %>%
  mutate("8843" = `8843_OOS` / UPC) %>%
  mutate("Total" = TOTAL_OOS / UPC)

oos_summary_total <- oos_summary_total %>%
  select(Release, `9901`, `8843`, Total)

oos_summary_total$`9901` <-
  round(oos_summary_total$`9901`, digits = 3)
oos_summary_total$`8843` <-
  round(oos_summary_total$`8843`, digits = 3)
oos_summary_total$Total <-
  round(oos_summary_total$Total, digits = 3)

oos_summary <- oos_summary %>%
  mutate("OOS_9901_PCT" = `9901_OOS` / UPC) %>%
  mutate("OOS_8843_PCT" = `8843_OOS` / UPC) %>%
  mutate("OOS_TOTAL_PCT" = TOTAL_OOS / UPC)

oos_summary$OOS_9901_PCT <-
  round(oos_summary$OOS_9901_PCT, digits = 3)
oos_summary$OOS_8843_PCT <-
  round(oos_summary$OOS_8843_PCT, digits = 3)
oos_summary$OOS_TOTAL_PCT <-
  round(oos_summary$OOS_TOTAL_PCT, digits = 3)

#oos_summary$OOS_9901_PCT <- percent(oos_summary$OOS_9901_PCT, suffix = "%")
#oos_summary$OOS_8843_PCT <- percent(oos_summary$OOS_8843_PCT, suffix = "%")
#oos_summary$OOS_TOTAL_PCT <- percent(oos_summary$OOS_TOTAL_PCT, suffix = "%")

oos_table <- oos_summary %>%
  select(RELEASE, OOS_9901_PCT, OOS_8843_PCT, OOS_TOTAL_PCT)

colnames(oos_table)[1] <- "Release"
colnames(oos_table)[2] <- "9901"
colnames(oos_table)[3] <- "8843"
colnames(oos_table)[4] <- "Total"

oos_table <- as.data.frame(oos_table)

oos_table <- bind_rows(oos_table, oos_summary_total)

#oos_table$Type <- if_else(oos_table$Type == "FRAMES", "Frames", "Sunglasses")

# Plot ----------------------------------------------------------

oos_table <- oos_table %>%
  gather(`9901`, `8843`, Total, key = "Site", value = "OOS")

oos_table$OOS <- oos_table$OOS * 100

p <-
  ggplot(data = oos_table, aes(fill = Release, y = OOS, x = Site)) +
  geom_bar(position = "dodge",
           stat = "identity",
           width = 0.50) +
  #theme(legend.position = "none") +
  scale_x_discrete(limits = c(9901, 8843, "Total")) +
  #scale_y_continuous(labels = paste0("%")) +
  theme_minimal() +
  scale_fill_manual("Release",
                    values = c(
                      "Carryover" = "#005192",
                      "NPI" = "#7FA8C8",
                      "Total" = "#DFE9F1"
                    )) +
  geom_text(aes(y = OOS + .9,    # nudge above top of bar
                label = paste0(OOS, '%')),
            # prettify
            position = position_dodge(width = .5),
            size = 2.2)

p <- p + theme(#axis.title.x = element_blank(),
  #axis.title.y = element_blank(),
  legend.position = "right")

p <-
  p + ggtitle(paste0("LensCrafters.com OOS % by Site/Release"))

p <- p + labs(subtitle = paste0("Updated", " ", Sys.time()))

p <- p + labs(caption = paste0("Source file : ", " ", char))

p <- p + theme(plot.title = element_text(size = 12))


png(
  "/home/dmarrero/ecomm/lc-oos-new.png",
  width = 16,
  height = 10,
  units = "cm",
  #res = 90
  res = 110
)
print(p)
dev.off()

#options(bitmapType = 'cairo', device = 'png')

ggsave(
  "lc-oos-report.png",                                                           
  plot = p,                                          
  width = 26,
  height = 10,
  units = c("cm"),
  dpi = 600,
  type = "cairo-png"
)

# Send Automated Email ----------------------------------------------------

subject <-
  paste0("LC.com OOS Report/Inventory Detail", " ", Sys.Date())

send.mail(                                    
  from = "DMarrero@us.luxottica.com",
  to = c( 
    "DMarrero@us.luxottica.com"#,
    #"silvia.lorenzi@luxottica.com",
    #"vshell@luxotticaretail.com",
    #"iperaert@luxotticaretail.com",
    #"ashaffer@luxotticaretail.com"
  )
  #,
  #cc = c(
  #  "elisabetta.frastalli@luxottica.com",
  #  "ecarraro@us.luxottica.com"
  #)
  ,
  subject = subject,
  body = "<html> <img src=\"/home/dmarrero/ecomm/lc-oos-new.png\"> </html>",
  html = TRUE,
  inline = TRUE,
  authenticate = TRUE,
  attach.files = xlsx,
  smtp = list(
    host.name = "smtp.office365.com",
    port = 587,
    user.name = "DMarrero@us.luxottica.com",
    passwd = "HoltWinters14",
    tls = TRUE
  )
)

# Clean Up Files ----------------------------------------------------------

file.remove(xlsx)
file.remove("lc-oos-new.png")

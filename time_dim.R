# Time Dimension Table for BigQuery Dataset

rm(list = ls(all = TRUE))

ptm <- proc.time()

library(bigrquery)
library(tidyverse)
library(lubridate)

project <- "ecomm-197702"
sql <- "SELECT * FROM [ecomm-197702:ecomm.fiscal_calendar]"
fiscal_calendar <- query_exec(sql, project = project)

today <- Sys.Date()

previous_sunday <- floor_date(today, "week")

xtd <-
  seq.Date(from = previous_sunday - 727, to = previous_sunday, "days")
xtd <- as.data.frame(xtd)
xtd <- xtd %>% rename(date = xtd)
xtd <- left_join(xtd, fiscal_calendar, by = "date")

xtd <- xtd %>%
  mutate(f_week_pad = str_pad(
    f_week,
    width = 2,
    side = "left",
    pad = "0"
  )) %>%
  mutate(f_month_pad = str_pad(
    f_month,
    width = 2,
    side = "left",
    pad = "0"
  )) %>%
  mutate(f_quarter_pad = str_pad(
    f_quarter,
    width = 2,
    side = "left",
    pad = "0"
  )) %>%
  unite(f_yr_week,
        f_year,
        f_week_pad,
        sep = "",
        remove = FALSE) %>%
  unite(f_yr_month,
        f_year,
        f_month_pad,
        sep = "",
        remove = FALSE) %>%
  unite(f_yr_quarter,
        f_year,
        f_quarter_pad,
        sep = "",
        remove = FALSE) %>%
  mutate(f_yr_quarter_max = max(f_yr_quarter)) %>%
  mutate(f_yr_month_max = max(f_yr_month)) %>%
  mutate(f_yr_week_max = max(f_yr_week)) %>%
  mutate(f_yr_week_min = min(f_yr_week)) %>%
  mutate(f_yr_max = max(f_year))

xtd$f_yr_week <- as.numeric(as.character(xtd$f_yr_week))
xtd$f_yr_month <- as.numeric(as.character(xtd$f_yr_month))
xtd$f_yr_quarter <- as.numeric(as.character(xtd$f_yr_quarter))
xtd$f_yr_quarter_max <-
  as.numeric(as.character(xtd$f_yr_quarter_max))
xtd$f_yr_month_max <- as.numeric(as.character(xtd$f_yr_month_max))
xtd$f_yr_week_max <- as.numeric(as.character(xtd$f_yr_week_max))
xtd$f_yr_week_min <- as.numeric(as.character(xtd$f_yr_week_min))

xtd <- xtd %>%
  mutate(ytd_ly_flag = if_else(f_year == f_yr_max - 1 &
                                 f_yr_week <= f_yr_week_max - 100,
                               1,
                               0)) %>%
  mutate(
    qtd_ly_flag = if_else(
      f_yr_quarter == f_yr_quarter_max - 100 &
        f_yr_week <= f_yr_week_max - 100,
      1,
      0
    )
  ) %>%
  mutate(mtd_ly_flag = if_else(
    f_yr_month == f_yr_month_max - 100 &
      f_yr_week <= f_yr_week_max - 100,
    1,
    0
  )) %>%
  mutate(wtd_ly_flag = if_else(f_yr_week == f_yr_week_max - 100,
                               1,
                               0)) %>%
  mutate(ytd_ty_flag = if_else(f_year == f_yr_max,
                               1,
                               0)) %>%
  mutate(qtd_ty_flag = if_else(f_yr_quarter == f_yr_quarter_max,
                               1,
                               0)) %>%
  mutate(mtd_ty_flag = if_else(f_yr_month == f_yr_month_max,
                               1,
                               0)) %>%
  mutate(wtd_ty_flag = if_else(f_yr_week == f_yr_week_max,
                               1,
                               0))

f_yr_wk <- xtd %>%
  select(f_yr_week) %>%
  distinct(f_yr_week) %>%
  arrange(desc(f_yr_week)) %>%
  mutate(id = row_number())

xtd <- left_join(xtd, f_yr_wk, by = "f_yr_week")

xtd <- xtd %>%
  mutate(l52w_ly_flag = if_else(id <= 104 & id >= 53,
                                1,
                                0)) %>%
  mutate(l26w_ly_flag = if_else(id <= 78 & id >= 53,
                                1,
                                0)) %>%
  mutate(l13w_ly_flag = if_else(id <= 65 & id >= 53,
                                1,
                                0)) %>%
  mutate(l04w_ly_flag = if_else(id <= 56 & id >= 53,
                                1,
                                0)) %>%
  mutate(l52w_ty_flag = if_else(id <= 52 & id >= 1,
                                1,
                                0)) %>%
  mutate(l26w_ty_flag = if_else(id <= 26 & id >= 1,
                                1,
                                0)) %>%
  mutate(l13w_ty_flag = if_else(id <= 13 & id >= 1,
                                1,
                                0)) %>%
  mutate(l04w_ty_flag = if_else(id <= 4 & id >= 1,
                                1,
                                0))

insert_upload_job(
  project = project,
  dataset = "ecomm",
  table = "time_dim",
  xtd,
  billing = project,
  write_disposition = "WRITE_TRUNCATE"
)

#rm(list = ls(all = TRUE))

proc.time() - ptm
Sys.time()
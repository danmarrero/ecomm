# Contribution Code Calculations

library(bigrquery)
library(tidyverse)

project <- "ecomm-197702"
sql <- "SELECT * FROM [ecomm-197702:ecomm.sales_lxw]"
sales_lxw <- query_exec(sql, project = project)


## Glasses.com CC

sales_lxw_glscom <- sales_lxw %>%
  filter(brand == "GLSCOM")

cc <- sales_lxw_glscom %>%
  select(upc, l13w_sls) %>%
  arrange(desc(l13w_sls)) %>%
  mutate(cum_sum = cumsum(l13w_sls)) %>%
  mutate(ttl_sum = sum(l13w_sls)) %>%
  mutate(sum_pct = cum_sum / ttl_sum) %>%
  mutate(cc = if_else(sum_pct <= 0.60, "A", if_else(
                      sum_pct <= 0.80, "B", if_else(
                      sum_pct <= 0.95, "C", if_else(
                      sum_pct <= 0.99, "D", if_else(
                      sum_pct <= 1.00, "E", "E")))))) %>%
  mutate(brand = "GLSCOM")

cc <- cc %>%
  select(upc, cc, brand)

insert_upload_job(
  project = project,
  dataset = "ecomm",
  table = "cc_dim",
  cc,
  billing = project,
  write_disposition = "WRITE_TRUNCATE"
)


## Lenscrafters.com CC

sales_lxw_lc <- sales_lxw %>%
  filter(brand == "LC")

cc_lc <- sales_lxw_lc %>%
  select(upc, l13w_sls) %>%
  arrange(desc(l13w_sls)) %>%
  mutate(cum_sum = cumsum(l13w_sls)) %>%
  mutate(ttl_sum = sum(l13w_sls)) %>%
  mutate(sum_pct = cum_sum / ttl_sum) %>%
  mutate(cc = if_else(sum_pct <= 0.60, "A", if_else(
    sum_pct <= 0.80, "B", if_else(
      sum_pct <= 0.95, "C", if_else(
        sum_pct <= 0.99, "D", if_else(
          sum_pct <= 1.00, "E", "E")))))) %>%
  mutate(brand = "LC")

cc_lc <- cc_lc %>%
  select(upc, cc, brand)

insert_upload_job(
  project = project,
  dataset = "ecomm",
  table = "cc_dim",
  cc_lc,
  billing = project,
  write_disposition = "WRITE_APPEND"
)


## TargetOptical.com CC

sales_lxw_to <- sales_lxw %>%
  filter(brand == "TO")

cc_to <- sales_lxw_to %>%
  select(upc, l13w_sls) %>%
  arrange(desc(l13w_sls)) %>%
  mutate(cum_sum = cumsum(l13w_sls)) %>%
  mutate(ttl_sum = sum(l13w_sls)) %>%
  mutate(sum_pct = cum_sum / ttl_sum) %>%
  mutate(cc = if_else(sum_pct <= 0.60, "A", if_else(
    sum_pct <= 0.80, "B", if_else(
      sum_pct <= 0.95, "C", if_else(
        sum_pct <= 0.99, "D", if_else(
          sum_pct <= 1.00, "E", "E")))))) %>%
  mutate(brand = "TO")

cc_to <- cc_to %>%
  select(upc, cc, brand)

insert_upload_job(
  project = project,
  dataset = "ecomm",
  table = "cc_dim",
  cc_to,
  billing = project,
  write_disposition = "WRITE_APPEND"
)
# Contribution Code Calculations

library(bigrquery)
library(tidyverse)

project <- "ecomm-197702"
sql <- "SELECT * FROM [ecomm-197702:ecomm.sales_lxw]"
sales_lxw <- query_exec(sql, project = project)

cc <- sales_lxw %>%
  select(upc, l13w_sls) %>%
  arrange(desc(l13w_sls)) %>%
  mutate(cum_sum = cumsum(l13w_sls)) %>%
  mutate(ttl_sum = sum(l13w_sls)) %>%
  mutate(sum_pct = cum_sum / ttl_sum) %>%
  mutate(cc = if_else(sum_pct <= 0.60, "A", if_else(
                      sum_pct <= 0.80, "B", if_else(
                      sum_pct <= 0.95, "C", if_else(
                      sum_pct <= 0.99, "D", if_else(
                      sum_pct <= 1.00, "E", "E"))))))

cc <- cc %>%
  select(upc, cc)

insert_upload_job(
  project = project,
  dataset = "ecomm",
  table = "cc_dim",
  cc,
  billing = project,
  write_disposition = "WRITE_TRUNCATE"
)

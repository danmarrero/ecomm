# Contribution Code Calculations

cc <- sales_lxw %>%
  select(UPC, PRODUCT_TYPE, L13W_SLS_U)

cc <- cc %>%
  group_by(PRODUCT_TYPE) %>%
  mutate(CUM_SUM = cumsum(L13W_SLS_U))

cc_group <- cc %>%
  group_by(PRODUCT_TYPE) %>%
  summarise(sum(L13W_SLS_U))

names(cc_group)[2] <- "TTL_SUM"

cc <- left_join(cc, cc_group, by = "PRODUCT_TYPE")

cc <- cc %>%
  mutate(SUM_PCT = CUM_SUM / TTL_SUM) %>%
  mutate(CC = if_else(SUM_PCT <= .6, "A",
                      if_else(
                        SUM_PCT <= .8, "B",
                        if_else(SUM_PCT <= .95, "C",
                                if_else(
                                  SUM_PCT <= .99, "D",
                                  if_else(SUM_PCT <= 1, "E", "E")
                                ))
                      )))

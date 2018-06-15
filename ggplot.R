oos_summary <- read_csv("oos_summary.csv")

oos_summary <- oos_summary %>%
  select(CC, ATP, ITR, POOS)

oos_summary <- oos_summary %>%
  gather(ATP, ITR, POOS, key = "TYPE", value = "OOS")

oos_summary$OOS <- oos_summary$OOS * 100

p <- ggplot(data = oos_summary, aes(fill = TYPE, y = OOS, x = CC)) +
  geom_bar(position = "dodge", stat = "identity", width = 0.85) +
  theme(legend.position = "none") +
  scale_y_continuous(limits = c(0, 50)) +
  theme_minimal() +
  scale_fill_manual("TYPE", values = c("ATP" = "#005192", "ITR" = "#7FA8C8",
                                       "POOS" = "#DFE9F1")) +
geom_text(aes(y = OOS + 1.5,    # nudge above top of bar
              label = paste0(OOS, '%')),    # prettify
              position = position_dodge(width = .8), 
              size = 3.2)

p <- p + theme(axis.title.x = element_blank(),
               axis.title.y = element_blank(),
               legend.position = "bottom"
)
p <- p + ggtitle(paste0("Glasses.com OOS % by Contribution Code - Updated", " ", Sys.Date()))
p <- p + theme(plot.title = element_text(size = 12))

p


# Historical --------------------------------------------------------------

hist_oos <-
  read_csv("hist_oos.csv")

oos_plot <- hist_oos %>%
  select(`ATP OOS %`, DATE, CC) %>%
  arrange(desc(`ATP OOS %`)) %>%
  filter(CC != "NPI")

#oos_plot$`ATP OOS %` <- oos_plot$`ATP OOS %` * 100

oos_plot$DATE <- as.Date(as.POSIXct(oos_plot$DATE))

oos_plot$facet = factor(oos_plot$CC,
                        levels = c("A",
                                   "B",
                                   "C",
                                   "D",
                                   "E",
                                   "Grand Total"))

p <-
  ggplot(data = oos_plot, aes(x = DATE, y = `ATP OOS %`, group = CC)) +
  geom_line(size = 0.45, color = "#3AAADC") +
  scale_y_continuous(labels = percent) +
  theme(legend.position = "none") +
  theme_bw()

p <- p + facet_wrap(~ facet, nr = 2) +
  theme(strip.background = element_rect(fill = "#005192"),
        strip.text = element_text(size = 10,
                                  color = "white"),
        axis.title.x = element_blank(),
        axis.title.y = element_blank())

p <- p + ggtitle(paste0("Historical ATP OOS % by CC - Updated", " ", Sys.Date()))
p <- p + theme(plot.title = element_text(size = 12))


p
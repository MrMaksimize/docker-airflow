## Airflow - Google Analytics

library(dplyr)
library(ggplot2)

#setwd("~/Documents/Code/poseidon-airflow/data/prod")

GA_Pages <- read.csv("/data/prod/all_pages_datasd.csv")

Treasurer <- GA_Pages %>% 
  group_by(date, page_path_level2) %>% 
  filter(page_path_level1 == "/treasurer/") %>% 
  summarise(views = n())
plot1 <- ggplot(data = Treasurer, aes(x=date, y=views, color=page_path_level2, group=page_path_level2)) +
  geom_line(alpha = 0.8) +
  theme_bw()
plot1

## Avg Bounce Rate
## Bounce Rate = the rate of viewing a single page and then exiting that page
GA_Pages$bounced <- GA_Pages$entrances * (GA_Pages$bounce_rate/100)
Bonuce_Rate <- GA_Pages %>% 
  group_by(page_path_level3) %>% 
  filter(page_path_level1 == "/treasurer/") %>% 
  filter(page_path_level2 == "/payments/") %>% 
  summarise(total_entrance = sum(entrances),
            total_bounce = sum(bounced)) %>% 
  mutate(total_bounce_rate = total_bounce/total_entrance)
Bonuce_Rate

plot2 <- ggplot(Bonuce_Rate, aes(x=as.factor(page_path_level3), y=total_bounce_rate)) +
  geom_bar(stat="identity") +
  geom_text(aes(label = total_entrance), vjust = 1.25, color = "white") +
  theme_bw()
plot2





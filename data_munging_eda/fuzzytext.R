
library(stringdist)
library(tidyverse)


data <- read.csv("/Users/niks/Downloads/advertiser.csv")

group_advertisers <- function(advertiser_names, threshold) {
  grouped_advertisers <- list()
  used <- logical(length(advertiser_names))
  
  for (i in seq_along(advertiser_names)) {
    if (!used[i]) {
      current_advertiser <- advertiser_names[i]
      distances <- stringdist(current_advertiser, advertiser_names, method = "jw")
      
      similar_advertisers <- advertiser_names[which(distances <= threshold)]
      grouped_advertisers[[length(grouped_advertisers) + 1]] <- similar_advertisers
      used[which(distances <= threshold)] <- TRUE
    }
  }
  
  return(grouped_advertisers)
}

advertiser_groups <- group_advertisers(data$x, threshold = 0.1)

write.csv(advertiser_groups, "/Users/niks/Downloads/advertiser.csv", row.names=FALSE)

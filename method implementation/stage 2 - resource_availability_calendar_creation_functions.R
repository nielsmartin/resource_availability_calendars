# RESOURCHE AVAILABILITY CALENDAR CREATION:

# Load libraries
library(dplyr)
library(reshape2)
library(lubridate)
library(ggplot2)
library(scales)
library(cluster)
library(NbClust)
library(factoextra)
library(fpc)
Sys.setenv(TZ='GMT')

#######################################################################
## METHOD 1: DIRECT SAMPLING
#######################################################################

# Retrieve a resource schedule that is sampled from daily availability records
# Inputs:
#   dar = daily availibilty records
#   weeks_in_sample = number of weeks in sample
#   type = "day" or "weekday"
#   replacement = allow dates to be sampled more than once (T or F)
#                 (even when F, this will be the case when weeks in sample > weeks in population)
#   show_dates = show dates in output
retrieve_resource_schedule_sampled <- function(dar, type, weeks_in_sample, replacement, show_dates){
  
  # Check input
  if(weeks_in_sample %% 1 != 0){
    stop("Weeks_in_sample has to be an integer.")
  }
  if(!(identical(replacement, TRUE) | identical(replacement, FALSE))){
    stop("Replacement has to be a boolean.")
  }
  if(!("resource" %in% colnames(dar)) | !("date" %in% colnames(dar))){
    stop("Daily availability records not in correct format.")
  }
  if(!(identical(show_dates, TRUE) | identical(show_dates, FALSE))){
    stop("Show_dates has to be a boolean.")
  }
  
  # Add day-type information to DAR
  if(type == "day"){
    dar <-
      dar %>%
      mutate(day = wday(date,
                        label=T,
                        week_start = getOption("lubridate.week.start", 1)),
             amount_in_week = 1) # number of each day-type in a week (e.g., one Monday per week)
  } else if(type == "weekday"){
    dar <-
      dar %>%
      mutate(day = ifelse(wday(date) %in% c(1, 7), "weekend", "weekday"),
             amount_in_week = ifelse(day=="weekday", 5, 2)) # number of weekdays/weekend in a week
  } else{
    stop("Input type unknown.")
  }
  # Note: additional day-types can be added similar to the ones above, which does not require
  #       adaptation of the remainder of this code
  
  
  # Initiate results
  results <- data.frame()
  sample <- list()
  
  # Find unique resource options
  resource_options <-
    dar %>%
    distinct(resource) %>%
    unlist() %>%
    as.vector()
  
  # Find unique day options
  day_options <-
    dar %>%
    distinct(day)
  day_options <-
    sort(day_options$day) %>%
    unlist() %>%
    as.vector()
  
  rm(.Random.seed, envir=globalenv()) #set random seed
  
  # Find sample
  for(i in 1:length(resource_options)){
    
    for(j in 1:length(day_options)){
      
      # Find unique dates per day/resource combination
      date_options <-
        dar %>%
        filter(resource == resource_options[i],
               day == day_options[j]) %>%
        distinct(date)
      
      # Determine number of days of this day_option in a week, and the total required sample size per day_option
      amount_in_week <- (dar[which(dar$day==day_options[j]),])[1,]$amount_in_week
      sample_size <- amount_in_week*weeks_in_sample
      
      # Sample from date options
      if(replacement == T){
        temp <- sample(date_options$date, sample_size, replace = T)
      } 
      else{
        if(sample_size <= nrow(date_options)){
          temp <- sample(date_options$date, sample_size, replace = F)
        } else {
          quotient <- floor(sample_size / nrow(date_options))
          remainder <- sample_size - (quotient*nrow(date_options))
          for (l in 1:quotient){
            temp <- sample(date_options$date, nrow(date_options), replace = F) %>%  # does not really "sample"
              as.data.frame() %>%
              mutate(resource = resource_options[i],
                     day = day_options[j])
            results <- rbind(results, temp)
          }
          temp <- sample(date_options$date, remainder, replace = F)
        }
      }
      temp <- 
        temp %>%
        as.data.frame() %>%
        mutate(resource = resource_options[i],
               day = day_options[j])
      results <- rbind(results, temp)
    }
  }
  
  # Add week counter to results
  results$week_counter <- rep(1:weeks_in_sample, nrow(results)/weeks_in_sample)
  
  # Retrieve sample information from DAR
  colnames(results)[1] <- c("date")
  for(i in 1:nrow(results)){
    temp <-
      dar %>%
      filter(date == results[i,]$date & resource == results[i,]$resource) %>%
      mutate(week = results[i,]$week_counter)
    sample[[i]] <- temp
  }
  
  # Convert sample from list to data.frame
  sample <- do.call(rbind, sample) %>%
    select(-amount_in_week) %>%
    arrange(resource, week)
  
  # Remove dates
  if(show_dates == F){
    sample <-
      sample %>%
      select(-date) %>%
      mutate(period_start = strftime(period_start, format="%H:%M:%S"),
             period_end = strftime(period_end, format="%H:%M:%S"))
  }
  
  return(sample)
}


# Convert a sampled resource schedule to list format
# Inputs:
#   sample = output from function retrieve_resource_schedule_sampled (with show_dates = T)
convert_sample_to_list <- function(sample){
  
  # Check input
  if(!("resource" %in% colnames(sample)) | !("day" %in% colnames(sample)) | !("week" %in% colnames(sample)) |
     !("period_start" %in% colnames(sample)) | !("period_end" %in% colnames(sample)) | !("status" %in% colnames(sample))){
    stop("Input sample not in correct format.")
  }
  
  # Add date counter to sample
  sample_new <- data.frame()
  for(i in 1:length(unique(sample$resource))){
    temp <-
      sample %>%
      filter(resource == unique(sample$resource)[i])
    
    temp$date_number <- NA
    temp$date_number[1] <- 0
    counter <- 0
    
    for(j in 2:nrow(temp)){
      # if the date is not equal to the previous date, this is a new day
      # if the date is equal to the previous date and the start of the new period is earlier than the
      # end of the previous period (i.e. the same date is sampled twice in a row), this is a new day
      if(temp$date[j] != temp$date[(j-1)] || (temp$date[j] == temp$date[(j-1)] & (temp[j,]$period_start < temp[(j-1),]$period_end))){
        counter <- counter + 1
        temp$date_number[j] <- counter
      } else{
        temp$date_number[j] <- temp$date_number[(j-1)]
      }
    }
    sample_new <- rbind(sample_new, temp)
  }
  sample <- sample_new
  
  # Remove date from start period
  if(is.POSIXct(sample$period_start)){
    sample <-
      sample %>%
      mutate(period_start = strftime(period_start, format="%H:%M:%S"),
             period_start = as.POSIXct(period_start, format="%H:%M:%S"))
  }else{
    stop("Period_end in wrong format.")
  }
  
  # Remove date from end period
  if(is.POSIXct(sample$period_end)){
    sample <-
      sample %>%
      mutate(period_end = strftime(period_end, format="%H:%M:%S"),
             period_end = as.POSIXct(period_end, format="%H:%M:%S"))
  }else{
    stop("Period_end in wrong format.")
  }
  
  # Add day start
  sample <-
    sample %>%
    mutate(period_day_start = as.Date(period_start),
           period_day_start = as.POSIXct(period_day_start))
  
  # Convert times to numeric (and express in minutes)
  sample$period_start <- as.numeric(sample$period_start) / 60
  sample$period_day_start <- as.numeric(sample$period_day_start) / 60
  sample$period_end <- as.numeric(sample$period_end) / 60
  
  # Convert start/end to time relative from day start
  sample <-
    sample %>%
    mutate(start = as.numeric(period_start - period_day_start),
           end = as.numeric(period_end - period_day_start))
  
  # Convert start/end to time relative from sample start
  sample <-
    sample %>%
    mutate(start = start + (60*24)*date_number,
           end = end + (60*24)*date_number)
  
  # Delete unnecessary columns
  sample <-
    sample %>%
    select(resource, status, start, end)
  
  # Filter sample: only available periods
  sample <- 
    sample %>%
    filter(status == "available")
  
  # Create list
  sample$period <- paste("[" , sample$start, "," , sample$end, "]", sep="")
  sample_list <- sample %>% group_by(resource) %>% summarize(schedule = glue::collapse(period, sep ="-,-"))
  
  
  # # Make list using R-list
  # outer_list <- list()
  # for(i in 1:length(unique(sample$resource))){
  #   temp <-
  #     sample %>%
  #     filter(resource == unique(sample$resource)[i])
  #   
  #   inner_list <- list()
  #   
  #   for(j in 1:nrow(temp)){
  #     inner_list[[j]] <- list(temp[j,]$start, temp[j,]$end)
  #   }
  #   
  #   outer_list[[temp[i,]$resource]] <- inner_list
  # }
  # sample_list <- outer_list
  
  return(sample_list)
}


#######################################################################
## METHOD 2: CLUSTER-BASED SAMPLING
#######################################################################

# Retrieve a resource schedule via the clustering of daily availability records
# Inputs:
#   dar = daily availibilty records
#   weeks_in_sample = number of weeks in sample
#   type = "day" or "weekday"
#   weight = value in [0, 1] that determines the importance of minute-minute similarity versus
#            total amount of available hours in a day (higher = more importance on minute-minute similarity).
#   show_dates = show dates in output
retrieve_resource_schedule_clustered <- function(dar, type, weeks_in_sample, weight, show_dates){
  
  # Check input
  if(weeks_in_sample %% 1 != 0){
    stop("Weeks_in_sample has to be an integer.")
  }
  if(!("resource" %in% colnames(dar)) | !("date" %in% colnames(dar))){
    stop("Daily availability records not in correct format.")
  }
  if(!(identical(show_dates, TRUE) | identical(show_dates, FALSE))){
    stop("Show_dates has to be a boolean.")
  }
  
  # Add day-type information to DAR
  if(type == "day"){
    dar <-
      dar %>%
      mutate(day = wday(date,
                        label=T,
                        week_start = getOption("lubridate.week.start", 1)),
             amount_in_week = 1) # number of each day-type in a week (e.g., one Monday per week)
  } else if(type == "weekday"){
    dar <-
      dar %>%
      mutate(day = ifelse(wday(date) %in% c(1, 7), "weekend", "weekday"),
             amount_in_week = ifelse(day=="weekday", 5, 2)) # number of weekdays/weekend in a week
  } else{
    stop("Input type unknown.")
  }
  # Note: additional day-types can be added similar to the ones above, which does not require
  #       adaptation of the remainder of this code
  
  
  # Initiate results
  all_clusters <- data.frame()
  sample <- list()
  counter <- 0
  results <- data.frame()
  
  # Find unique resource options
  resource_options <-
    dar %>%
    distinct(resource) %>%
    unlist() %>%
    as.vector()
  
  # Find unique day options
  day_options <-
    dar %>%
    distinct(day)
  day_options <-
    sort(day_options$day) %>%
    unlist() %>%
    as.vector()
  
  # Make progress bar
  print("Calculating clusters ...")
  pb <- txtProgressBar(min = 0, max = (length(resource_options)*length(day_options) - 1), style = 3)
  
  
  # Make clusters
  for(i in 1:length(resource_options)){
    
    for(j in 1:length(day_options)){
      
      # Find unique dates per day/resource combination
      data <-
        dar %>%
        filter(resource == resource_options[i],
               day == day_options[j])
      
      # Make clusters
      dissimilarity_values <- calculate_dissimilarity_values(data, resource_options[i], weight)
      clusters <- make_clusters(dissimilarity_values)
      
      # Calculate cluster distribution
      distribution <-
        clusters %>%
        group_by(cluster) %>%
        summarize(freq = n()/nrow(clusters))
      
      # Select cluster medoids
      clusters <-
        clusters %>%
        filter(medoid == TRUE) %>%
        select(-medoid)
      
      # Join medoids and distribution
      clusters <-
        full_join(clusters, distribution, by = c("cluster"))
      
      clusters$resource <- resource_options[i]
      clusters$day <- day_options[j]
      
      # Add to total
      all_clusters <- rbind(all_clusters, clusters)
      
      # Adapt progress bar counter
      counter <- counter + 1
      setTxtProgressBar(pb, counter)
    }
  }
  close(pb)
  
  # Sample from clusters
  
  rm(.Random.seed, envir=globalenv()) #set random seed
  
  for(i in 1:length(resource_options)){
    
    for(j in 1:length(day_options)){
      
      # Filter all_clusters
      temp <- 
        all_clusters %>%
        filter(resource == resource_options[i],
               day == day_options[j])
      
      # Calculate required amount of days in sample per resource/day-combination
      amount_in_week <-
        (dar[which(dar$day==day_options[j]),])[1,]$amount_in_week
      sample_size <-
        amount_in_week*weeks_in_sample
      
      # Sample dates from cluster medoids based on the distribution calculated above
      temp2 <- sample(temp$date, sample_size, prob = temp$freq, replace = T) %>%
        as.character() %>%
        as.data.frame()
      
      temp2$resource <- resource_options[i]
      temp2$day <- day_options[j]
      
      temp2$week_counter <- rep(1:weeks_in_sample, each=amount_in_week)
      
      results <- rbind(results, temp2)
    }
  }
  
  colnames(results)[1] <- "date"
  results <-
    results %>%
    group_by(resource) %>%
    arrange(week_counter) %>%
    arrange(resource)
  
  # Retrieve sample information from DAR
  for(i in 1:nrow(results)){
    temp <-
      dar %>%
      filter(date == as.Date(as.character(results[i,]$date)) & resource == results[i,]$resource) %>%
      mutate(week = results[i,]$week_counter) %>%
      select(-amount_in_week)
    
    sample[[i]] <- temp
  }
  
  # Convert sample from list to data.frame
  sample <- do.call(rbind, sample)
  
  # Remove dates
  if(show_dates == F){
    sample <-
      sample %>%
      select(-date) %>%
      mutate(period_start = strftime(period_start, format="%H:%M:%S"),
             period_end = strftime(period_end, format="%H:%M:%S"))
  }
  
  return(sample)
}


# # Convert a sampled resource schedule to list format
# # Inputs:
# #   sample = output from function retrieve_resource_schedule_sampled (with show_dates = T)
# convert_sample_to_list <- function(sample){
#   
#   # Check input
#   if(!("resource" %in% colnames(sample)) | !("day" %in% colnames(sample)) | !("week" %in% colnames(sample)) |
#      !("period_start" %in% colnames(sample)) | !("period_end" %in% colnames(sample)) | !("status" %in% colnames(sample))){
#     stop("Input sample not in correct format.")
#   }
#   
#   # Add date counter to sample
#   sample_new <- data.frame()
#   for(i in 1:length(unique(sample$resource))){
#     temp <-
#       sample %>%
#       filter(resource == unique(sample$resource)[i])
#     
#     temp$date_number <- NA
#     temp$date_number[1] <- 0
#     counter <- 0
#     
#     for(j in 2:nrow(temp)){
#       # if the date is not equal to the previous date, this is a new day
#       # if the date is equal to the previous date and the start of the new period is earlier than the
#       # end of the previous period (i.e. the same date is sampled twice in a row), this is a new day
#       if(temp$date[j] != temp$date[(j-1)] || (temp$date[j] == temp$date[(j-1)] & (temp[j,]$period_start < temp[(j-1),]$period_end))){
#         counter <- counter + 1
#         temp$date_number[j] <- counter
#       } else{
#         temp$date_number[j] <- temp$date_number[(j-1)]
#       }
#     }
#     sample_new <- rbind(sample_new, temp)
#   }
#   sample <- sample_new
#   
#   # Remove date from start period
#   if(is.POSIXct(sample$period_start)){
#     sample <-
#       sample %>%
#       mutate(period_start = strftime(period_start, format="%H:%M:%S"),
#              period_start = as.POSIXct(period_start, format="%H:%M:%S"))
#   }else{
#     stop("Period_end in wrong format.")
#   }
#   
#   # Remove date from end period
#   if(is.POSIXct(sample$period_end)){
#     sample <-
#       sample %>%
#       mutate(period_end = strftime(period_end, format="%H:%M:%S"),
#              period_end = as.POSIXct(period_end, format="%H:%M:%S"))
#   }else{
#     stop("Period_end in wrong format.")
#   }
#   
#   # Add day start
#   sample <-
#     sample %>%
#     mutate(period_day_start = as.Date(period_start),
#            period_day_start = as.POSIXct(period_day_start))
#   
#   # Convert times to numeric
#   sample$period_start <- as.numeric(sample$period_start)
#   sample$period_day_start <- as.numeric(sample$period_day_start)
#   sample$period_end <- as.numeric(sample$period_end)
#   
#   # Convert start/end to time relative from day start
#   sample <-
#     sample %>%
#     mutate(start = as.numeric(period_start - period_day_start),
#            end = as.numeric(period_end - period_day_start))
#   
#   # Convert start/end to time relative from sample start
#   sample <-
#     sample %>%
#     mutate(start = start + (60*60*24)*date_number,
#            end = end + (60*60*24)*date_number)
#   
#   # Delete unnecessary columns
#   sample <-
#     sample %>%
#     select(resource, status, start, end)
#   
#   # Filter sample: only available periods
#   sample <- 
#     sample %>%
#     filter(status == "available")
#   
#   # Make list
#   outer_list <- list()
#   for(i in 1:length(unique(sample$resource))){
#     temp <-
#       sample %>%
#       filter(resource == unique(sample$resource)[i])
#     
#     inner_list <- list()
#     
#     for(j in 1:nrow(temp)){
#       inner_list[[j]] <- list(temp[j,]$start, temp[j,]$end)
#     }
#     
#     outer_list[[temp[i,]$resource]] <- inner_list
#   }
#   sample_list <- outer_list
#   return(sample_list)
# }


# Make clusters
# Inputs:
#    dissimilarity_values = dissimilarity values from function calculate_dissimilarity_values
make_clusters <- function(dissimilarity_values){
  
  # Make hierarchical cluster
  diss_matrix <- 
    acast(dissimilarity_values, day_2 ~ day_1, value.var="dissimilarity")
  
  dist_diss_matrix <-
    as.dist(diss_matrix, diag = TRUE)
  
  hier_cluster <- 
    hclust(dist_diss_matrix, method = "average")
  
  # Plot cluster tree
  plot(hier_cluster, cex = 0.8, hang = -1)
  
  
  # Make squared distance matrix (required for function fviz_nbclust)
  full_diss_matrix <- as.matrix(dist_diss_matrix)
  
  # Make maximum 10 cluster, unless there are fewer data points
  if(ncol(full_diss_matrix) <= 10){
    max_n_clusters <- ncol(full_diss_matrix) - 1
  } else{
    max_n_clusters <- 10
  }
  
  # Use the silhouette method to calculate the optimal cluster amount
  cluster_data <- fviz_nbclust(full_diss_matrix, hcut, method = "silhouette", k.max = max_n_clusters)
  cluster_data <- cluster_data$data
  
  cluster_data <-
    cluster_data %>%
    filter(y == max(cluster_data$y)) %>%
    slice(1)
  
  n_of_clusters <- cluster_data$clusters %>% as.numeric()
  
  # Make non-hierarchical cluster
  #
  # PAM (Partitioning Around Medoids) is practically simpler for our application, because it uses a mediod (the 
  # object of a cluster whose average dissimilarity to all the objects in the cluster is minimal, i.e. the most 
  # centrally located point in the cluster) instead of a calculated center (i.e. the centroid). 
  # By minimizing the absolute distance between the points and the selected centroid, rather than minimizing
  # the square distance, k-medoids is additionally more robust to noise and outliers than k-means (k-centroids).
  
  pam_clusters <- pam(full_diss_matrix, diss = TRUE, n_of_clusters)
  
  # Retrieve medoids
  medoids <- pam_clusters$medoids
  
  # Retrieve cluster allocation
  pam_clusters <- as.data.frame(pam_clusters$clustering)
  # pam_clusters <-
  #   pam_clusters$silinfo$widths %>%
  #   as.data.frame() %>%
  #   select(1)
  
  pam_clusters <-
    cbind(date = rownames(pam_clusters), pam_clusters)
  
  rownames(pam_clusters) <- c()
  colnames(pam_clusters) <- c("date", "cluster")
  pam_clusters$medoid <- FALSE
  
  # Add medoid information
  for(i in 1:nrow(pam_clusters)){
    if(pam_clusters$date[i] %in% medoids){
      pam_clusters$medoid[i] <- TRUE
    }
  }
  
  return(pam_clusters)
  
}


# Determine the degree of dissimilarity between two different days based on a comparison of minute per minute status inequality
# Input:
#     day_# = daily availability record of a specific day
calculate_minute_dissimilarity <- function(day_1, day_2){
  
  # Initialize variables
  dissimilarity <- 0.0
  
  # Strip specific dates
  day_1 <- 
    day_1 %>%
    mutate(period_start = strftime(period_start, format="%H:%M:%S"),
           period_start = as.POSIXct(period_start, format="%H:%M:%S"),
           period_end = strftime(period_end, format="%H:%M:%S"),
           period_end = as.POSIXct(period_end, format="%H:%M:%S"))
  day_2 <- 
    day_2 %>%
    mutate(period_start = strftime(period_start, format="%H:%M:%S"),
           period_start = as.POSIXct(period_start, format="%H:%M:%S"),
           period_end = strftime(period_end, format="%H:%M:%S"),
           period_end = as.POSIXct(period_end, format="%H:%M:%S"))
  
  # For each period in day_1, check its degree of dissimilarity with day_2
  for(i in 1:nrow(day_1)){
    
    # Select corresponding periods in day_2 with a different status
    dissimilar_periods <- day_2 %>%
      filter((period_start >= day_1$period_start[i] & period_start < day_1$period_end[i]) |
               (period_end > day_1$period_start[i] & period_end <= day_1$period_end[i]) |
               (period_start <= day_1$period_start[i] & period_end >= day_1$period_end[i]), 
             status != day_1$status[i])
    
    # When periods with a different status are detected, the non-corresponding part is determined
    if(nrow(dissimilar_periods) >= 1){
      for(j in 1:nrow(dissimilar_periods)){
        
        # Non-corresponding period is completely included in the period under analysis
        if(dissimilar_periods$period_start[j] >= day_1$period_start[i] & dissimilar_periods$period_end[j] <= day_1$period_end[i]){
          if(is.POSIXct(day_1$period_start[i])){
            dissimilarity <- dissimilarity + as.numeric(difftime(dissimilar_periods$period_end[j], dissimilar_periods$period_start[j], units = "mins"))
          } else if(is.numeric(day_1$period_start[i])){
            dissimilarity <- dissimilarity + (dissimilar_periods$period_end[j] - dissimilar_periods$period_start[j])
          } else{
            stop("Timestamp format not supported. Check timestamp format.")
          }
        } else if(dissimilar_periods$period_start[j] >= day_1$period_start[i]){
          # Non-corresponding period lasts longer than the period that is currently considered
          if(is.POSIXct(day_1$period_start[i])){
            dissimilarity <- dissimilarity + as.numeric(difftime(day_1$period_end[i], dissimilar_periods$period_start[j], units = "mins"))
          } else if(is.numeric(day_1$period_start[i])){
            dissimilarity <- dissimilarity + (day_1$period_end[i] - dissimilar_periods$period_start[j])
          } else{
            stop("Timestamp format not supported. Check timestamp format.")
          }
        } else if(dissimilar_periods$period_end[j] <= day_1$period_end[i]){
          # Non-corresponding period starts before the period that is currently considered
          if(is.POSIXct(day_1$period_end[i])){
            dissimilarity <- dissimilarity + as.numeric(difftime(dissimilar_periods$period_end[j], day_1$period_start[i], units = "mins"))
          } else if(is.numeric(day_1$period_end[i])){
            dissimilarity <- dissimilarity + (dissimilar_periods$period_end[j] - day_1$period_start[i])
          } else{
            stop("Timestamp format not supported. Check timestamp format.")
          }
        } else{
          # Non-corresponding period starts before the period that is currently considered and ends after this period ended
          if(is.POSIXct(day_1$period_start[i])){
            dissimilarity <- dissimilarity + as.numeric(difftime(day_1$period_end[i], day_1$period_start[i], units = "mins"))
          } else if(is.numeric(day_1$period_start[i])){
            dissimilarity <- dissimilarity + (day_1$period_end[i] - day_1$period_start[i])
          } else{
            stop("Timestamp format not supported. Check timestamp format.")
          }
        }
      }
    }
  }
  
  return(dissimilarity)
}


# Determine the degree of dissimilarity between two different days based on a comparison of available minutes inequality
# Input:
#     day_# = daily availability record of a specific day
calculate_availability_dissimilarity <- function(day_1, day_2){
  
  # Calculate number of available minutes for day 1
  av_min_day_1 <-
    day_1 %>%
    filter(status == "available") %>%
    mutate(value = as.numeric(difftime(period_end, period_start, units = "mins")))
  av_min_day_1 <-
    sum(av_min_day_1$value)
  
  # Calculate number of available minutes for day 2
  av_min_day_2 <-
    day_2 %>%
    filter(status == "available") %>%
    mutate(value = as.numeric(difftime(period_end, period_start, units = "mins")))
  av_min_day_2 <-
    sum(av_min_day_2$value)
  
  # Calculate absolute difference
  dissimilarity <- abs(av_min_day_1 - av_min_day_2)
  
  return(dissimilarity)
}


# Calculate the dissimilarity values for a specific resource
# Input:
#   dar = daily availability records
#   res = resource
#   weight = value in [0, 1] that determines the importance of minute-minute similarity versus
#            amount of available hours in a day (higher = more importance on minute - minute similarity).
calculate_dissimilarity_values <- function(dar, res, weight){
  # Filter DAR
  temp <-
    dar %>%
    filter(resource == res)
  
  dissimilarity_values <- data.frame()
  
  # For each unique day/day-commbination, calculate the dissimularity
  for(i in 1:length(unique(temp$date))){
    day_1 <-
      temp %>%
      filter(date == unique(temp$date)[i])
    
    # Start from i, to avoid calculating duplicates
    for(j in i:length(unique(temp$date))){
      day_2 <-
        temp %>%
        filter(date == unique(temp$date)[j])
      
      # Calculate minute-minute dissimilarity
      minute_diss <- calculate_minute_dissimilarity(day_1, day_2)
      
      # Calculate available hours dissimilarity
      av_diss <- calculate_availability_dissimilarity(day_1, day_2)
      
      # Combine results
      result <- c(day_1$date[1], day_2$date[1], minute_diss, av_diss)
      
      dissimilarity_values <- rbind(dissimilarity_values, result)
    }
  }
  
  colnames(dissimilarity_values) <- c("day_1", "day_2", "minute_diss", "av_diss")
  
  # Normalize dissimilarity values
  dissimilarity_values$minute_diss <- normalize(dissimilarity_values$minute_diss)
  dissimilarity_values$av_diss <- normalize(dissimilarity_values$av_diss)
  
  # Weigh dissimilarity values
  dissimilarity_values$dissimilarity <- (weight*dissimilarity_values$minute_diss) + ((1 - weight)*dissimilarity_values$av_diss)
  
  dissimilarity_values$day_1 <- as.Date(dissimilarity_values$day_1, origin = "1970-01-01")
  dissimilarity_values$day_2 <- as.Date(dissimilarity_values$day_2, origin = "1970-01-01")
  
  dissimilarity_values <-
    dissimilarity_values %>%
    select(day_1, day_2, dissimilarity)
  
  return(dissimilarity_values)
}


# Plot the dissimilarity heat map from dissimilarity values
# Input:
#     dissimilarity_values = dissimilarity values from function calculate_dissimilarity_values
plot_dissimilarity <- function(dissimilarity_values){
  
  plot_data <-
    dissimilarity_values %>%
    mutate(day_1 = as.POSIXct(day_1),
           day_2 = as.POSIXct(day_2))
  
  plot_data %>%
    ggplot() +
    geom_tile(aes(x = day_1, y = day_2, fill = dissimilarity)) +
    scale_x_datetime(breaks=date_breaks("24 hour"),
                     labels = date_format("%d-%m", tz = "GMT")) +
    scale_y_datetime(breaks=date_breaks("24 hour"),
                     labels = date_format("%d-%m", tz = "GMT"))
}

# Normalize a vector between 0 and 1
normalize <- function(x){
  x <- (x - min(x)) / (max(x) - min(x))
  
  # If normalized values contain NaN, this should be corrected to zero
  index <-  is.nan(x)
  x[index] <- 0
  remove(index)
  
  
  return(x)
}


#######################################################################
## CONVERT TO SAMPLE TO LIST
#######################################################################

# Convert a sampled resource schedule to list format - REIMPLEMENTED 26/07/2018
# Inputs:
#   sample = output from function retrieve_resource_schedule_sampled (with show_dates = T)
convert_sample_to_list2 <- function(sample){
  
  # Retrieve availability periods
  sample <- sample %>% filter(status == "available")
  
  # Determine relative timestamp (in numeric form), relative to the earliest start of availability
  earliest_start <- min(sample$period_start)
  
  sample$start_rel <- as.numeric(difftime(sample$period_start, earliest_start, units = "mins"))
  sample$end_rel <- as.numeric(difftime(sample$period_end, earliest_start, units = "mins"))
  
  
  return()
  
}

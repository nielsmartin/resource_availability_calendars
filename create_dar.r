# SCRIPT TO CREATE DAILY AVAILABILITY RECORDS

# IMPORT LIBRARIES
library(reshape)
library(dplyr)
library(tidyr)
library(lubridate)

# SUPPORTING FUNCTION - DETERMINE AGGREGATION TOLERANCE
# Function that determines the time gap tolerated between activity executions when merging
# activity periods
calculate_aggregation_tolerances <- function(event_log, tolerance_percentage){
  # check if timestamps are in POSIXct format
  if(class(event_log$start_with_date)[1] != "POSIXct"){
    event_log$start_with_date <- ymd_hms(event_log$start_with_date)
  }
  if(class(event_log$complete_with_date)[1] != "POSIXct"){
    event_log$complete_with_date <- ymd_hms(event_log$complete_with_date)
  }
  
  # add activity duration to event log
  event_log$act_dur <- as.numeric(difftime(event_log$complete_with_date, event_log$start_with_date, units="secs"))
  
  # determine mean and median duration and tolerance
  tolerances <- as.data.frame(event_log %>% group_by(resource,activity) %>%
                                      summarize(mean_dur = mean(act_dur), median_dur = median(act_dur),
                                                mean_tol = mean(act_dur) * tolerance_percentage,
                                                median_tol = median(act_dur) * tolerance_percentage))
  
  return(tolerances)
}


# SUPPORTING FUNCTION - POST PROCESSING AVAILABILITY PERIODS
# Function merging availability periods that are overlapping in time
merge_overlapping_av_periods <- function(summary_results){
  
  # sort summary_results
  summary_results <- summary_results %>% 
    arrange(resource,period_start,period_end)
  
  # change timestamp format
  summary_results$period_start <- as.character(summary_results$period_start)
  summary_results$period_end <- as.character(summary_results$period_end)
  
  # filter available periods from summary_results
  av_rows <- summary_results %>% 
    filter(status == "available")
  
  # determine which availability periods need to be merged
  av_rows$prior_res <- c(NA,av_rows$resource[-nrow(av_rows)])
  av_rows$prior_start <- c(NA, av_rows$period_start[-nrow(av_rows)])
  av_rows$prior_end <- c(NA, av_rows$period_end[-nrow(av_rows)])
  
  av_rows$period_start <- ymd_hms(av_rows$period_start)
  av_rows$period_end <- ymd_hms(av_rows$period_end)
  av_rows$prior_start <- ymd_hms(av_rows$prior_start)
  av_rows$prior_end <- ymd_hms(av_rows$prior_end)
  
  av_rows$combine_with_prior <- FALSE
  av_rows$combine_with_prior <- av_rows$resource == av_rows$prior_res & 
    av_rows$period_start <= av_rows$prior_end
  av_rows$combine_with_prior[1] <- FALSE
  
  while(is.na(as.numeric(table(av_rows$combine_with_prior)[2])) == FALSE){
    
    # add counter to support aggregation
    av_rows$aggregation_counter <- NA
    aggr_counter <- 0
    for(i in 1:nrow(av_rows)){
      if(av_rows$combine_with_prior[i] == FALSE){
        aggr_counter <- aggr_counter + 1
      }
      av_rows$aggregation_counter[i] <- aggr_counter
    }
    
    # determine replacement rows 
    av_rows <- as.data.frame(av_rows %>% 
                                 group_by(aggregation_counter) %>% 
                                 summarize(resource = resource[1], date = date[1], 
                                           period_start = min(period_start), 
                                           period_end = max(period_end), status = status[1]))
    
    # check if new iteration is required
    av_rows$period_start <- as.character(av_rows$period_start)
    av_rows$period_end <- as.character(av_rows$period_end)
    
    av_rows$prior_res <- c(NA,av_rows$resource[-nrow(av_rows)])
    av_rows$prior_start <- c(NA, av_rows$period_start[-nrow(av_rows)])
    av_rows$prior_end <- c(NA, av_rows$period_end[-nrow(av_rows)])
    
    av_rows$period_start <- ymd_hms(av_rows$period_start)
    av_rows$period_end <- ymd_hms(av_rows$period_end)
    av_rows$prior_start <- ymd_hms(av_rows$prior_start)
    av_rows$prior_end <- ymd_hms(av_rows$prior_end)
    
    av_rows$combine_with_prior <- FALSE
    av_rows$combine_with_prior <- av_rows$resource == av_rows$prior_res & 
      av_rows$period_start <= av_rows$prior_end
    av_rows$combine_with_prior[1] <- FALSE
    
  }
  
  # change timestamp format
  av_rows$period_start <- as.character(av_rows$period_start)
  av_rows$period_end <- as.character(av_rows$period_end)
  
  # remove temporary columns
  av_rows$prior_res <- NULL
  av_rows$prior_start <- NULL
  av_rows$prior_end <- NULL
  av_rows$prior_status <- NULL
  av_rows$combine_with_prior <- NULL
  av_rows$aggregation_counter <- NULL
  
  # replace availability rows
  summary_results <- summary_results[-which(summary_results$status == "available"),]
  summary_results <- rbind(summary_results, av_rows)
  
  # change timestamp format
  summary_results$period_start <- ymd_hms(summary_results$period_start)
  summary_results$period_end <- ymd_hms(summary_results$period_end)
  
  # sort summary_results
  summary_results <- summary_results %>% 
    arrange(resource,period_start,period_end)
  
  return(summary_results)
}


# SUPPORTING FUNCTION - POST PROCESSING UNAVAILABILITY PERIODS
# Function merging unavailability periods that are overlapping in time
merge_overlapping_unav_periods <- function(summary_results){
  
  # sort summary_results
  summary_results <- summary_results %>% 
    arrange(resource,period_start,period_end)
  
  # change timestamp format
  summary_results$period_start <- as.character(summary_results$period_start)
  summary_results$period_end <- as.character(summary_results$period_end)
  
  # determine which unavailability periods need to be merged
  summary_results$prior_res <- c(NA,summary_results$resource[-nrow(summary_results)])
  summary_results$prior_start <- c(NA, summary_results$period_start[-nrow(summary_results)])
  summary_results$prior_end <- c(NA, summary_results$period_end[-nrow(summary_results)])
  summary_results$prior_status <- c(NA, summary_results$status[-nrow(summary_results)])
  
  summary_results$period_start <- ymd_hms(summary_results$period_start)
  summary_results$period_end <- ymd_hms(summary_results$period_end)
  summary_results$prior_start <- ymd_hms(summary_results$prior_start)
  summary_results$prior_end <- ymd_hms(summary_results$prior_end)
  
  summary_results$combine_with_prior <- FALSE
  summary_results$combine_with_prior <- summary_results$resource == summary_results$prior_res & 
                                          summary_results$period_start <= summary_results$prior_end &
                                          summary_results$status == "unavailable" &
                                          summary_results$prior_status == "unavailable"
  summary_results$combine_with_prior[1] <- FALSE
  
  while(is.na(as.numeric(table(summary_results$combine_with_prior)[2])) == FALSE){
    
    # filter unavailable periods from summary_results
    unav_rows <- summary_results %>% 
      filter(status == "unavailable")
    
    # add counter to support aggregation
    unav_rows$aggregation_counter <- NA
    aggr_counter <- 0
    for(i in 1:nrow(unav_rows)){
      if(unav_rows$combine_with_prior[i] == FALSE){
        aggr_counter <- aggr_counter + 1
      }
      unav_rows$aggregation_counter[i] <- aggr_counter
    }
    
    # determine replacement rows 
    unav_rows <- as.data.frame(unav_rows %>% 
                                 group_by(aggregation_counter) %>% 
                                 summarize(resource = resource[1], date = date[1], 
                                           period_start = min(period_start), 
                                           period_end = max(period_end), status = status[1]))
    
    # remove temporary columns and replace unavailability rows
    summary_results$prior_res <- NULL
    summary_results$prior_start <- NULL
    summary_results$prior_end <- NULL
    summary_results$prior_status <- NULL
    summary_results$combine_with_prior <- NULL
    unav_rows$aggregation_counter <- NULL
    
    summary_results <- summary_results[-which(summary_results$status == "unavailable"),]
    summary_results <- rbind(summary_results, unav_rows)
    
    # sort summary_results
    summary_results <- summary_results %>% 
      arrange(resource,period_start,period_end)
    
    # check if new iteration is required
    summary_results$period_start <- as.character(summary_results$period_start)
    summary_results$period_end <- as.character(summary_results$period_end)
    
    summary_results$prior_res <- c(NA,summary_results$resource[-nrow(summary_results)])
    summary_results$prior_start <- c(NA, summary_results$period_start[-nrow(summary_results)])
    summary_results$prior_end <- c(NA, summary_results$period_end[-nrow(summary_results)])
    summary_results$prior_status <- c(NA, summary_results$status[-nrow(summary_results)])
    
    summary_results$period_start <- ymd_hms(summary_results$period_start)
    summary_results$period_end <- ymd_hms(summary_results$period_end)
    summary_results$prior_start <- ymd_hms(summary_results$prior_start)
    summary_results$prior_end <- ymd_hms(summary_results$prior_end)
    
    summary_results$combine_with_prior <- FALSE
    summary_results$combine_with_prior <- summary_results$resource == summary_results$prior_res & 
                                            summary_results$period_start <= summary_results$prior_end &
                                            summary_results$status == "unavailable" &
                                            summary_results$prior_status == "unavailable"
    summary_results$combine_with_prior[1] <- FALSE
    
  }
  
  # remove temporary columns
  summary_results$prior_res <- NULL
  summary_results$prior_start <- NULL
  summary_results$prior_end <- NULL
  summary_results$prior_status <- NULL
  summary_results$combine_with_prior <- NULL
  
  # change timestamp format
  summary_results$period_start <- ymd_hms(summary_results$period_start)
  summary_results$period_end <- ymd_hms(summary_results$period_end)
  
  return(summary_results)
}


# SUPPORTING FUNCTION - SPLIT AVAILABILITY PERIODS
# Functions that splits up availability periods when the period stretches multiple days
split_av_periods_different_days <- function(summary_results){
  
  # format timestamps
  summary_results$period_start <- as.character(summary_results$period_start)
  summary_results$period_end <- as.character(summary_results$period_end)
  
  # mark unavailability periods for which a split is required
  summary_results$split_required <- FALSE
  summary_results$split_required <- summary_results$status == "available" &
    as.Date(summary_results$period_start) != as.Date(summary_results$period_end)
  
  # determine number of rows in initial summary_results
  n_lines <- nrow(summary_results)
  
  # split unavailability periods
  for(i in 1:n_lines){
    
    if(summary_results$split_required[i] == TRUE){
      
      # add availability for the remainder of the current day
      summary_results[nrow(summary_results)+1,] <- c(summary_results$resource[i], as.character(as.Date(summary_results$period_start[i])), summary_results$period_start[i], paste(as.Date(summary_results$period_start[i]), "23:59:59"), "available", FALSE)
      
      # determine number of intermediate days
      n_of_intermediate_days <- as.numeric(as.Date(summary_results$period_end[i]) - as.Date(summary_results$period_start[i]))
      
      # if necessary, add intermediate "available" days
      if(n_of_intermediate_days > 1){
        for(j in 1:(n_of_intermediate_days - 1)){
          intermed_date <- as.Date(as.Date(summary_results$period_start[i]) + j)
          summary_results[nrow(summary_results)+1,] <- c(summary_results$resource[i], intermed_date, paste(intermed_date, "00:00:01"), paste(intermed_date, "23:59:59"), "available", FALSE)
        }
      } 
      
      # add availability for the period on the "next" day
      summary_results[nrow(summary_results)+1,] <- c(summary_results$resource[i], as.character(as.Date(summary_results$period_end[i])), paste(as.Date(summary_results$period_end[i]), "00:00:00"), summary_results$period_end[i], "available", FALSE)
      
    }
  }
  
  # remove rows that have been split
  summary_results <- summary_results %>% filter(split_required == FALSE)
  
  # remove redundant column
  summary_results$split_required <- NULL
  
  # format timestamps
  summary_results$period_start <- ymd_hms(summary_results$period_start)
  summary_results$period_end <- ymd_hms(summary_results$period_end)
  
  return(summary_results)
}


# SUPPORTING FUNCTION - SPLIT UNAVAILABILITY PERIODS
# Functions that splits up unavailability periods when the period stretches multiple days
split_unav_periods_different_days <- function(summary_results){
  
  # format timestamps
  summary_results$period_start <- as.character(summary_results$period_start)
  summary_results$period_end <- as.character(summary_results$period_end)
  
  # mark unavailability periods for which a split is required
  summary_results$split_required <- FALSE
  summary_results$split_required <- summary_results$status == "unavailable" &
    as.Date(summary_results$period_start) != as.Date(summary_results$period_end)
  
  # determine number of rows in initial summary_results
  n_lines <- nrow(summary_results)
  
  # split unavailability periods
  for(i in 1:n_lines){
    
    if(summary_results$split_required[i] == TRUE){
      
      # add unavailability for the remainder of the current day
      #summary_results <- rbind(summary_results, c(summary_results$resource[i], as.character(as.Date(summary_results$period_start[i])), summary_results$period_start[i], paste(as.Date(summary_results$period_start[i]), "23:59:59"), "unavailable", FALSE))
      summary_results[nrow(summary_results)+1,] <- c(summary_results$resource[i], as.character(as.Date(summary_results$period_start[i])), summary_results$period_start[i], paste(as.Date(summary_results$period_start[i]), "23:59:59"), "unavailable", FALSE)
      
      # determine number of intermediate days
      n_of_intermediate_days <- as.numeric(as.Date(summary_results$period_end[i]) - as.Date(summary_results$period_start[i]))
      
      # if necessary, add intermediate "unavailable" days
      if(n_of_intermediate_days > 1){
        for(j in 1:(n_of_intermediate_days - 1)){
          intermed_date <- as.Date(as.Date(summary_results$period_start[i]) + j)
          #summary_results <- rbind(summary_results, c(summary_results$resource[i], intermed_date, paste(intermed_date, "00:00:00"), paste(intermed_date, "23:59:59"), "unavailable", FALSE))
          summary_results[nrow(summary_results)+1,] <- c(summary_results$resource[i], intermed_date, paste(intermed_date, "00:00:01"), paste(intermed_date, "23:59:59"), "unavailable", FALSE)
        }
      } 
      
      # add unavailability for the period on the "next" day
      #summary_results <- rbind(summary_results, c(summary_results$resource[i], as.character(as.Date(summary_results$period_end[i])), paste(as.Date(summary_results$period_end[i]), "00:00:00"), summary_results$period_end[i], "unavailable", FALSE))
      summary_results[nrow(summary_results)+1,] <- c(summary_results$resource[i], as.character(as.Date(summary_results$period_end[i])), paste(as.Date(summary_results$period_end[i]), "00:00:00"), summary_results$period_end[i], "unavailable", FALSE)
      
    }
  }
  
  # remove rows that have been split
  summary_results <- summary_results %>% filter(split_required == FALSE)
  
  # remove redundant column
  summary_results$split_required <- NULL
  
  # format timestamps
  summary_results$period_start <- ymd_hms(summary_results$period_start)
  summary_results$period_end <- ymd_hms(summary_results$period_end)
  
  return(summary_results)
}


# SUPPORTING FUNCTION - ADD UNKNOWN PERIODS
# After extracting all information from the event log, this function adds unknown periods
# to summary_results
add_unknown_periods <- function(summary_results){
  
  # sort summary_results
  summary_results <- summary_results %>% arrange(resource, period_start, period_end)
  
  # format timestamps
  summary_results$period_start <- as.character(summary_results$period_start)
  summary_results$period_end <- as.character(summary_results$period_end)
  summary_results$date <- as.character(summary_results$date)
  
  # add unknown periods when required
  for(i in 2:nrow(summary_results)){
    
    if(summary_results$resource[i] == summary_results$resource[i-1] &
       summary_results$period_start[i] != summary_results$period_end[i-1] &
       # correction is included to avoid that the time period between 23:59:59 and 00:00:01 is included as an unknown period
       unlist(strsplit(summary_results$period_end[i-1], " "))[c(FALSE,TRUE)] != "23:59:59" &
       unlist(strsplit(summary_results$period_start[i], " "))[c(FALSE,TRUE)] != "00:00:01"){
      
      if(as.Date(summary_results$period_start[i]) == as.Date(summary_results$period_end[i-1])){
        #summary_results <- rbind(summary_results, c(summary_results$resource[i], summary_results$date[i], summary_results$period_end[i-1], summary_results$period_start[i], "unknown"))
        summary_results[nrow(summary_results)+1,] <- c(summary_results$resource[i], summary_results$date[i], summary_results$period_end[i-1], summary_results$period_start[i], "unknown")
      } else{
        summary_results <- split_unknown_periods_different_days(summary_results, summary_results$resource[i],
                                                                summary_results$period_end[i-1], summary_results$period_start[i])
      }
    }
  }
  
  # format timestamps
  summary_results$period_start <- ymd_hms(summary_results$period_start)
  summary_results$period_end <- ymd_hms(summary_results$period_end)
  
  return(summary_results)
}

# SUPPORTING FUNCTION - POST PROCESSING UNKNOWN PERIODS
# Functions that splits up unknown periods when the period stretches multiple days
split_unknown_periods_different_days <- function(summary_results, resource, period_start, period_end){
  
  # add unavailability for the remainder of the current day
  #summary_results <- rbind(summary_results, c(resource, as.character(as.Date(period_start)), period_start, paste(as.Date(period_start), "23:59:59"), "unknown"))
  summary_results[nrow(summary_results)+1,] <- c(resource, as.character(as.Date(period_start)), period_start, paste(as.Date(period_start), "23:59:59"), "unknown")
  
  # determine number of intermediate days
  n_of_intermediate_days <- as.numeric(as.Date(period_end) - as.Date(period_start))
  
  # if necessary, add intermediate "unavailable" days
  if(n_of_intermediate_days > 1){
    for(j in 1:(n_of_intermediate_days - 1)){
      intermed_date <- as.Date(as.Date(period_start) + j)
      #summary_results <- rbind(summary_results, c(resource, intermed_date, paste(intermed_date, "00:00:00"), paste(intermed_date, "23:59:59"), "unknown"))
      summary_results[nrow(summary_results)+1,] <- c(resource, intermed_date, paste(intermed_date, "00:00:01"), paste(intermed_date, "23:59:59"), "unknown")
    }
  } 
  
  # add unavailability for the period on the "next" day
  #summary_results <- rbind(summary_results, c(resource, as.character(as.Date(period_end)), paste(as.Date(period_end), "00:00:00"), period_end, "unavailable"))
  summary_results[nrow(summary_results)+1,] <- c(resource, as.character(as.Date(period_end)), paste(as.Date(period_end), "00:00:01"), period_end, "unknown")
  
  return(summary_results)
}


# DAR CREATION FUNCTION
create_dar <- function(event_log, aggr_tol_pct, single_queue, timestamp_format = "ymd_hms"){
  
  # CREATE WORKING LOG
  # Create working_log
  working_log <- event_log
  
  # Change timestamp format
  if(timestamp_format == "ymd_hms"){
    working_log$start_with_date <- ymd_hms(working_log$start_with_date)
    working_log$complete_with_date <- ymd_hms(working_log$complete_with_date)
    working_log$arrival_proxy_with_date <- ymd_hms(working_log$arrival_proxy_with_date)
  } else if(timestamp_format == "dmy_hms"){
    working_log$start_with_date <- dmy_hms(working_log$start_with_date)
    working_log$complete_with_date <- dmy_hms(working_log$complete_with_date)
    working_log$arrival_proxy_with_date <- dmy_hms(working_log$arrival_proxy_with_date)
  } else{
    stop("Invalid timestamp format!")
  }

  # Add date to working log
  if(!("date" %in% names(working_log))){
    working_log$date <- as.Date(working_log$start_with_date) 
  }
  
  # Arrange working log
  working_log <- working_log %>% arrange(resource,start_with_date,complete_with_date)
  
  # AGGREGATE ACTIVITY PERIODS
  
  # Determine tolerances for aggregation
  tolerances <- calculate_aggregation_tolerances(working_log, aggr_tol_pct)
  
  # Add tolerances to working_log
  working_log$tolerance <- NA
  act_res_comb <- unique(working_log[,c('activity', 'resource')])
  
  for(i in 1:nrow(act_res_comb)){
    index <- working_log$activity == act_res_comb$activity[i] & working_log$resource == act_res_comb$resource[i]
    working_log$tolerance[index] <- tolerances[which(tolerances$resource == act_res_comb$resource[i] & tolerances$activity == act_res_comb$activity[i]),]$median_tol
    remove(index)
  }
  
  working_log$selected_row <- NULL
  
  # Convert timestamps to character for copying purposes
  working_log$complete_with_date <- as.character(working_log$complete_with_date)
  
  # Combine subsequent activity executions, when appropriate, in a single activity period
  working_log$prior_res <- c(NA,working_log$resource[-nrow(working_log)])
  working_log$prior_complete_with_date <- c(NA,working_log$complete_with_date[-nrow(working_log)])
  
  working_log$prior_complete_with_date <- ymd_hms(working_log$prior_complete_with_date)
  working_log$complete_with_date <- ymd_hms(working_log$complete_with_date)
  
  working_log$combine_with_prior <- FALSE
  working_log$combine_with_prior <- ((working_log$resource == working_log$prior_res) & 
    (working_log$start_with_date <= working_log$prior_complete_with_date + working_log$tolerance))
  working_log$combine_with_prior[1] <- FALSE
  
  working_log$aggregation_counter <- NA
  aggr_counter <- 0
  for(i in 1:nrow(working_log)){
    if(working_log$combine_with_prior[i] == FALSE){
      aggr_counter <- aggr_counter + 1
    }
    working_log$aggregation_counter[i] <- aggr_counter
  }
  
  activity_periods <- as.data.frame(working_log %>% group_by(aggregation_counter) %>% summarize(resource = resource[1], date = date[1], start = min(start_with_date), end = max(complete_with_date)))
  
  remove(aggr_counter)
  working_log$prior_res <- NULL
  working_log$prior_complete <- NULL
  working_log$combine_with_prior <- NULL
  working_log$aggregation_counter <- NULL
  
  # Add obtained information to summary results
  summary_results <- data.frame(resource = as.character(activity_periods$resource), date = activity_periods$date, period_start = activity_periods$start, 
                                period_end = activity_periods$end, status = as.character("available"))
  summary_results$resource <- as.character(summary_results$resource)
  summary_results$date <- as.character(summary_results$date)
  summary_results$status <- as.character(summary_results$status)
  
  # Check if further merging of activity periods is required
  summary_results <- merge_overlapping_av_periods(summary_results)
  
  # Determine number of availability periods (will be used later in the algorithm)
  n_availability_periods <- nrow(summary_results)
  
  remove(activity_periods)
  
  
  # CHECK IF BOUNDARY PENDING CASES ARE PRESENT (i.e. pending cases at the end of a period of availability)
  
  # Determine which resources execute which activities
  act_res_comb <- unique(working_log[,c('activity', 'resource')])
  
  # Adjust column format
  working_log$arrival_proxy_with_date <- as.character(working_log$arrival_proxy_with_date)
  working_log$start_with_date <- as.character(working_log$start_with_date)
  working_log$complete_with_date <- as.character(working_log$complete_with_date)
  summary_results$period_start <- as.character(summary_results$period_start)
  summary_results$period_end <- as.character(summary_results$period_end)
  
  # Create new (temporary) columns
  summary_results$res_pending_cases <- FALSE
  summary_results$ores_pending_cases <- FALSE
  summary_results$ores_last_pending_case_start <- NA
  
  # Determine whether pending cases are present
  
  print("Detecting boundary pending cases...")
  pb <- txtProgressBar(min = 0, max = (n_availability_periods - 1), style = 3)
  for(i in 1:(n_availability_periods - 1)){
    
    # Determine pending cases
    if(single_queue == FALSE){
      # if the resource has a dedicated queue, only cases that the resource eventually serviced himself are eligible as pending cases
      res_pending_cases <- working_log %>% 
        filter(resource == summary_results$resource[i], arrival_proxy_with_date < summary_results$period_end[i], start_with_date > summary_results$period_end[i])
      # mark row when pending cases are present
      if(nrow(res_pending_cases) > 0) {
        summary_results$res_pending_cases[i] <- TRUE
      }
      
    } else{
      # if the activity has a single queue, cases that are eventually serviced by another resource are also eligbile as pending cases
      
      # pending cases that the resource eventually services himself
      res_pending_cases <- working_log %>% 
        filter(resource == summary_results$resource[i], arrival_proxy_with_date < summary_results$period_end[i], start_with_date > summary_results$period_end[i])
      
      # pending cases that the resource does not service himself
      act <- (act_res_comb %>% filter(resource == summary_results$resource[i]))$activity
      ores_pending_cases <- working_log %>% 
        filter(activity %in% act, resource != summary_results$resource[i], arrival_proxy_with_date < summary_results$period_end[i], start_with_date > summary_results$period_end[i])
      
      # mark row when pending cases are present
      if(nrow(res_pending_cases) > 0) {
        summary_results$res_pending_cases[i] <- TRUE
      }
      if(single_queue == TRUE && nrow(ores_pending_cases) > 0) {
        summary_results$ores_pending_cases[i] <- TRUE
        summary_results$ores_last_pending_case_start[i] <- max(ores_pending_cases$start_with_date)
      }
    }
    setTxtProgressBar(pb, i)
  }
  close(pb)
  
  remove(res_pending_cases, ores_pending_cases, act, i)
  
  # When boundary pending cases are present, mark the appropriate period as unavailable
  
  print("Adding unavailability periods to summary_results...")
  pb <- txtProgressBar(min = 0, max = (n_availability_periods - 1), style = 3)
  for(i in 1:(n_availability_periods - 1)){
    if(summary_results$res_pending_cases[i] == TRUE){
      # if pending cases are present which the resource will eventually service himself, mark the entire period until the next activity period as unavailable
      if(summary_results$resource[i] == summary_results$resource[i+1]){
        #summary_results <- rbind(summary_results, c(summary_results$resource[i], summary_results$date[i], summary_results$period_end[i], summary_results$period_start[i+1], "unavailable"))
        summary_results[nrow(summary_results)+1,] <- c(summary_results$resource[i], summary_results$date[i], summary_results$period_end[i], summary_results$period_start[i+1], "unavailable", NA, NA, NA)
      } 
      
    } else if(single_queue == TRUE & summary_results$ores_pending_cases[i] == TRUE){
      # otherwise, if a single queue situation prevails and pending cases are present which will eventually be serviced by another resource, mark the period until the last of
      # these cases started as unavailable (unless this is later than the start of the next availability period of the resource under consideration)
      if(summary_results$ores_last_pending_case_start[i] < summary_results$period_start[i+1] ){
        # when the last boundary pending case started before the next availability period starts, the resource is unavailable until the processing of this last pending case starts
        if(summary_results$resource[i] == summary_results$resource[i+1]){
          #summary_results <- rbind(summary_results, c(summary_results$resource[i], summary_results$date[i], summary_results$period_end[i], summary_results$ores_last_pending_case_start[i], "unavailable"))
          summary_results[nrow(summary_results)+1,] <- c(summary_results$resource[i], summary_results$date[i], summary_results$period_end[i], summary_results$ores_last_pending_case_start[i], "unavailable", NA, NA, NA)
        } 
      } else{
        # otherwise, the resource is unavailable until the activity period starts
        if(summary_results$resource[i] == summary_results$resource[i+1]){
          #summary_results <- rbind(summary_results, c(summary_results$resource[i], summary_results$date[i], summary_results$period_end[i], summary_results$period_start[i+1], "unavailable" ))
          summary_results[nrow(summary_results)+1,] <- c(summary_results$resource[i], summary_results$date[i], summary_results$period_end[i], summary_results$period_start[i+1], "unavailable", NA, NA, NA) 
        } 
      } 
    }
    setTxtProgressBar(pb, i)
  }
  close(pb)
  
  
  # CHECK IF INTERMEDIATE PENDING CASES ARE PRESENT (i.e. pending cases in between periods of availability)
  
  print("Detecting intermediate pending cases and adding unavailability periods to summary_results...")
  pb <- txtProgressBar(min = 0, max = (n_availability_periods - 1), style = 3)
  for(i in 1:(n_availability_periods - 1)){
    
    if(summary_results$res_pending_cases[i] == FALSE){
      
      # Determine whether intermediate pending cases are present which the resource eventually processed himself
      res_im_pending_cases <- as.data.frame(working_log %>% 
                                              filter(resource == summary_results$resource[i],
                                                     start_with_date > arrival_proxy_with_date,
                                                     arrival_proxy_with_date > summary_results$period_end[i],
                                                     arrival_proxy_with_date < summary_results$period_start[i+1]))
      
      # In case of a single queue, determine whether intermediate pending cases are present which another resource eventually processed                       
      if(single_queue == TRUE){
        act <- (act_res_comb %>% filter(resource == summary_results$resource[i]))$activity
        ores_im_pending_cases <- as.data.frame(working_log %>% 
                                                 filter(resource != summary_results$resource[i],
                                                        activity %in% act,
                                                        start_with_date > arrival_proxy_with_date,
                                                        arrival_proxy_with_date > summary_results$period_end[i],
                                                        arrival_proxy_with_date < summary_results$period_start[i+1]))
        
      }
      
      
      # Mark the appropriate periods as unavailable when intermediate pending cases are detected
      if(nrow(res_im_pending_cases)> 0){
        if(summary_results$resource[i] == summary_results$resource[i+1]){
          #summary_results <- rbind(summary_results, c(summary_results$resource[i], summary_results$date[i], min(res_im_pending_cases$arrival_proxy_with_date), summary_results$period_start[i+1], "unavailable"))
          summary_results[nrow(summary_results)+1,] <- c(summary_results$resource[i], summary_results$date[i], min(res_im_pending_cases$arrival_proxy_with_date), summary_results$period_start[i+1], "unavailable", NA, NA, NA)
        } 
      }
      
      if(single_queue == TRUE && nrow(ores_im_pending_cases) > 0){
        for(j in 1:nrow(ores_im_pending_cases)){
          # if execution of the pending case starts after the next availability period starts, the unavailability period ends at that moment
          if(ores_im_pending_cases$start_with_date[j] >= summary_results$period_start[i+1]){
            #summary_results <- rbind(summary_results, c(summary_results$resource[i], summary_results$date[i], ores_im_pending_cases$arrival_proxy_with_date[j], summary_results$period_start[i+1], "unavailable"))
            summary_results[nrow(summary_results)+1,] <- c(summary_results$resource[i], summary_results$date[i], ores_im_pending_cases$arrival_proxy_with_date[j], summary_results$period_start[i+1], "unavailable", NA, NA, NA)
          } else{
            # if execution of the pending case (by another resource) starts before the next availability period starts, the unavailability period ends when processing starts
            #summary_results <- rbind(summary_results, c(summary_results$resource[i], summary_results$date[i], ores_im_pending_cases$arrival_proxy_with_date[j], ores_im_pending_cases$start_with_date[j], "unavailable"))
            summary_results[nrow(summary_results)+1,] <- c(summary_results$resource[i], summary_results$date[i], ores_im_pending_cases$arrival_proxy_with_date[j], ores_im_pending_cases$start_with_date[j], "unavailable", NA, NA, NA)
          }     
        }
      } 
      
    } 
    setTxtProgressBar(pb, i)
  }
  close(pb)
  
  # Remove temporary columns
  summary_results$res_pending_cases <- NULL
  summary_results$ores_pending_cases <- NULL
  summary_results$ores_last_pending_case_start <- NULL
  
  # Post-process unavailability periods
  summary_results <- merge_overlapping_unav_periods(summary_results)
  summary_results <- split_unav_periods_different_days(summary_results)
  
  # Sort summary results
  summary_results <- summary_results %>% arrange(resource,period_start,period_end)
  
  
  # UNKNOWN PERIOD HANDLING
  
  # Mark unknown periods in summary_results
  summary_results <- add_unknown_periods(summary_results)
  
  # Sort summary results
  summary_results <- summary_results %>% arrange(resource,period_start,period_end)
  
  # Set date column
  summary_results$date <- as.character(summary_results$date)
  summary_results$date <- as.Date(summary_results$period_start)
  
  return(summary_results)
  
}

# FILTER FUNCTION
# Function that removes daily availability records for days on which no resource activity is recorded
# and recordings before the first activity period on a particular day and after the last activity period
# on a particular day

filter_daily_availability_records <- function(daily_availability_records){
  
  # check if timestamps are in POSIXct format
  if(class(daily_availability_records$period_start)[1] != "POSIXct"){
    daily_availability_records$period_start <- ymd_hms(daily_availability_records$period_start)
  }
  if(class(daily_availability_records$period_end)[1] != "POSIXct"){
    daily_availability_records$period_end <- ymd_hms(daily_availability_records$period_end)
  }
  
  # apply filter
  
  daily_availability_records <- as.data.frame(daily_availability_records %>%
                                                group_by(as.Date(period_start), resource) %>%
                                                filter(n() > 1, "available" %in% status))
  daily_availability_records <- as.data.frame(daily_availability_records %>%
                                                group_by(resource, date) %>%
                                                mutate(wd_start = min(period_start[status == "available"]), wd_end = max(period_end[status == "available"])) %>%
                                                filter(period_start >= wd_start, period_end <= wd_end)) %>%
                                                select(resource, date, period_start, period_end, status)
  
  return(daily_availability_records)
}

# FUNCTIONS FOR IMPUTATION AND AGGREGATION OF DAILY AVAILABILITY RECORDS
# Functions that aim to impute and aggregate daily availability records

# Function that introduces the working day notion in the daily availability records (i.e. a resource
# is assumed to be unavailable before the start of the first activity instance on a particular day and
# the end of the last activity instance on a particular day)
working_day <- function(daily_availability_records){
  
  # Remove all entries in daily availability records that lay outside the working day
  daily_availability_records <- as.data.frame(daily_availability_records %>%
                                                group_by(resource, date) %>%
                                                mutate(wd_start = min(period_start[status == "available"]), wd_end = max(period_end[status == "available"])) %>%
                                                filter(period_start >= wd_start, period_end <= wd_end) %>%
                                                select(resource, date, period_start, period_end, status, wd_start, wd_end))
  
  # Add unavailable periods before the start of the working day and after the end of the working day
  # - before start of working day
  prior_to_start_periods <- as.data.frame(daily_availability_records %>% filter(period_start == wd_start))
  prior_to_start_periods$period_start <- as.character(prior_to_start_periods$period_start)
  prior_to_start_periods$period_end <- as.character(prior_to_start_periods$period_end)
  prior_to_start_periods$period_end <- prior_to_start_periods$period_start
  prior_to_start_periods$period_start <- paste(prior_to_start_periods$date, "00:00:00")
  prior_to_start_periods$status <- "unavailable"
  prior_to_start_periods$period_start <- ymd_hms(prior_to_start_periods$period_start)
  prior_to_start_periods$period_end <- ymd_hms(prior_to_start_periods$period_end)
  
  # - after end of working day
  after_end_periods <- as.data.frame(daily_availability_records %>% filter(period_end == wd_end))
  after_end_periods$period_start <- as.character(after_end_periods$period_start)
  after_end_periods$period_end <- as.character(after_end_periods$period_end)
  after_end_periods$period_start <- after_end_periods$period_end
  after_end_periods$period_end <- paste(after_end_periods$date, "23:59:59")
  after_end_periods$status <- "unavailable"
  after_end_periods$period_start <- ymd_hms(after_end_periods$period_start)
  after_end_periods$period_end <- ymd_hms(after_end_periods$period_end)
  
  # Combine data frames
  daily_availability_records <- rbind(daily_availability_records, prior_to_start_periods)
  daily_availability_records <- rbind(daily_availability_records, after_end_periods)
  
  # Prepare intermediate output data frame
  daily_availability_records <- as.data.frame(daily_availability_records %>%
                                                arrange(resource,period_start) %>%
                                                select(resource, date, period_start, period_end, status))
  
  # Determine wheter fully unavailable days need to be added
  daily_availability_records$date <- as.character(daily_availability_records$date)
  daily_availability_records$prior_date <- c(NA, daily_availability_records$date[-nrow(daily_availability_records)])
  daily_availability_records$prior_resource <- c(NA, daily_availability_records$resource[-nrow(daily_availability_records)])
  daily_availability_records$date <- ymd(daily_availability_records$date)
  daily_availability_records$prior_date <- ymd(daily_availability_records$prior_date)
  
  daily_availability_records$add_unav_days <- (daily_availability_records$resource == daily_availability_records$prior_resource) & (as.numeric(difftime(daily_availability_records$date, daily_availability_records$prior_date, units = "days")) > 1)
  
  # Add fully unavailable days
  temp <- daily_availability_records %>% filter(add_unav_days == TRUE)
  
  if(nrow(temp) > 0){
    temp$start_intermed_days <- temp$prior_date + 1
    temp$end_intermed_days <- temp$date - 1
    temp$n_intermed_days <- as.numeric(difftime(temp$end_intermed_days, temp$start_intermed_days, "days")) + 1
    
    for(i in 1:nrow(temp)){
      
      next_row <- nrow(daily_availability_records) + 1
      
      # Add first fully unavailable day
      daily_availability_records[next_row, "resource"] <- temp$resource[i]
      daily_availability_records[next_row, "date"] <- temp$start_intermed_days[i]
      daily_availability_records[next_row, "period_start"] <- ymd_hms(paste(temp$start_intermed_days[i], "00:00:00"))
      daily_availability_records[next_row, "period_end"] <- ymd_hms(paste(temp$start_intermed_days[i], "23:59:59"))
      daily_availability_records[next_row, "status"] <- "unavailable"
      
      if(temp$n_intermed_days[i] > 1){
        # Add next fully unavailable days
        for(j in 1:(n_intermed_days - 1)){
          next_row <- nrow(daily_availability_records) + 1
          
          daily_availability_records[next_row, "resource"] <- temp$resource[i]
          daily_availability_records[next_row, "date"] <- temp$start_intermed_days[i] + j
          daily_availability_records[next_row, "period_start"] <- ymd_hms(paste((temp$start_intermed_days[i] + j), "00:00:00"))
          daily_availability_records[next_row, "period_end"] <- ymd_hms(paste((temp$start_intermed_days[i] + j), "23:59:59"))
          daily_availability_records[next_row, "status"] <- "unavailable"
          
        }
      }
    }
    
  }
  
  # Prepare output data frame
  daily_availability_records <- as.data.frame(daily_availability_records %>%
                                                arrange(resource,period_start) %>%
                                                select(resource, date, period_start, period_end, status))
  
  return(daily_availability_records)
}

# Function that imputes uknown periods using the following rules of thumb (preliminary implementation):
# * when adjacent periods of the unknown period are of the same type, the unknown period is imputed with this type
# * when adjecent periods of the unknown period are of a different type, the unknown period is imputed by allocating them proportionally to the 
# adjacent periods (proportional on the length of the adjacent periods), unless the user specifies the proportion of the unknown period that needs
# to be added to the prior period
impute_unknown_periods <- function(daily_availability_records, imputation_pct = NA){
  
  # check if timestamps are in POSIXct format
  if(class(daily_availability_records$period_start)[1] != "POSIXct"){
    daily_availability_records$period_start <- ymd_hms(daily_availability_records$period_start)
  }
  if(class(daily_availability_records$period_end)[1] != "POSIXct"){
    daily_availability_records$period_end <- ymd_hms(daily_availability_records$period_end)
  }
  
  # add temporary colmuns
  daily_availability_records$prior_period_status <- c(NA,daily_availability_records$status[-nrow(daily_availability_records)])
  daily_availability_records$next_period_status <- c(daily_availability_records$status[-1], NA)
  daily_availability_records$evolution <- paste(daily_availability_records$prior_period_status, daily_availability_records$status, daily_availability_records$next_period_status)
  daily_availability_records$mark_for_removal <- FALSE
  
  # impute unknown periods
  for(i in 2:(nrow(daily_availability_records)-1)){
    if(daily_availability_records$evolution[i] == "available unknown available"){
      daily_availability_records$status[i] <- "available"
    } else if(daily_availability_records$evolution[i] == "unavailable unknown unavailable"){
      daily_availability_records$status[i] <- "unavailable"
    } else if(daily_availability_records$evolution[i] == "available unknown unavailable" | daily_availability_records$evolution[i] == "unavailable unknown available"){
      # when the preceding and next period have a different status, impute proportionally to the length of the
      # adjacent periods, unless imputation_pct is specified
      
      unknown_period_length <- as.numeric(difftime(daily_availability_records$period_end[i], daily_availability_records$period_start[i], unit = "secs"))
      daily_availability_records$mark_for_removal[i] <- TRUE
      
      if(is.na(imputation_pct)){
        prec_period_length <- as.numeric(difftime(daily_availability_records$period_end[i-1], daily_availability_records$period_start[i-1], unit = "secs"))
        next_period_length <- as.numeric(difftime(daily_availability_records$period_end[i+1], daily_availability_records$period_start[i+1], unit = "secs"))
        
        if(next_period_length > 0) {
          period_proportion <- prec_period_length / next_period_length
        } else{
          period_proportion <- 1
        }
        
        if(period_proportion <= 1){
          prop_add_to_preceding_period <- period_proportion
        } else{
          prop_add_to_preceding_period <- 1 - (1 / period_proportion)
        }
        
        daily_availability_records$period_end[i-1] <- daily_availability_records$period_end[i-1] + (unknown_period_length * prop_add_to_preceding_period)
        daily_availability_records$period_start[i+1] <- daily_availability_records$period_start[i+1] - (unknown_period_length * (1 - prop_add_to_preceding_period))
        
      } else{
        daily_availability_records$period_end[i-1] <- daily_availability_records$period_end[i-1] + (unknown_period_length * imputation_pct)
        daily_availability_records$period_start[i+1] <- daily_availability_records$period_start[i+1] - (unknown_period_length * (1 - imputation_pct))
      }
    } 
  }
  
  # remove rows that have become redundant
  daily_availability_records <- daily_availability_records %>% filter(mark_for_removal == FALSE)

  # remove temporary columns
  daily_availability_records$prior_period_status <- NULL
  daily_availability_records$next_period_status <- NULL
  daily_availability_records$evolution <- NULL
  daily_availability_records$mark_for_removal <- NULL
  
  # merge availabile and unavailable periods
  daily_availability_records <- merge_overlapping_av_periods(daily_availability_records)
  daily_availability_records <- merge_overlapping_unav_periods(daily_availability_records)
  
  return(daily_availability_records)
}

# Function that enforces a threshold for the length of unavailability periods
enforce_unav_threshold <- function(daily_availability_records, unav_threshold, threshold_unit = "hours"){
  
  # convert unav_threshold to hours
  if(threshold_unit == "mins"){
    unav_threshold <- unav_threshold / 60
  } else if(threshold_unit == "secs"){
    unav_threshold <- unav_threshold / 3600
  }
  
  # check if timestamps are in POSIXct format
  if(class(daily_availability_records$period_start)[1] != "POSIXct"){
    daily_availability_records$period_start <- ymd_hms(daily_availability_records$period_start)
  }
  if(class(daily_availability_records$period_end)[1] != "POSIXct"){
    daily_availability_records$period_end <- ymd_hms(daily_availability_records$period_end)
  }
  
  # add temporary duration column
  daily_availability_records$dur <- as.numeric(difftime(daily_availability_records$period_end, daily_availability_records$period_start, units = "hours"))
  
  # determine which unavailability periods are below the defined threshold
  index <- daily_availability_records$status == "unavailable" & 
              daily_availability_records$dur < unav_threshold
  daily_availability_records$status[index] <- "available"
  remove(index)
  
  # remove temporary duration column
  daily_availability_records$dur <- NULL
  
  # merge availabile and unavailable periods
  daily_availability_records <- merge_overlapping_av_periods(daily_availability_records)
  
  return(daily_availability_records)
}


# Function that returns summary statistics for unavailability periods, which can support the 
# specification of a threshold (summary_statistics expressed in hours)
unav_periods_summary_statistics <- function(daily_availability_records){
  
  unav_rows <- daily_availability_records %>% filter(status == "unavailable")
  
  # check if timestamps are in POSIXct format
  if(class(unav_rows$period_start)[1] != "POSIXct"){
    unav_rows$period_start <- ymd_hms(unav_rows$period_start)
  }
  if(class(unav_rows$period_end)[1] != "POSIXct"){
    unav_rows$period_end <- ymd_hms(unav_rows$period_end)
  }
  
  # add temporary duration column
  unav_rows$dur <- as.numeric(difftime(unav_rows$period_end, unav_rows$period_start, units = "hours"))
  
  # determine summary statistics
  summary_stats <- unav_rows %>%
                    group_by(resource) %>%
                    summarize(mean_n = n() / length(unique(as.Date(period_start))),
                              mean_dur = mean(dur),
                              sd_dur = sd(dur),
                              median_dur = median(dur),
                              min_dur = min(dur),
                              max_dur = max(dur),
                              q25 = quantile(dur, 0.25),
                              q75 = quantile(dur, 0.75),
                              q95 = quantile(dur, 0.95),
                              q99 = quantile(dur, 0.99))

  return(summary_stats)
}

# Supporting function: calculate working day length based on daily availability records
working_day_length_dar_based <- function(daily_availability_records){
  
  # check if timestamps are in POSIXct format
  if(class(daily_availability_records$period_start)[1] != "POSIXct"){
    daily_availability_records$period_start <- ymd_hms(daily_availability_records$period_start)
  }
  if(class(daily_availability_records$period_end)[1] != "POSIXct"){
    daily_availability_records$period_end <- ymd_hms(daily_availability_records$period_end)
  }
  
  # filter available time periods
  daily_availability_records <- as.data.frame(daily_availability_records %>% filter(status == "available"))
  
  # determine first and last activity of a resource on a particular day
  results <- as.data.frame(daily_availability_records %>% 
                            group_by(resource, date = as.Date(period_start)) %>% 
                            summarize(start_wd = min(period_start), end_wd = max(period_end)))
  results$wd_length <- as.numeric(difftime(results$end_wd, results$start_wd, units = "hours"))
  
  return(results)
}

# FUNCTION TO EVALUATE DAILY AVAILABILITY RECORDS ACCURACY

# Supporting function - Determine length of a known day schedule
determine_day_length <- function(real_day_schedule){
  
  available_periods <- as.data.frame(real_day_schedule %>% filter(status == "available"))
  
  if(is.POSIXct(available_periods$period_start[1])){
    day_length <- as.numeric(difftime(max(available_periods$period_end), min(available_periods$period_start), units = "mins"))
  } else if(is.numeric(available_periods$period_start[i])){
    day_length <- max(available_periods$period_end) - min(available_periods$period_start)
  } else{
    stop("Timestamp format not supported. Check timestamp format.")
  }
  
  return(day_length)
}


# Determine the degree of non-correspondence of (i) the daily availability records and (ii) a known day schedule 
calculate_non_correspondence <- function(real_day_schedule, daily_availability_record){
  
  # Initialize variables
  non_correspondence <- 0.0
  
  # For each period in the real_day_schedule, check its degree of non-correspondence with daily_availability_record
  for(i in 1:nrow(real_day_schedule)){
    
    # Select corresponding periods in daily availability period with a different status
    dar_periods <- daily_availability_record %>%
                      filter((period_start >= real_day_schedule$period_start[i] & period_start < real_day_schedule$period_end[i]) |
                               (period_end > real_day_schedule$period_start[i] & period_end <= real_day_schedule$period_end[i]) |
                               (period_start <= real_day_schedule$period_start[i] & period_end >= real_day_schedule$period_end[i]), 
                             status != real_day_schedule$status[i])
    
    # When periods with a different status are detected, the non-corresponding part is determined
    if(nrow(dar_periods) >= 1){
      for(j in 1:nrow(dar_periods)){
        
        # Non-corresponding period is completely included in the period under analysis
        if(dar_periods$period_start[j] >= real_day_schedule$period_start[i] & dar_periods$period_end[j] <= real_day_schedule$period_end[i]){
          if(is.POSIXct(real_day_schedule$period_start[i])){
            non_correspondence <- non_correspondence + as.numeric(difftime(dar_periods$period_end[j], dar_periods$period_start[j], units = "mins"))
          } else if(is.numeric(real_day_schedule$period_start[i])){
            non_correspondence <- non_correspondence + (dar_periods$period_end[j] - dar_periods$period_start[j])
          } else{
            stop("Timestamp format not supported. Check timestamp format.")
          }
        } else if(dar_periods$period_start[j] >= real_day_schedule$period_start[i]){
          # Non-corresponding period lasts longer than the period that is currently considered
          if(is.POSIXct(real_day_schedule$period_start[i])){
            non_correspondence <- non_correspondence + as.numeric(difftime(real_day_schedule$period_end[i], dar_periods$period_start[j], units = "mins"))
          } else if(is.numeric(real_day_schedule$period_start[i])){
            non_correspondence <- non_correspondence + (real_day_schedule$period_end[i] - dar_periods$period_start[j])
          } else{
            stop("Timestamp format not supported. Check timestamp format.")
          }
        } else if(dar_periods$period_end[j] <= real_day_schedule$period_end[i]){
          # Non-corresponding period starts before the period that is currently considered
          if(is.POSIXct(real_day_schedule$period_end[i])){
            non_correspondence <- non_correspondence + as.numeric(difftime(dar_periods$period_end[j], real_day_schedule$period_start[i], units = "mins"))
          } else if(is.numeric(real_day_schedule$period_end[i])){
            non_correspondence <- non_correspondence + (dar_periods$period_end[j] - real_day_schedule$period_start[i])
          } else{
            stop("Timestamp format not supported. Check timestamp format.")
          }
        } else{
          # Non-corresponding period starts before the period that is currently considered and ends after this period ended
          if(is.POSIXct(real_day_schedule$period_start[i])){
            non_correspondence <- non_correspondence + as.numeric(difftime(real_day_schedule$period_end[i], real_day_schedule$period_start[i], units = "mins"))
          } else if(is.numeric(real_day_schedule$period_start[i])){
            non_correspondence <- non_correspondence + (real_day_schedule$period_end[i] - real_day_schedule$period_start[i])
          } else{
            stop("Timestamp format not supported. Check timestamp format.")
          }
        }
      }
    }
  }
  
  return(non_correspondence)
}


# OTHER FUNCTIONS

# Supporting function - Determine numeric time value
determine_numeric_time_value <- function(time, origin = "01/01/2016 0:00:00"){
  if(!is.POSIXct(time)){
    time <- dmy_hms(time)
  }
  
  numeric_time_value <- as.numeric(difftime(time, dmy_hms(origin), unit = "mins"))
  
  return(numeric_time_value)
}



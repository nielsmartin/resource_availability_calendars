# RESOURCE SCHEDULE CREATION - EVALUATION SUMMER 2018
# last update: 24/07/2018

source('scripts/create_dar.r')
source('scripts/resource_schedule_creation_functions.R')

#######################################################################
## GENERAL SETTINGS
#######################################################################

# Numer of weeks for which a schedule needs to be created for each resource in stage 2
schedule_n_of_weeks <- 6

# Select activity log time segment to create DAR (1.0 indicates full activity log is used, 0.8 indicates that 80% of the activity log is used, ...)
#factor <- 1.0

# DAR-factor: more convenient then the prior as the DARs do not have to be learnt any longer
#dar_factor <- 0.6

# Number of replications used in SimPy simulations
n_replications <- 1

#######################################################################
## STAGE 1: EVENT LOG PREPARATION
#######################################################################

# IMPORT AND PREPARE EVENT LOG

# Import log
raw_event_log <- read.csv("evaluation_files/event_log_1.csv", header = TRUE, sep = ";")

# Transform event log to wide format
temp <- raw_event_log %>% filter(activity != "no_activity")
# make sure that arrival events have the instance resource in their resource column
case_act_comb <- unique(temp[,c('case_id', 'activity')])
pb <- txtProgressBar(min = 1, max = nrow(case_act_comb), style = 3)
for(i in 1:nrow(case_act_comb)){
  events <- as.data.frame(temp %>% filter(case_id == case_act_comb$case_id[i], activity == case_act_comb$activity[i]))
  if(nrow(events) == 3){
    index <- temp$case_id == case_act_comb$case_id[i] & temp$activity == case_act_comb$activity[i]
    temp$resource[index] <- events$resource[2]
    remove(index)
  }
  setTxtProgressBar(pb, i)
}
close(pb)
remove(pb)


temp$two_ts_formats <- paste(temp$timestamp, temp$timestamp_date, sep = "-")
temp$timestamp <- NULL
temp$timestamp_date <- NULL
event_log <- reshape(temp, timevar = "event_type", idvar = c("activity","case_id", "resource"), direction = "wide")
colnames(event_log) <- c("activity", "case_id", "resource", "arrival_two_formats", "start_two_formats", "complete_two_formats")
event_log <- separate(data = event_log, col = arrival_two_formats, into = c("arrival_proxy", "arrival_proxy_with_date"), sep ="-", remove = TRUE)
event_log <- separate(data = event_log, col = start_two_formats, into = c("start", "start_with_date"), sep ="-", remove = TRUE)
event_log <- separate(data = event_log, col = complete_two_formats, into = c("complete", "complete_with_date"), sep ="-", remove = TRUE)
event_log$activity <- as.character(event_log$activity)
event_log$resource <- as.character(event_log$resource)

event_log$arrival_proxy <- as.numeric(event_log$arrival_proxy)
event_log$start <- as.numeric(event_log$start)
event_log$complete <- as.numeric(event_log$complete)
event_log$date <- as.character(reshape::colsplit(event_log$start_with_date, split=" ", names=c("start_date", "remove"))$start_date)

# Remove instances that have not been completed
event_log <- event_log %>% filter(!is.na(complete))

# Rename as activity_log
activity_log <- event_log

# Remove clutter
remove(event_log, events, case_act_comb, temp, i)

# SELECT ACTIVITY LOG TIME SEGMENT

select_activity_log_time_segment <- function(activity_log, factor){
  
  activity_log$date <- dmy(activity_log$date)
  timespan <- as.numeric(max(activity_log$date) - min(activity_log$date))
  last_day_to_include <- min(activity_log$date) + ceiling(timespan * factor)
  activity_log <- activity_log %>% filter(date <= last_day_to_include)
  
  return(activity_log)
}

if(factor < 1){
  activity_log <- select_activity_log_time_segment(activity_log, factor)
}

#######################################################################
## STAGE 2: RETRIEVE DAILY AVAILABILITY RECORDS
#######################################################################

dar <- create_dar(activity_log, aggr_tol_pct = 0.10, single_queue = TRUE, timestamp_format = "dmy_hms")
dar <- dar %>% working_day() %>% impute_unknown_periods()


#######################################################################
## REMOVE LAST DAY FROM BOTH ACTIVITY LOG AND DAR
#######################################################################

# dar <- dar %>% filter(date != "2016-07-31")
# activity_log <- activity_log %>% filter(date != "31/07/2016")

dar <- dar %>% filter(date != "2016-03-27")
activity_log <- activity_log %>% filter(date != "27/03/2016")


#######################################################################
## STAGE 3: RETRIEVE RESOURCE SCHEDULES USING DAR
#######################################################################

# # Create DAR-copy: DO NOT EXECUTE!!!
#dar_backup <- dar

# Determine timespan of original dar
dar <- dar_backup
write.table(dar, "dar_full.csv", sep = ";", row.names = FALSE)
timespan <- as.numeric(max(dar$date) - min(dar$date))

# Prepare generation of resource schedules

# prefaces <- c("100_1", "100_2", "100_3", "100_4", "100_5",
#               "50_1", "50_2", "50_3", "50_4", "50_5",
#               "25_1", "25_2", "25_3", "25_4", "25_5")
# 
# dar_factor <- c(1,1,1,1,1,
#                 0.50,0.50,0.50,0.50,0.50,
#                 0.25,0.25,0.25,0.25,0.25)

prefaces <- c("100_1", "100_2", "100_3", "100_4", "100_5",
              "100_6", "100_7", "100_8", "100_9", "100_10",
              "100_11", "100_12", "100_13", "100_14", "100_15",
              "100_16", "100_17", "100_18", "100_19", "100_20")

dar_factor <- rep(1,20)

# Generate resource schedules

pb <- txtProgressBar(min = 1, max = length(prefaces), style = 3)
for(p in 1:length(prefaces)){
  
  print_statement <- paste("Creating schedules for preface", p, "of", length(prefaces), sep=" ")
  print(print_statement)
  
  # Determine whether dar should be subsetted
  if(p > 1 && dar_factor[p] < 1){
    
    if(dar_factor[p] != dar_factor[p-1]){ #dar_factor changed so subsetting is required
      last_day_to_include <- min(dar$date) + ceiling(timespan * dar_factor[p])
      dar <- dar %>% filter(date <= last_day_to_include)
      
      n <- paste("dar_", prefaces[p], ".csv", sep = "")
      write.table(dar, n, sep = ";", row.names = FALSE)
    }
  }
  
  # METHOD 1: DIRECT SAMPLING
  
  # Create "readable" schedule
  res_schedule_sampled_ds <- retrieve_resource_schedule_sampled(dar, "day", schedule_n_of_weeks, FALSE, TRUE)
  n <- paste("schedule_ds_", prefaces[p], "_Rformat", ".csv", sep = "")
  write.table(res_schedule_sampled_ds, n, sep = ";", row.names = FALSE)

  # Create SimPy-schedule
  res_schedule_sampled_list <- convert_sample_to_list(res_schedule_sampled_ds)
  n <- paste("schedule_ds_", prefaces[p], ".csv", sep = "")
  write.table(res_schedule_sampled_list, n, sep = ";", row.names = FALSE)
  
  # METHOD 2: CLUSTER-BASED SAMPLING
  
  # Create "readable" schedule
  res_schedule_sampled_cbs <- retrieve_resource_schedule_clustered(dar, "day", schedule_n_of_weeks, 1.00, TRUE)
  n <- paste("schedule_cbs_", prefaces[p], "_Rformat", ".csv", sep = "")
  write.table(res_schedule_sampled_cbs, n, sep = ";", row.names = FALSE)

  # Create SimPy-schedule
  res_schedule_sampled_list <- convert_sample_to_list(res_schedule_sampled_cbs)
  n <- paste("schedule_cbs_", prefaces[p], ".csv", sep = "")
  write.table(res_schedule_sampled_list, n, sep = ";", row.names = FALSE)
  
  setTxtProgressBar(pb, p)
}
close(pb)


# OLD CODE
# # Save "old" dar
# dar_25 <- dar
# res_schedule_sampled_cbs_25_4 <- res_schedule_sampled_cbs
# res_schedule_sampled_ds_25_4 <- res_schedule_sampled_ds
# 
# # SAMPLE DAR (when required)
# if(dar_factor < 1){
#   last_day_to_include <- min(dar$date) + ceiling(timespan * dar_factor)
#   dar <- dar %>% filter(date <= last_day_to_include)
# }
# 
# # METHOD 1: DIRECT SAMPLING
# 
# # Create "readable" schedule
# res_schedule_sampled_ds <- retrieve_resource_schedule_sampled(dar, "day", schedule_n_of_weeks, FALSE, TRUE)
# write.table(res_schedule_sampled_ds, "schedule_ds.csv", sep = ";", row.names = FALSE)
# 
# # Create SimPy-schedule
# res_schedule_sampled_list <- convert_sample_to_list(res_schedule_sampled_ds)
# write.table(res_schedule_sampled_list, "schedule_ds.csv", sep = ";", row.names = FALSE)
# 
# # METHOD 2: CLUSTER-BASED SAMPLING
# 
# # Create "readable" schedule
# res_schedule_sampled_cbs <- retrieve_resource_schedule_clustered(dar, "day", schedule_n_of_weeks, 1.00, TRUE)
# 
# # Create SimPy-schedule
# res_schedule_sampled_list <- convert_sample_to_list(res_schedule_sampled_cbs)
# write.table(res_schedule_sampled_list, "schedule_cbs.csv", sep = ";", row.names = FALSE)




#######################################################################
## STAGE 4: CALCULATE PROCESS PERFORMANCE MEASURES USING EVENT LOG
#######################################################################

# Prepare results dataframe
# type <- c(rep("real", n_of_replications), rep("mined_ds", n_of_replications), rep("mined_cbs", n_of_replications))
# rep_nr <- c(rep(seq(1,n_of_replications), 3))
results <- data.frame(log_type = NA, dar_cut = NA, replication = NA, stat_type = NA, key = NA, key2 = NA, value = NA)

# Prepare read-in of files: comment out the two lines that apply

# first_part_log_file_name <- "evaluation_files/simulated_log_real_schedule_repl_"
# first_part_sched_file_name <- "evaluation_files/simulated_schedule_real_schedule_repl_"

# first_part_log_file_name <- "evaluation_files/simulated_log_mined_schedule_ds_"
# first_part_sched_file_name <- "evaluation_files/simulated_schedule_mined_schedule_ds_"

 first_part_log_file_name <- "evaluation_files/simulated_log_mined_schedule_cbs_"
# first_part_sched_file_name <- "evaluation_files/simulated_schedule_mined_schedule_cbs_"

if(grepl("real", first_part_log_file_name)){
  n_replications <- 1
  dar_cuts <- c(NA)
  n_dar_cuts <- 1
} else{
  #dar_cut <- c(100,50,25)
  dar_cuts <- c(100)
  n_dar_cuts <- as.numeric(length(dar_cuts))
  #n_replications <- 5 #number of times a schedule is generated and the resulting schedule is used to simulate the process
  n_replications <- 20
}


# Fill results dataframe

for(c in 1:n_dar_cuts){
  
  # Print progress
  print_statement <- paste("Analyzing dar cut", c, "of", n_dar_cuts, sep=" ")
  print(print_statement)
  
  for(l in 1:n_replications){
    
    # Print progress
    print_statement <- paste("Analyzing event log from replication", l, "of", n_replications, sep=" ")
    print(print_statement)
    
    # Determine file names
    if(grepl("real", first_part_log_file_name)){
      log_file <- paste(first_part_log_file_name, l, ".csv", sep = "")
      #schedule_file <- paste(first_part_sched_file_name, l, ".csv", sep = "")
    } else{
      log_file <- paste(first_part_log_file_name, dar_cuts[c],"_", l, "_repl_1", ".csv", sep = "")
      #schedule_file <- paste(first_part_sched_file_name, dar_cuts[c], "_repl_", l, ".csv", sep = "")
    }
    
    # Import files
    raw_event_log <- read.csv(log_file, header = TRUE, sep = ";")
    #applied_schedule <- read.csv(schedule_file, header = TRUE, sep = ";")
    
    ## TRANSFORM EVENT LOG TO ACTIVITY LOG
    
    temp <- raw_event_log %>% filter(activity != "no_activity")
    # make sure that arrival events have the instance resource in their resource column
    case_act_comb <- unique(temp[,c('case_id', 'activity')])
    pb <- txtProgressBar(min = 1, max = nrow(case_act_comb), style = 3)
    for(i in 1:nrow(case_act_comb)){
      events <- as.data.frame(temp %>% filter(case_id == case_act_comb$case_id[i], activity == case_act_comb$activity[i]))
      if(nrow(events) == 3){
        index <- temp$case_id == case_act_comb$case_id[i] & temp$activity == case_act_comb$activity[i]
        temp$resource[index] <- events$resource[2]
        remove(index)
      }
      setTxtProgressBar(pb, i)
    }
    close(pb)
    
    temp$two_ts_formats <- paste(temp$timestamp, temp$timestamp_date, sep = "-")
    temp$timestamp <- NULL
    temp$timestamp_date <- NULL
    event_log <- reshape(temp, timevar = "event_type", idvar = c("activity","case_id", "resource"), direction = "wide")
    colnames(event_log) <- c("activity", "case_id", "resource", "arrival_two_formats", "start_two_formats", "complete_two_formats")
    event_log <- separate(data = event_log, col = arrival_two_formats, into = c("arrival_proxy", "arrival_proxy_with_date"), sep ="-", remove = TRUE)
    event_log <- separate(data = event_log, col = start_two_formats, into = c("start", "start_with_date"), sep ="-", remove = TRUE)
    event_log <- separate(data = event_log, col = complete_two_formats, into = c("complete", "complete_with_date"), sep ="-", remove = TRUE)
    event_log$activity <- as.character(event_log$activity)
    event_log$resource <- as.character(event_log$resource)
    
    event_log$arrival_proxy <- as.numeric(event_log$arrival_proxy)
    event_log$start <- as.numeric(event_log$start)
    event_log$complete <- as.numeric(event_log$complete)
    event_log$date <- as.character(reshape::colsplit(event_log$start_with_date, split=" ", names=c("start_date", "remove"))$start_date)
    
    # Remove instances that have not been completed
    event_log <- event_log %>% filter(!is.na(complete))
    
    # Rename as activity_log
    activity_log <- event_log
    remove(event_log)
    
    # Convert activity_log timestamps
    activity_log$start_with_date <- dmy_hms(activity_log$start_with_date)
    activity_log$complete_with_date <- dmy_hms(activity_log$complete_with_date)
    activity_log$arrival_proxy_with_date <- dmy_hms(activity_log$arrival_proxy_with_date)
    
    
    ## CONVERT SCHEDULE INPUT FILE TO ANALYZABLE FORMAT
    
    # real_schedule_file <- reshape(applied_schedule, timevar = "period_type", idvar = c("period_id","resource"), direction = "wide")
    # real_schedule_file$status <- "unavailable"
    # colnames(real_schedule_file) <- c("period_id", "resource", "period_start", "period_start_with_date", "period_end", "period_end_with_date", "status")
    # real_schedule_file$period_start_with_date <- as.character(real_schedule_file$period_start_with_date)
    # real_schedule_file$period_end_with_date <- as.character(real_schedule_file$period_end_with_date)
    # real_schedule_file$start_date <- as.character(reshape::colsplit(real_schedule_file$period_start_with_date, split=" ", names=c("start_date", "remove"))$start_date)
    # real_schedule_file$end_date <- as.character(reshape::colsplit(real_schedule_file$period_end_with_date, split=" ", names=c("end_date", "remove"))$end_date)
    # 
    # # add available periods (currently, real_schedule_file only contains unavailable periods)
    # real_schedule_file <- as.data.frame(real_schedule_file %>% arrange(resource, period_start))
    # 
    # n_unav_rows <- nrow(real_schedule_file)
    # 
    # for(i in 2:n_unav_rows){
    #   
    #   if(real_schedule_file$resource[i-1] == real_schedule_file$resource[i] && 
    #      real_schedule_file$period_end[i-1] != real_schedule_file$period_start[i]){
    #     
    #     next_row <- nrow(real_schedule_file) + 1
    #     
    #     real_schedule_file[next_row, "period_id"] <- max(real_schedule_file$period_id) + 1 
    #     real_schedule_file[next_row, "resource"] <- real_schedule_file$resource[i]
    #     real_schedule_file[next_row, "period_start"] <- real_schedule_file$period_end[i-1]
    #     real_schedule_file[next_row, "period_start_with_date"] <- real_schedule_file$period_end_with_date[i-1]
    #     real_schedule_file[next_row, "period_end_with_date"] <- real_schedule_file$period_start_with_date[i]
    #     real_schedule_file[next_row, "period_end"] <- real_schedule_file$period_start[i]
    #     real_schedule_file[next_row, "start_date"] <- real_schedule_file$end_date[i-1]
    #     real_schedule_file[next_row, "end_date"] <- real_schedule_file$start_date[i]
    #     real_schedule_file[next_row, "status"] <- "available"
    #   }
    # }
    # 
    # real_schedule_file <- as.data.frame(real_schedule_file %>% arrange(resource, period_start))
    # 
    # # split up periods that cross a "day boundary"
    # index <- real_schedule_file$start_date != real_schedule_file$end_date 
    # real_schedule_file$change_required[index] <- TRUE
    # remove(index)
    # 
    # change_rows <- real_schedule_file %>% filter(change_required == TRUE)
    # n_change_rows <- nrow(change_rows)
    # 
    # for(i in 1:n_change_rows){ 
    #   change_row_next <- nrow(change_rows) + 1
    #   change_rows[change_row_next, "period_id"] <- change_rows$period_id[i]
    #   change_rows[change_row_next, "resource"] <- change_rows$resource[i]
    #   change_rows[change_row_next, "period_start"] <- change_rows$period_start[i]
    #   change_rows[change_row_next, "period_start_with_date"] <- change_rows$period_start_with_date[i]
    #   change_rows[change_row_next, "period_end_with_date"] <- paste(change_rows$start_date[i],"23:59:59")
    #   change_rows[change_row_next, "period_end"] <- determine_numeric_time_value(change_rows$period_end_with_date[change_row_next])
    #   change_rows[change_row_next, "start_date"] <- change_rows$start_date[i]
    #   change_rows[change_row_next, "end_date"] <- change_rows$start_date[i]
    #   change_rows[change_row_next, "status"] <- change_rows$status[i]
    #   
    #   change_row_next <- nrow(change_rows) + 1
    #   change_rows[change_row_next, "period_id"] <- change_rows$period_id[i]
    #   change_rows[change_row_next, "resource"] <- change_rows$resource[i]
    #   change_rows[change_row_next, "period_start_with_date"] <- paste(change_rows$end_date[i],"00:00:00")
    #   change_rows[change_row_next, "period_start"] <- determine_numeric_time_value(change_rows$period_start_with_date[change_row_next])
    #   change_rows[change_row_next, "period_end_with_date"] <- change_rows$period_end_with_date[i]
    #   change_rows[change_row_next, "period_end"] <- change_rows$period_end[i]
    #   change_rows[change_row_next, "start_date"] <- change_rows$end_date[i]
    #   change_rows[change_row_next, "end_date"] <- change_rows$end_date[i]
    #   change_rows[change_row_next, "status"] <- change_rows$status[i]
    # }
    # 
    # change_rows <- change_rows %>% filter(is.na(change_required))
    # 
    # real_schedule_file <- real_schedule_file %>% filter(is.na(change_required))
    # real_schedule_file <- rbind(real_schedule_file, change_rows)
    # 
    # real_schedule_file <- as.data.frame(real_schedule_file %>% select(resource, start_date, period_start_with_date, period_end_with_date, 
    #                                                                   period_start, period_end, status) %>% 
    #                                       rename(date = start_date, period_start = period_start_with_date, period_end = period_end_with_date, period_start_num = period_start,
    #                                              period_end_num = period_end) %>% arrange(resource, period_start_num))
    # 
    # real_schedule_file$date <- as.Date(real_schedule_file$date, "%d/%m/%Y")
    # real_schedule_file$period_start <- dmy_hms(real_schedule_file$period_start)
    # real_schedule_file$period_end <- dmy_hms(real_schedule_file$period_end)
    # 
    # applied_schedule <- real_schedule_file
    
    
    ## REMOVE LAST DAY FROM SCHEDULE AND ACTIVITY LOG
    
    #applied_schedule <- applied_schedule %>% filter(date != max(date))
    activity_log$date <- dmy(activity_log$date)
    activity_log <- activity_log %>% filter(date != max(date))
    
    
    ## PERFORMANCE MEASURE CALCULATION
    
    
    # Prepare filling results dataframe by determining type of activity log
    if(grepl("real", first_part_log_file_name)){
      log_type <- "real"
    } else if(grepl("ds", first_part_log_file_name)){
      log_type <- "ds"
    } else{
      log_type <- "cbs"
    }
    
    # OUTPUT MEASURE: number of processed cases
    output <- activity_log %>% group_by(activity) %>% summarize(n_cases = n())
    
    next_row <- nrow(results) + 1
    end_row <- next_row + nrow(output) - 1
    
    results[next_row:end_row, "log_type"] <- log_type
    results[next_row:end_row, "dar_cut"] <- dar_cut[c]
    results[next_row:end_row, "replication"] <- l
    results[next_row:end_row, "stat_type"] <- "n_processed_cases"
    results[next_row:end_row, "key"] <- output$activity
    results[next_row:end_row, "value"] <- output$n_cases
    
    # OUTPUT MEASURE: flow time summary statistics
    output <- activity_log %>% group_by(case_id) %>% 
      summarize(first_start = min(arrival_proxy_with_date), last_complete = max(complete_with_date)) %>%
      mutate(flow_time = as.numeric(difftime(last_complete, first_start, units = "mins"))) %>%
      summarize(min = min(flow_time),
                max = max(flow_time),
                mean = mean(flow_time),
                median = median(flow_time),
                sd = sd(flow_time),
                q1 = quantile(flow_time, 0.25),
                q3 = quantile(flow_time, 0.75))
    
    for(i in 1:ncol(output)){
      next_row <- nrow(results) + 1
      
      results[next_row, "log_type"] <- log_type
      results[next_row, "dar_cut"] <- dar_cut[c]
      results[next_row, "replication"] <- l
      results[next_row, "stat_type"] <- "flow_time"
      results[next_row, "key"] <- names(output)[i]
      results[next_row, "value"] <- as.numeric(output[1, i])
      
    }
    
    # OUTPUT MEASURE: waiting time summary statistics
    output <- activity_log %>% mutate(act_waiting_time = as.numeric(difftime(start_with_date, arrival_proxy_with_date, units = "mins"))) %>%
      group_by(case_id) %>% summarize(waiting_time = sum(act_waiting_time)) %>% ungroup() %>%
      summarize(min = min(waiting_time),
                max = max(waiting_time),
                mean = mean(waiting_time),
                median = median(waiting_time),
                sd = sd(waiting_time),
                q1 = quantile(waiting_time, 0.25),
                q3 = quantile(waiting_time, 0.75))
    
    for(i in 1:ncol(output)){
      next_row <- nrow(results) + 1
      
      results[next_row, "log_type"] <- log_type
      results[next_row, "dar_cut"] <- dar_cut[c]
      results[next_row, "replication"] <- l
      results[next_row, "stat_type"] <- "waiting_time"
      results[next_row, "key"] <- names(output)[i]
      results[next_row, "value"] <- as.numeric(output[1, i])
      
    }
    
    # OUTPUT MEASURE: number of processed activity instances per resource
    output <- activity_log %>% group_by(resource) %>% summarize(n_act_inst = n())
    
    next_row <- nrow(results) + 1
    end_row <- next_row + nrow(output) - 1
    
    results[next_row:end_row, "log_type"] <- log_type
    results[next_row:end_row, "dar_cut"] <- dar_cut[c]
    results[next_row:end_row, "replication"] <- l
    results[next_row:end_row, "stat_type"] <- "n_processed_act_instances"
    results[next_row:end_row, "key"] <- output$resource
    results[next_row:end_row, "value"] <- output$n_act_inst
    
    # OUTPUT MEASURE: resource utilization
    
    # # Determine total time that a resource is available according to the specified schedule
    # 
    # applied_schedule$dur <- as.numeric(difftime(applied_schedule$period_end, applied_schedule$period_start, units = "mins"))
    # total_available_time <- as.data.frame(applied_schedule %>% filter(status == "available") %>% group_by(resource, date) %>% summarize(avail_time = sum(dur)))
    # total_available_time$date <- as.character(total_available_time$date)
    # total_available_time$resource <- as.character(total_available_time$resource)
    # 
    # 
    # # Determine resource daily active time
    # 
    # active_time <- activity_log %>% mutate(duration = complete_with_date - start_with_date) %>%
    #   group_by(resource, date) %>% summarize(total_active_time = sum(duration))
    # active_time$date <- as.character(active_time$date)
    # active_time$total_active_time <- as.numeric(active_time$total_active_time)
    # 
    # # Determine resource utilization
    # res_util <- dplyr::left_join(total_available_time, active_time, by = c("resource", "date"))
    # 
    # # replace NA values by 0 active time
    # index <- is.na(res_util$total_active_time)
    # res_util$total_active_time[index] <- 0
    # remove(index)
    # 
    # # calculate resource utilization
    # res_util$resource_utilization <- res_util$total_active_time / res_util$avail_time
    # 
    # remove(active_time, total_available_time)
    # 
    # output <- res_util %>% summarize(mean = mean(resource_utilization),
    #                                  sd = sd(resource_utilization),
    #                                  median = median(resource_utilization),
    #                                  min = min(resource_utilization),
    #                                  max = max(resource_utilization),
    #                                  q1 = quantile(resource_utilization, 0.25),
    #                                  q3 = quantile(resource_utilization, 0.75)) 
    # 
    # for(i in 1:ncol(output)){
    #   next_row <- nrow(results) + 1
    #   
    #   results[next_row, "log_type"] <- log_type
    #   results[next_row, "dar_cut"] <- dar_cut[c]
    #   results[next_row, "replication"] <- l
    #   results[next_row, "stat_type"] <- "res_utilz"
    #   results[next_row, "key"] <- names(output)[i]
    #   results[next_row, "value"] <- as.numeric(output[1, i])
    #   
    # }
    # 
    # output <- res_util %>% group_by(resource) %>% summarize(mean = mean(resource_utilization),
    #                                                         sd = sd(resource_utilization),
    #                                                         median = median(resource_utilization),
    #                                                         min = min(resource_utilization),
    #                                                         max = max(resource_utilization),
    #                                                         q1 = quantile(resource_utilization, 0.25),
    #                                                         q3 = quantile(resource_utilization, 0.75))
    # 
    # for(j in 1:nrow(output)){
    #   
    #   for(i in 2:ncol(output)){
    #     next_row <- nrow(results) + 1
    #     
    #     results[next_row, "log_type"] <- log_type
    #     results[next_row, "dar_cut"] <- dar_cut[c]
    #     results[next_row, "replication"] <- l
    #     results[next_row, "stat_type"] <- "res_utilz"
    #     results[next_row, "key"] <- names(output)[i]
    #     results[next_row, "key2"] <- output$resource[j]
    #     results[next_row, "value"] <- as.numeric(output[j, i])
    #     
    #   }
    # }
  }
  
}





#######################################################################
## FINAL WORKSPACE CLEAN-UP
#######################################################################

# Remove first row in results table
results <- results[-1,]

# Remove clutter
remove(n_replications, output, raw_event_log, res_util, change_rows, case_act_comb)
remove(end_row,change_row_next,next_row,i,j,l,log_file,first_part_log_file_name,first_part_sched_file_name)
remove(pb,print_statement,n_change_rows,schedule_file)
remove(c, dar_cut,l, dar_factor, last_day_to_include, n_dar_cuts,timespan, n, p)



#######################################################################
## ANALYZE RESULTS
#######################################################################

#results_backup <- results

results_wv <- results

# Restructure results_wv
results_wv <- as.data.frame(dcast(results_wv, stat_type + key + key2 + replication ~ log_type + dar_cut, value.var = "value"))

# Add real schedule results to all
results_wv <- as.data.frame(results_wv %>% group_by(stat_type, key, key2) %>% mutate(real_NA = real_NA[1]))


# Select first row for each performance metric
#results_wv <- results_wv %>% group_by(stat_type, key, key2) %>% filter(row_number() == 1)

# Determine percentage deviation from reality
results_wv$ds_100_real <- abs((results_wv$ds_100 - results_wv$real_NA)/results_wv$real_NA) * 100
#results_wv$ds_50_real <- abs((results_wv$ds_50 - results_wv$real_NA)/results_wv$real_NA) * 100
#results_wv$ds_25_real <- abs((results_wv$ds_25 - results_wv$real_NA)/results_wv$real_NA) * 100
results_wv$cbs_100_real <- abs((results_wv$cbs_100 - results_wv$real_NA)/results_wv$real_NA) * 100
#results_wv$cbs_50_real <- abs((results_wv$cbs_50 - results_wv$real_NA)/results_wv$real_NA) * 100
#results_wv$cbs_25_real <- abs((results_wv$cbs_25 - results_wv$real_NA)/results_wv$real_NA) * 100

# Average over replications

results_wv_summ <- results_wv %>% group_by(stat_type, key, key2) %>%
                      summarize(avg_ds_100_real = mean(ds_100_real),
                                med_ds_100_real = median(ds_100_real),
                                sd_ds_100_real = sd(ds_100_real),
                                avg_cbs_100_real = mean(cbs_100_real),
                                med_cbs_100_real = median(cbs_100_real),
                                sd_cbs_100_real = sd(cbs_100_real),
                                min_ds_100_real = min(ds_100_real),
                                min_cbs_100_real = min(cbs_100_real),
                                max_ds_100_real = max(ds_100_real),
                                max_cbs_100_real = max(cbs_100_real))

# results_wv_summ <- results_wv %>% group_by(stat_type, key, key2) %>%
#                       summarize(avg_ds_100_real = mean(ds_100_real),
#                                 avg_ds_50_real = mean(ds_50_real),
#                                 avg_ds_25_real = mean(ds_25_real),
#                                 avg_cbs_100_real = mean(cbs_100_real),
#                                 avg_cbs_50_real = mean(cbs_50_real),
#                                 avg_cbs_25_real = mean(cbs_25_real))

# Average over replications

# results_wv <- results_wv %>% 
#                   group_by(log_type, stat_type, key, key2) %>% 
#                   summarize(mean = mean(value)) %>%
#                   arrange(stat_type, key, key2)

# Determine how close sampling method approximates real schedule outputs

results_wv <- results_wv %>% 
                  arrange(stat_type,key,key2) %>%
                  group_by(stat_type,key,key2) %>%
                  mutate(real_mean = mean[3], diff = abs(mean - real_mean)) %>%
                  mutate(max_diff = max(diff))

results_wv$closest_to_real <- 0
index <- results_wv$diff == results_wv$max_diff
results_wv$closest_to_real[index] <- 1
remove(index)

results_wv %>% filter(closest_to_real == 1) %>% group_by(log_type) %>% summarize(n = n())
results_wv %>% filter(closest_to_real == 1, log_type == "real")

# Create summary for average flow time
results_wv %>% filter(stat_type == "flow_time", key == "median")

# Paired t-test




# Integrated the code shown above
# # Note: read in event log and convert to activity log using the first part of this script
# activity_log$start_with_date <- dmy_hms(activity_log$start_with_date)
# activity_log$complete_with_date <- dmy_hms(activity_log$complete_with_date)
# activity_log$arrival_proxy_with_date <- dmy_hms(activity_log$arrival_proxy_with_date)
# 
# # NUMBER OF PROCESSED CASES
# activity_log %>% group_by(activity) %>% summarize(n_cases = n())
# 
# # FLOW TIME SUMMARY STATISTICS
# activity_log %>% group_by(case_id) %>% 
#                   summarize(first_start = min(start_with_date), last_complete = max(complete_with_date)) %>%
#                   mutate(flow_time = as.numeric(difftime(last_complete, first_start, units = "mins"))) %>%
#                   summarize(min = min(flow_time),
#                             max = max(flow_time),
#                             mean = mean(flow_time),
#                             median = median(flow_time),
#                             sd = sd(flow_time),
#                             q1 = quantile(flow_time, 0.25),
#                             q3 = quantile(flow_time, 0.75))
# 
# # WAITING TIME SUMMARY STATISTICS
# activity_log %>% mutate(waiting_time = as.numeric(difftime(start_with_date, arrival_proxy_with_date, units = "mins"))) %>%
#                   summarize(min = min(waiting_time),
#                             max = max(waiting_time),
#                             mean = mean(waiting_time),
#                             median = median(waiting_time),
#                             sd = sd(waiting_time),
#                             q1 = quantile(waiting_time, 0.25),
#                             q3 = quantile(waiting_time, 0.75))
# 
# # NUMBER OF PROCESSED ACTIVITY INSTANCES PER RESOURCE
# activity_log %>% group_by(resource) %>% summarize(n_act_inst = n())
# activity_log %>% group_by(resource, activity) %>% summarize(n_act_inst = n())
# 
# # RESOURCE UTILIZATION
# 
# # Determine total time that a resource is available according to the specified schedule
# 
# applied_schedule$dur <- as.numeric(difftime(applied_schedule$period_end, applied_schedule$period_start, units = "mins"))
# total_available_time <- as.data.frame(applied_schedule %>% filter(status == "available") %>% group_by(resource, date) %>% summarize(avail_time = sum(dur)))
# total_available_time$date <- as.character(total_available_time$date)
# total_available_time$resource <- as.character(total_available_time$resource)
# 
# 
# # Determine resource daily active time
# 
# active_time <- activity_log %>% mutate(duration = complete_with_date - start_with_date) %>%
#                   group_by(resource, date) %>% summarize(total_active_time = sum(duration))
# active_time$date <- as.character(dmy(active_time$date))
# active_time$total_active_time <- as.numeric(active_time$total_active_time)
# 
# # Determine resource utilization
# res_util <- dplyr::left_join(total_available_time, active_time, by = c("resource", "date"))
# 
# # replace NA values by 0 active time
# index <- is.na(res_util$total_active_time)
# res_util$total_active_time[index] <- 0
# remove(index)
# 
# # calculate resource utilization
# res_util$resource_utilization <- res_util$total_active_time / res_util$avail_time
# 
# remove(active_time, total_available_time)
# 
# res_util %>% summarize(mean = mean(resource_utilization),
#                                             sd = sd(resource_utilization),
#                                             median = median(resource_utilization),
#                                             min = min(resource_utilization),
#                                             max = max(resource_utilization),
#                                             q1 = quantile(resource_utilization, 0.25),
#                                             q3 = quantile(resource_utilization, 0.75)) 
# 
# res_util %>% group_by(resource) %>% summarize(mean = mean(resource_utilization),
#                                                                    sd = sd(resource_utilization),
#                                                                    median = median(resource_utilization),
#                                                                    min = min(resource_utilization),
#                                                                    max = max(resource_utilization),
#                                                                    q1 = quantile(resource_utilization, 0.25),
#                                                                    q3 = quantile(resource_utilization, 0.75)) 
  


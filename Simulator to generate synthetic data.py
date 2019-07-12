# -*- coding: utf-8 -*-
"""
RESOURCE AVAILABILITY CALENDARS, implementation synthetic log generator

@author: Niels Martin
"""
# IMPORT PACKAGES

from simpy import Environment, FilterStore, PriorityResource
from simpy.events import Event, AnyOf, AllOf
import random
from math import ceil
from copy import deepcopy
from time import gmtime, strftime

# EVENT LOG GENERATION
# This code generates event logs. The generated event logs contain 
# the timestamp of an event, the case id, the activity, the event type and the 
# resource executing an activity.

# Variables that need to be specified
n_logs = 1 # number of event logs that should be generated
n_ent = 100000 # number of entities (cases) that can maximally be included in a single event log
postpone_interruptions = False # boolean indicating whether periods of interruption are postponed when they cannot start at the time indicated by the schedule

# Fill out one of the following to reflect the simulation run duration (fill out
# integer digits)
n_of_minutes = None
n_of_hours = None
n_of_days = None
n_of_weeks = 25

# Calculation of sim_run_duration
if n_of_minutes is not None:
    sim_run_duration = n_of_minutes
elif n_of_hours is not None:
    sim_run_duration = n_of_hours * 60
elif n_of_days is not None:
    sim_run_duration = n_of_days * 1440
elif n_of_weeks is not None:
    sim_run_duration = n_of_weeks * 10080
else:
    raise ValueError("Simulation run duration not specified")

# Event log generation log

for k in range(n_logs):

    # Prepare event log file
    print("WRITING LOG " + str(k+1) + " OF " + str(n_logs) + "...")
    file_name = "event_log_" + str(k+1) + ".csv"
    event_log = open(file_name, "w")
    event_log.write("timestamp;activity;case_id;event_type;resource;timestamp_date" + "\n")
    event_log.close()
    
    # Prepare real schedule file    
    file_name = "real_schedule_" + str(k+1) + ".csv"
    real_schedule_file = open(file_name, "w")
    real_schedule_file.write("period_id;resource;ts_num;ts_with_date;period_type" + "\n")
    real_schedule_file.close()
        
    # Supporting function: Activity generator function
    # Generator function postpones activity execution in several stages:
    # 1. Postpone activity execution until its start event is triggered (i.e.
    # when the previous activity for the case under consideration is executed)
    # 2. Postpone activity execution until the resource under consideration 
    # becomes available. 
    
    def act_generator(act_name, res_list):  
        
        def act(env, startevent, fdur, eid):
            
#            print("%s;%s;%d;initialize" % (env.now, act_name, eid))
        
            # postpone activity execution until its preceding activity is executed
            yield startevent
            
            # record the arrival of a case at an activity            
            file_name = "event_log_" + str(k+1) + ".csv"
            event_log = open(file_name, "a")
#            print("%s;%s;%d;start;%s" % (env.now, act_name, eid, res.name))
            event_log.write(str(env.now) + ";" + str(act_name) + ";" +
                            str(eid) + ";arrival;" + "no_res" + ";" +
                            str(strftime("%d/%m/%Y %H:%M:%S", gmtime(1451606400 + env.now * 60))) + "\n")
                                
            event_log.close()
            
            # select resource and postpone activity execution until resource 
            # becomes available
            res = yield res_list.get()
            # activity resource requests have lower priority than schedule resource requests
            req = res.request(priority = 1) 
            yield req
            
            # record start event
            file_name = "event_log_" + str(k+1) + ".csv"
            event_log = open(file_name, "a")
#            print("%s;%s;%d;start;%s" % (env.now, act_name, eid, res.name))
            event_log.write(str(env.now) + ";" + str(act_name) + ";" +
                            str(eid) + ";start;" + str(res.name) + ";" +
                            str(strftime("%d/%m/%Y %H:%M:%S", gmtime(1451606400 + env.now * 60))) + "\n")
                                
            event_log.close()
                      
            # activity execution
            yield env.timeout(fdur())
            
            # record complete event
            file_name = "event_log_" + str(k+1) + ".csv"
            event_log = open(file_name, "a")
#            print("%s;%s;%d;complete;%s" % (env.now, act_name, eid, res.name))
            event_log.write(str(env.now) + ";" + str(act_name) + ";" +
                            str(eid) + ";complete;" + str(res.name) + ";" +
                            str(strftime("%d/%m/%Y %H:%M:%S", gmtime(1451606400 + env.now * 60))) + "\n")
            event_log.close()
        
            # release resource
            res.release(req)
            res_list.put(res)
    
        return act
    
    
    # Supporting function: Schedule control
    
    def schedule_control(env, res, log_nr):

        for i in range(len(res.schedule)):
            if i == len(res.schedule):
                yield env.timeout(float("inf"))
            
            if env.now < res.schedule[i][0]:
                # schedule resource requests have higher priority than activity resource requests
                req = res.request(priority = 0)
                yield req
                
                # record unavailability period start                
                file_name = "real_schedule_" + str(log_nr+1) + ".csv"
                real_schedule_file = open(file_name, "a")
                real_schedule_file.write(str(i+1) + ";" + str(res.name) + ";" +
                            str(env.now) + ";" + 
                            str(strftime("%d/%m/%Y %H:%M:%S", gmtime(1451606400 + env.now * 60))) + ";unav_period_start" + "\n")
                real_schedule_file.close()
                
                if res.postpone == False or i == 0:
                    # due to excessive activity durations, a situation can occur in which a break is "skipped" when interruptions are not postponed
                    if res.schedule[i][0] > env.now:
                        yield env.timeout(res.schedule[i][0] - env.now)
                    else:
                        print("Warning! Period of interruption skipped for resource " + str(res.name) +
                                ". Period of interruption should have started at time " + str(res.schedule[i][0]) + ", but running activity execution ended at time " + 
                                str(env.now) + ".")
                else:
                    yield env.timeout(res.schedule[i][0] - res.schedule[i-1][1])
                                
                res.release(req)
                
                # record unavailability period end                
                file_name = "real_schedule_" + str(log_nr+1) + ".csv"
                real_schedule_file = open(file_name, "a")
                real_schedule_file.write(str(i+1) + ";" + str(res.name) + ";" +
                            str(env.now) + ";" + 
                            str(strftime("%d/%m/%Y %H:%M:%S", gmtime(1451606400 + env.now * 60))) + ";unav_period_end" + "\n")
                real_schedule_file.close()                
                
                
                yield env.timeout(res.schedule[i][1] - res.schedule[i][0])
    
       
    # Supporting function: Function to create resources
    
    def res_creation(env, name, user_specified_schedule, repetition_factor,postpone_interruptions):
        res = PriorityResource(env)
        res.name = name
        res.schedule = []
        res.event_list = []
        
        res.postpone = postpone_interruptions

        if repetition_factor == "none":
            res.schedule = user_specified_schedule
        elif repetition_factor == "daily":
            res.schedule = user_specified_schedule
            n_schedule_elements = len(user_specified_schedule)
            #n_of_repetitions = int(ceil(sim_run_duration / 1440)) - 1
            n_of_repetitions = int(ceil(sim_run_duration / 1440)) + 1
            for i in range(n_of_repetitions):
                res.schedule = res.schedule + deepcopy(user_specified_schedule)
            j = 0
            for i in range(len(res.schedule)):
                res.schedule[i][0] = res.schedule[i][0] + (j * 1440)
                res.schedule[i][1] = res.schedule[i][1] + (j * 1440)
                if (i + 1) % len(user_specified_schedule) == 0:
                    j = j + 1  
        elif repetition_factor == "weekly":
            res.schedule = user_specified_schedule
            n_schedule_elements = len(user_specified_schedule)
            #n_of_repetitions = int(ceil(sim_run_duration / 10080)) - 1
            n_of_repetitions = int(ceil(sim_run_duration / 10080)) + 1
            for i in range(n_of_repetitions):
                res.schedule = res.schedule + deepcopy(user_specified_schedule)
            j = 0
            for i in range(len(res.schedule)):
                res.schedule[i][0] = res.schedule[i][0] + (j * 10080)
                res.schedule[i][1] = res.schedule[i][1] + (j * 10080)
                if (i + 1) % len(user_specified_schedule) == 0:
                    j = j + 1  
        else:
            raise ValueError("No valid repetition factor is specified")
        
        return(res)

    
    # Supporting function: Arrival period generation

    def arrival_period_creation(user_specified_arrival_period, repetition_factor):
        
        if repetition_factor == "none":
            arrival_period = user_specified_arrival_period
        elif repetition_factor == "daily":
            arrival_period = user_specified_arrival_period
            n_elements = len(user_specified_arrival_period)
            n_of_repetitions = int(ceil(sim_run_duration / 1440)) - 1
            for i in range(n_of_repetitions):
                arrival_period = arrival_period + deepcopy(user_specified_arrival_period)
            j = 0
            for i in range(len(arrival_period)):
                arrival_period[i][0] = arrival_period[i][0] + (j * 1440)
                arrival_period[i][1] = arrival_period[i][1] + (j * 1440)
                if (i + 1) % len(user_specified_arrival_period) == 0:
                    j = j + 1  
        elif repetition_factor == "weekly":
            arrival_period = user_specified_arrival_period
            n_elements = len(user_specified_arrival_period)
            n_of_repetitions = int(ceil(sim_run_duration / 10080)) - 1
            for i in range(n_of_repetitions):
                arrival_period = arrival_period + deepcopy(user_specified_arrival_period)
            j = 0
            for i in range(len(arrival_period)):
                arrival_period[i][0] = arrival_period[i][0] + (j * 10080)
                arrival_period[i][1] = arrival_period[i][1] + (j * 10080)
                if (i + 1) % len(user_specified_arrival_period) == 0:
                    j = j + 1   
        else:
            raise ValueError("No valid repetition factor is specified")
        
        return(arrival_period)
    
    # Supporting function: Generate random schedule

    def generate_random_schedule(morning_available_prob, morning_break_prob, 
                                 afternoon_available_prob, afternoon_break_prob):
        
        schedule = []
        
        #determine if resource is available in the morning
        morning = random.uniform(0,1)
        
        if morning <= morning_available_prob: #resource works in morning
            morning_start = random.randint(480,570)
            morning_end = random.randint(690,750)

            #determine if resource takes morning break
            take_break = random.uniform(0,1)
            if take_break <= morning_break_prob: # resource takes morning break
               break_dur = random.randint(5,20)
               break_start = random.randint(morning_start,morning_end - break_dur - 1)
               #add to schedule
               schedule.append([morning_start,break_start])
               schedule.append([break_start + break_dur,morning_end])
            else: # resource does not take morning break
               schedule.append([morning_start,morning_end])
               
            #determine if resource is available in the afternoon
               
            afternoon = random.uniform(0,1)
            
            if afternoon <= afternoon_available_prob: # resource works in the afternoon
                afternoon_start = random.randint(780,840)
                afternoon_end = random.randint(990,1110)
                
                #determine if resource takes afternoon break
                take_break = random.uniform(0,1)
                if take_break <= afternoon_break_prob: # resource takes afternoon break
                    break_dur = random.randint(5,20)
                    break_start = random.randint(afternoon_start,afternoon_end - break_dur - 1)
                    #add to schedule
                    schedule.append([afternoon_start,break_start])
                    schedule.append([break_start + break_dur,afternoon_end])
                else:  #resource does not take afternoon break
                    schedule.append([afternoon_start,afternoon_end])
            
        else: #resource does not work in morning
            afternoon_start = random.randint(780,840)
            afternoon_end = random.randint(990,1110)
            
            #determine if resource takes afternoon break
            take_break = random.uniform(0,1)
            if take_break <= afternoon_break_prob: # resource takes afternoon break
                break_dur = random.randint(5,20)
                break_start = random.randint(afternoon_start,afternoon_end - break_dur - 1)
                #add to schedule
                schedule.append([afternoon_start,break_start])
                schedule.append([break_start + break_dur,afternoon_end])
            else:  #resource does not take afternoon break
                schedule.append([afternoon_start,afternoon_end])
        
        return(schedule)
    
    
    # Create SimPy environment
    
    env = Environment()
    
    # Determine resource schedules
    avail_in_morning_prob = 0.90
    break_in_morning_prob = 0.60
    avail_in_afternoon_prob = 0.80
    break_in_afternoon_prob = 0.80
    r1_schedule = generate_random_schedule(avail_in_morning_prob, break_in_morning_prob, avail_in_afternoon_prob, break_in_afternoon_prob)
    r2_schedule = generate_random_schedule(avail_in_morning_prob, break_in_morning_prob, avail_in_afternoon_prob, break_in_afternoon_prob)
    r3_schedule = generate_random_schedule(avail_in_morning_prob, break_in_morning_prob, avail_in_afternoon_prob, break_in_afternoon_prob)
    r4_schedule = generate_random_schedule(avail_in_morning_prob, break_in_morning_prob, avail_in_afternoon_prob, break_in_afternoon_prob)
    r5_schedule = generate_random_schedule(avail_in_morning_prob, break_in_morning_prob, avail_in_afternoon_prob, break_in_afternoon_prob)
    r6_schedule = generate_random_schedule(avail_in_morning_prob, break_in_morning_prob, avail_in_afternoon_prob, break_in_afternoon_prob)
    r7_schedule = generate_random_schedule(avail_in_morning_prob, break_in_morning_prob, avail_in_afternoon_prob, break_in_afternoon_prob)
   
    # Resource specification
    # Besides the resource name, a resource schedule and a repetition factor
    # needs to be specified. The repetition factor can have values "none", "daily",
    # or "weekly". "None" indicates that the entire schedule is user-defined.
    # "Daily" indicates that the user-defined schedule needs to be repeated
    # every day (i.e. a daily schedule is provided by the user). "Weekly" 
    # indicates that the user-defined schedule needs to be repeated every week
    # (i.e. a weekly schedule is provided by the user). Finally, the postpone_interruptions
    # parameter is passed to indicate whether interruptions need to be postponed when they
    # cannot start at the pre-specified time according to the schedule. 
    r1 = res_creation(env, 'r1', r1_schedule, "daily", postpone_interruptions)
    r2 = res_creation(env, 'r2', r2_schedule, "daily", postpone_interruptions)
    r3 = res_creation(env, 'r3', r3_schedule, "daily", postpone_interruptions)
    r4 = res_creation(env, 'r4', r4_schedule, "daily", postpone_interruptions)
    r5 = res_creation(env, 'r5', r5_schedule, "daily", postpone_interruptions)
    r6 = res_creation(env, 'r6', r6_schedule, "daily", postpone_interruptions)
    r7 = res_creation(env, 'r7', r7_schedule, "daily", postpone_interruptions)
    
    # Assignment of resources to activities

    resources_A = FilterStore(env, capacity = 1)
    resources_A.items = [r1]
    resources_B = FilterStore(env, capacity = 1)
    resources_B.items = [r2,r6]
    resources_C = FilterStore(env, capacity = 1)
    resources_C.items = [r3]
    resources_D = FilterStore(env, capacity = 1)
    resources_D.items = [r4]
    resources_E = FilterStore(env, capacity = 1)
    resources_E.items = [r5,r4]
    resources_F = FilterStore(env, capacity = 1)
    resources_F.items = [r6]
    resources_G = FilterStore(env, capacity = 1)
    resources_G.items = [r7,r3]
    
    # Activity specification
    
    act_A = act_generator(act_name = 'A', res_list = resources_A)
    act_B = act_generator(act_name = 'B', res_list = resources_B)
    act_C = act_generator(act_name = 'C', res_list = resources_C)
    act_D = act_generator(act_name = 'D', res_list = resources_D)
    act_E = act_generator(act_name = 'E', res_list = resources_E)
    act_F = act_generator(act_name = 'F', res_list = resources_F)
    act_G = act_generator(act_name = 'G', res_list = resources_G)
    
    # Specify activity durations
    
    def dur_A(entity=None):
        def fdur():
            return (random.triangular(1, 8, 6))
        return fdur
    
    def dur_B(entity=None):
        def fdur():
            return (random.triangular(2, 7, 5))
        return fdur
    
    def dur_C(entity=None):
        def fdur():
            return (random.triangular(4, 13, 9))
        return fdur
    
    def dur_D(entity=None):
        def fdur():
            return (random.triangular(4, 11, 6))
        return fdur
    
    def dur_E(entity=None):
        def fdur():
            return (random.triangular(7, 11, 9))
        return fdur
    
    def dur_F(entity=None):
        def fdur():
            return (random.triangular(2, 6, 4))
        return fdur
    
    def dur_G(entity=None):
        def fdur():
            return (random.triangular(1, 5, 3))
        return fdur
    
    # Specify XOR-construct
    
    def xor1(event_yes, event_no):
        def xor(event):
            x = random.uniform(0,1)
    
            if x < 0.5:
                event_yes.succeed()
            else:
                event_no.succeed()
                
        return xor
    
    # Entity creation class
    
    class Entity:
        def __init__(self, env, arrival_time, eid):
                self.eid = eid
                
#                start_event = env.timeout(arrival_time)
                start_event = env.timeout(0)
                xor1_yes = Event(env)
                xor1_no = Event(env)
                
                end_act_A = env.process(act_A(env, start_event, dur_A(self), self.eid))
                end_act_B = env.process(act_B(env, end_act_A, dur_B(self), self.eid))
                end_act_B.callbacks.append(xor1(xor1_yes, xor1_no))
                end_act_C = env.process(act_C(env, xor1_yes, dur_C(self), self.eid))
                end_act_D = env.process(act_D(env, end_act_C, dur_D(self), self.eid))
                end_act_E = env.process(act_E(env, xor1_no, dur_E(self), self.eid))
                end_act_F = env.process(act_F(env, AnyOf(env, [end_act_D, end_act_E]), dur_F(self), self.eid))
                env.process(act_G(env, end_act_F, dur_G(self), self.eid))
    
    env.process(schedule_control(env, r1, k))
    env.process(schedule_control(env, r2, k))
    env.process(schedule_control(env, r3, k))
    env.process(schedule_control(env, r4, k))
    env.process(schedule_control(env, r5, k))
    env.process(schedule_control(env, r6, k))
    env.process(schedule_control(env, r7, k)) 
    
    
    def entity_generator(env): 
        periods_of_arrival = arrival_period_creation([[480,1020]], "daily")
        
        for i in range(n_ent):
            while (len(periods_of_arrival) > 0) & (env.now >= periods_of_arrival[0][1]):
                del periods_of_arrival[0]
                
                if len(periods_of_arrival) == 0:
                        yield env.timeout(float("inf"))
    
            if env.now < periods_of_arrival[0][0]:
                yield env.timeout(periods_of_arrival[0][0] - env.now)
                arrival_time = periods_of_arrival[0][0]
                
            iat = random.expovariate(1.0/6)
            yield env.timeout(iat)
            
            arrival_time += iat
            
            Entity(env, arrival_time, i + 1)
            
            file_name = "event_log_" + str(k+1) + ".csv"
            event_log = open(file_name, "a")
#            print("%s;no_act;%d;arrival;no_res" % (arrival_time, i + 1))
            event_log.write(str(env.now) + ";no_activity;" +
                            str(i + 1) + ";arrival;no_res;" +
                            str(strftime("%d/%m/%Y %H:%M:%S", gmtime(1451606400 + arrival_time * 60))) + "\n")
            event_log.close()

    env.process(entity_generator(env))

    env.run(until = sim_run_duration) # runs the model for sim_run_duration
    # specified simulation duration is required to run the code without errors (due to infinity postponing in activity generator function)
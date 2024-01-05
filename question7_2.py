import sys

import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
import time
import re
import os
import matplotlib.pyplot as plt
import time


#Utility functions
    

#### Driver program
    
localVar = 10
conf = SparkConf().setAppName("question7").setMaster("local["+ str(localVar) + "]").setAppName("question6").set("spark.driver.memory", "6g")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

#remove output file if it already exists
os.system("rm -rf ./results/question7")
os.system("mkdir ./results/question7")

#Depends on the file, we load the CSV file
# wholeFile1 = sc.textFile("./data/task_events/*.csv")
wholeFile1 = sc.textFile("./data/task_events/part-0000*-of-00500.csv")
# wholeFile2 = sc.textFile("./data/task_usage/*.csv")
wholeFile2 = sc.textFile("./data/task_usage/part-0000*-of-00500.csv")

#TASK EVENTS
# 0 -> timestamp,
# 1 -> missing info,
# 2 -> job ID,
# 3 -> task index,
# 4 -> machine ID,
# 5 -> event type,
# 6 -> user name,
# 7 -> scheduling class,
# 8 -> priority,
# 9 -> resource request for CPU cores,
# 10 -> resource request for RAM,
# 11 -> resource request for local disk space,
# 12 -> different-machine constraint

#TASK USAGE
# 0 -> start time,
# 1 -> end time,
# 2 -> job ID,
# 3 -> task index,
# 4 -> machine ID,
# 5 -> CPU rate,
# 6 -> canonical memory usage,
# 7 -> assigned memory usage,
# 8 -> unmapped page cache,
# 9 -> total page cache,
# 10 -> maximum memory usage,
# 11 -> disk I/O time,
# 12 -> local disk space usage,
# 13 -> maximum CPU rate of task,
# 14 -> maximum disk IO time,
# 15 -> cycles per instruction (CPI),
# 16 -> memory accesses per instruction (MAI),
# 17 -> sample portion,
# 18 -> aggregation type,
# 19 -> sampled CPU usage


entries1 = wholeFile1.map(lambda x: x.split(',')).cache()
entries2 = wholeFile2.map(lambda x: x.split(',')).cache()

start = time.time()

# Get number of task evition per machine
evicted_events_per_machine = entries1.filter(lambda x: x[5] == "2").map(lambda x: (x[4], 1)).reduceByKey(lambda x, y: x + y).cache()

# We retrieve the machine with the most task evictions
most_evicted_machine = evicted_events_per_machine.max(lambda x: x[1])[0]

#most_evicted_machine = "446225856"

# We retrieve timestamps of evict events 
timestamps_event_type_2 = entries1.filter(lambda x: x[4] == most_evicted_machine and x[5] == "2").map(lambda x: x[0]).collect()

# We retrieve CPU and memory usage
usage_CPU_memory = entries2.filter(lambda x: x[4] == most_evicted_machine and x[0] != '' and x[5] != '' and x[6] != '').map(lambda x: (x[0], (x[5], x[6])))



end = time.time()
duration = end - start


# Plot of the graph
timestamps_cpu_memory = usage_CPU_memory.map(lambda x: x[0]).collect()
cpu_usage = usage_CPU_memory.map(lambda x: x[1][0]).collect()
memory_usage = usage_CPU_memory.map(lambda x: x[1][1]).collect()


plt.figure(figsize=(10, 6)) 

# CPU usage
cpu_line, = plt.plot(timestamps_cpu_memory, cpu_usage, label='CPU Usage', marker='o', linestyle='-', color='green', alpha=0.5)
# Memory usage
memory_line, = plt.plot(timestamps_cpu_memory, memory_usage, label='Memory Usage', marker='x', linestyle='-', color='blue', alpha=0.5)
# Tasks evicted marks
for timestamp in timestamps_event_type_2:
    plt.axvline(x=timestamp, color='red', linestyle='-', linewidth=1.5)

plt.axis('off')
plt.title('CPU and memory usage over time')
plt.legend(handles=[cpu_line, memory_line, plt.Line2D([], [], color='red', linestyle='-', label='Task evicted event')], 
           labels=['CPU usage', 'Memory usage', 'Task evicted event'])

plt.tight_layout()
fig1 = plt.gcf()
plt.show()

fig1.savefig('./results/question7/task_eviction_events.png')

with open("./results/question7/computationTimes_2.txt", "w") as f:
    f.write(str(duration)+ "\n")



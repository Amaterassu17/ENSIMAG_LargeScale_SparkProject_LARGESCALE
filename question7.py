import sys

import findspark
findspark.init()

from pyspark import SparkContext
import time
import re
import os
import matplotlib.pyplot as plt
import time


#Utility functions
    

#### Driver program
    
localVar = 10
sc = SparkContext("local["+ str(localVar) + "]")
sc.setLogLevel("ERROR")

#remove output file if it already exists
os.system("rm -rf ./results/question7")
os.system("mkdir ./results/question7")

#Depends on the file, we load the CSV file
wholeFile1 = sc.textFile("./data/task_events/*.csv")
wholeFile2 = sc.textFile("./data/task_usage/*.csv")

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




#keep the RDD in memory

start = time.time()
table1 = entries1.filter(lambda x: x[9]!='' and x[5] == '2').map(lambda x: ((x[2], x[3], x[4]), (x[0], x[1])))
table2 = entries2.map(lambda x: ((x[2], x[3], x[4]), (x[13], x[10], x[14])))

table = table1.join(table2).map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][1][0], x[1][1][1], x[1][1][2]))).sortBy(lambda x: x[1][2], ascending=False).sortBy(lambda x: x[1][1], ascending=False).sortBy(lambda x: x[1][0], ascending=False).take(200)
end = time.time()

duration = end - start

with open("./results/question7/joinedTablesSortedByCPU.csv", "w") as f:
    f.write("jobId,taskIndex,machineId,startTime, endTime, maxCPU, maxMem, maxDisk\n")
    for line in table:
        f.write(line[0][0]+","+line[0][1]+","+line[0][2]+","+line[1][0]+","+line[1][1]+","+line[1][2]+","+line[1][3]+","+line[1][4]+"\n")

with open("./results/question7/computationTimes.txt", "w") as f:
    f.write(str(duration))


sc.stop()

input("Press enter to exit ;)")

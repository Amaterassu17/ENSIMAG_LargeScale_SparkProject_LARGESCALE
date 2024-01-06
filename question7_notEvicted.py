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
os.system("rm ./results/question7/averageCPU_NonEvictMachines.png")
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




#keep the RDD in memory

# start = time.time()
# table1 = entries1.filter(lambda x: x[9]!='' and  x[5] == "2").map(lambda x: ((x[2], x[3], x[4]), (x[0], x[5])))
# # table2 = entries2.filter(lambda x: x[19]!= "0").map(lambda x: ((x[2], x[3], x[4]), (x[19], x[10], x[14])))
# table2 = entries2.filter(lambda x: x[19]!= "0").map(lambda x: ((x[2], x[3], x[4]), (x[19])))

# #table = table1.join(table2).map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][1][0]))).sortBy(lambda x: x[1][2], ascending=False).sortBy(lambda x: x[1][1], ascending=False).sortBy(lambda x: x[1][0], ascending=False).take(200)
# table = table1.join(table2).map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][1][0]))).cache()
# end = time.time()

# #step1
# #for a timestamp there could be multiple evictione events, we reduce by key by averaging the number of CPU sampled and then from this see if >= 7 (statistically for us high usage) and then from this we see how many evicted events are effectively there
# step1 = table.map(lambda x: (x[0], (x[1][2])))

# duration = end - start





# with open("./results/question7/joinedTablesSortedByCPU_NonEvict.csv", "w") as f:
#     f.write("jobId,taskIndex,machineId,timestamp,event_type, sampledCPU\n")
#     for line in table:
#         f.write(line[0][0]+","+line[0][1]+","+line[0][2]+","+line[1][0]+","+line[1][1]+","+line[1][2]+"\n")

# with open("./results/question7/computationTimes.txt", "w") as f:
#     f.write(str(duration)+ "\n")



table1 = entries1.filter(lambda x: x[9]!='' and x[5] != "2").map(lambda x: ((x[2], x[3], x[4]), (x[0])))
table2 = entries2.map(lambda x: ((x[2], x[3], x[4]), ((x[5]))))

table = table1.join(table2).map(lambda x: ((x[0][0],x[0][1],x[0][2],x[1][1][0]), (x[1][0][0], 1))).reduceByKey(lambda x,y: (float(x[0])+float(y[0]), x[1]+y[1])).map(lambda x: (x[0][2], float(x[1][0])/float(x[1][1])))
# table = table1.join(table2).map(lambda x: ((x[0][0],x[0][1],x[0][2],x[1][1][0]), (x[1][0][0], 1))).take(1)

#map with counter for all events and a counter that is 1 only if x[1] >= 7
step2 = table.map(lambda x: (x[0], (x[1],1, 1 if x[1] >= 7 else 0))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2])).map(lambda x: (int(x[0]), float(x[1][2])/float(x[1][1]))).sortBy(lambda x: x[0], ascending=True).collect()


# graph it with matplotlib
print(step2[0:100])

plt.locator_params(axis='x', nbins=10)
# plot results in a 3 different line graphs with requested CPU, CPU

machine_id, average_evict_CPURATE = zip(*step2)


plt.plot(machine_id, average_evict_CPURATE, marker='', color='r', ls='-')
plt.xlabel('machineID Ascending')
plt.ylabel('averageCPU usage')
plt.title('Not Evicted events machine average CPU usage')
plt.savefig("./results/question7/averageCPU_NonEvictMachines.png")
plt.show()

# print(step2)

# with open("./results/question7/avgCPU.csv", "w") as f:
#     f.write("jobId,taskIndex,machineId,avgCPU\n")
#     for line in table:
#         f.write(line[0][0]+","+line[0][1]+","+line[0][2]+","+str(line[1])+"\n")
# sc.stop()



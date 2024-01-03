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

def findCol(firstLine, name):
    if name in firstLine:
        return firstLine.index(name)
    else:
        return -1
    
#### Driver program

localVar = 10
sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

#remove output file if it already exists
os.system("rm -rf ./results/question5")
os.system("mkdir ./results/question5")
#Depends on the file, we load the CSV file
wholeFile = sc.textFile("./data/task_events/*.csv")

entries = wholeFile.map(lambda x: x.split(','))

#keep the RDD in memory
entries.cache()


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

start_time = time.time()
jobs_count = entries.map(lambda x: (x[2])).distinct().count()


tasks = entries.map(lambda x: (x[2], x[4], x[3])).distinct()

tasks_per_job = tasks.map(lambda x: ((x[0]), [x[1]])).reduceByKey(lambda x, y: x + y)

def same_machine_checker(machines, threshold):
    return len(set(machines)) == threshold
    

thresholds = range(1, 21)
results = []
for threshold in thresholds:
    res = tasks_per_job.mapValues(lambda machine: same_machine_checker(machine, threshold))
    # Counting true values, meaning the machines used for the job are the same 
    true_count = res.filter(lambda x: x[1] == True).count()
    results.append(100.0 * true_count / jobs_count)

end_time = time.time()

duration = end_time - start_time

with open("./results/question5/time_computation.txt", "w") as f:
    f.write(str(duration) + " s")

with open("./results/question5/thresholds_results.txt", "w") as f:
    for i in range(len(thresholds)):
        f.write(str(thresholds[i]) + " " + str(results[i]) + "\n")

plt.bar(thresholds, results, align='center', alpha=0.5, color='green')

plt.xlabel('Number of unique machines')
plt.ylabel('Percentage of jobs having their tasks running on same machine(s)')
plt.title('Percentage of jobs\' tasks on the same machine(s) for different number of unique machines')
plt.xticks(thresholds)

plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()
plt.savefig('./results/question5/percentage_of_jobs_tasks_on_same_machines.png')



sc.stop()



input("Press enter to exit ;)")

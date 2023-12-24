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
    
sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

#remove output file if it already exists
os.system("rm -rf ./results/question5")

#Depends on the file, we load the CSV file
wholeFile = sc.textFile("./data/task_events/*.csv")

#The first line of the file defines the name of each column in the cvs file
#We store it as an array in the driver program


#WE HAVE TO CHANGE SOMETHING HERE ETI ;)
#firstLine =wholeFile.filter(lambda x: "RecID" in x).collect()[0].replace('"', '').split(',')

#filter out the first line from the initial RDD
# entries = wholeFile.filter(lambda x: not ("RecID" in x))

#split each line into an array of items
entries = wholeFile.map(lambda x: x.split(','))

#keep the RDD in memory
entries.cache()


#### HERE THE FUN BEGINS
# 0 -> time
# 1 -> machine ID
# 2 -> event type
# 3 -> platform ID
# 4 -> capacity: number of CPUs
# 5 -> memory

jobs_count = entries.map(lambda x: (x[2])).distinct().count()

# job_and_machines = entries.map(lambda x: ((x[2], x[4]), 1)).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0][0], x[1])).reduceByKey(lambda x,y: max(x,y)).map(lambda x: (x[0], (x[1], 1))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])).map(lambda x: (x[0], int(x[1][0]/x[1][1]))).sortByKey().take(10)
# print(job_and_machines)

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


plt.bar(thresholds, results, align='center', alpha=0.5, color='green')

plt.xlabel('Number of unique machines')
plt.ylabel('Percentage of jobs having their tasks running on same machine(s)')
plt.title('Percentage of jobs\' tasks on the same machine(s) for different number of unique machines')
plt.xticks(thresholds)

plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()




sc.stop()







input("Press enter to exit ;)")

import sys

import findspark
findspark.init()

from pyspark import SparkContext
import time
import re
import os
import matplotlib.pyplot as plt
import time
from statistics import mean, median



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

directory = "./results/question9/"
if not os.path.exists(directory):
    os.makedirs(directory)

start_time = time.time()

task_events_filtered = wholeFile.filter(lambda x: x[9] != ',' and x[10] != ',') # We remove lines with empty values

relevant_columns = task_events_filtered.map(lambda x: (int(x[8]), float(x[9]), float(x[10]))) # extraction of Priority, CPU request and Memory request

grouped_by_priority = relevant_columns.groupBy(lambda x: x[0]) # We group data by priority

# Now we collect CPU and memory request for each priority in a list
list_by_priority = grouped_by_priority.mapValues(lambda values: (
    len(values),
    [v[1] for v in values], # List of CPU request for one priority
    [v[2] for v in values]  # List of Memory request for one priority
))

# Finally we calculate the statistics we are interested in and we sort the RDD by priority
statistics_by_priority = list_by_priority.map(lambda x: (
    x[0],  # priority
    x[1][0],  # total tasks
    mean(x[1][1]),  # CPU request mean
    median(x[1][1]),  # CPU request median
    mean(x[1][2]),  # memory request mean
    median(x[1][2])  # memory request median
))
sorted_statistics = statistics_by_priority.sortBy(lambda x: x[0])

end_time = time.time()

duration = end_time - start_time

with open("./results/question9/time_computation.txt", "w") as f:
    f.write(str(duration) + " s")



# We plot the data

data = sorted_statistics.collect()

priorities = [x[0] for x in data]
cpu_mean = [x[2] for x in data]
cpu_median = [x[3] for x in data]
mem_mean = [x[4] for x in data]
mem_median = [x[5] for x in data]

# CPU stats plot
plt.figure(figsize=(10, 6))

bar_width = 0.4
index = range(len(priorities))

plt.bar([i - 0.5 * bar_width for i in index], cpu_mean, width=bar_width, alpha=0.5, color='green', label='CPU Request Mean')
plt.bar([i + 0.5 * bar_width for i in index], cpu_median, width=bar_width, alpha=1, color='green', label='CPU Request Median')

plt.xlabel('Priorities')
plt.ylabel('Values')
plt.title('CPU Request Statistics by Priority')
plt.legend()
plt.xticks(index, priorities)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()

plt.savefig("./results/question9/cpu_req_by_prio.png")
plt.show()

########################

# Memory stats plot
plt.figure(figsize=(10, 6))

bar_width = 0.4
index = range(len(priorities))

plt.bar([i - 0.5 * bar_width for i in index], mem_mean, width=bar_width, alpha=0.5, color='blue', label='Memory Request Mean')
plt.bar([i + 0.5 * bar_width for i in index], mem_median, width=bar_width, alpha=1, color='blue', label='Memory Request Median')

plt.xlabel('Priorities')
plt.ylabel('Values')
plt.title('Memory Request Statistics by Priority')
plt.legend()
plt.xticks(index, priorities)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()

plt.savefig("./results/question9/mem_req_by_prio.png")
plt.show()


sc.stop()







input("Press enter to exit ;)")

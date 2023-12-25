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
#localVar = *
sc = SparkContext("local[" + str(localVar) + "]")
sc.setLogLevel("ERROR")

#remove output file if it already exists
os.system("rm -rf ./results/question1")
os.system("mkdir ./results/question1")

#Depends on the file, we load the CSV file
wholeFile = sc.textFile("./data/machine_events/*.csv")


#split each line into an array of items
entries = wholeFile.map(lambda x: x.split(','))

#keep the RDD in memory
entries.cache()



# 0 -> time
# 1 -> machine ID
# 2 -> event type
# 3 -> platform ID
# 4 -> capacity: number of CPUs
# 5 -> memory

start_time = time.time()
cpus_mapped = entries.map(lambda x: (int(x[1]),x[4])).distinct().map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[0]).cache();
elapsed_time = time.time() - start_time

#map result into a list
cpus_mapped.saveAsTextFile("./results/question1/splitResults")
cpus_mapped = cpus_mapped.collect()

cpu_capabilities, quantities = zip(*cpus_mapped)

print(cpus_mapped)
print("Computation time: " + str(round(elapsed_time, 2)) + "s")

with open('./results/question1/time_computation' + str(localVar) + '.txt', 'w') as f:
    f.write("TimeComputation: " + str(round(elapsed_time, 2)) + "s")


#Graph part
plt.bar(cpu_capabilities, quantities, align='center', alpha=0.5, color='green')
# plt.hist(cpus_mapped, bins ='auto', alpha=0.7, color='b', label = 'CPU capabilities')
plt.xlabel('CPU capabilities')
plt.ylabel('Frequency')
plt.title('CPU capabilities histogram')
# plt.legend(loc='upper right')
plt.savefig('./results/question1/cpu_capabilities.png')


sc.stop()







input("Press enter to exit ;)")

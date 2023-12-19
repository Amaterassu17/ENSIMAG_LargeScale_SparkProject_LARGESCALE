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
    
def extract_column(line, column_index):
    # Split the line into a list of values
    values = line.split(',')
    
    # Return the desired column based on the index
    return values[column_index]
    

#### Driver program
    
sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

#remove output file if it already exists
os.system("rm -rf ./results/question4")

#Depends on the file, we load the CSV file
wholeFile = sc.textFile("./data/task_events/part-00001-of-00500.csv")

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

start_time = time.time()

#just to try
scheduled_events = entries.map( lambda x: ((int(x[2]),int(x[3]), int(x[7])), (int(x[5]), 0))).cache()

def function1(x, y):
    if (x[0] == 2 or y[0] == 2):
        return (x[0], 1)
    else:
        return (x[0], x[1])

step1 = scheduled_events.reduceByKey(function1).cache()

step2 = step1.map(lambda x : (x[0][2], 1)).reduceByKey(lambda x, y: x+y).sortBy(lambda x: x[0]).collect()
step3= step1.map(lambda x: (x[0][2], x[1][1])).reduceByKey(lambda x,y : x+1).sortBy(lambda x: x[0]).collect()

print(step2)
print(step3)

#for every scheduling class we compute the probability of eviction

probabilities = []
for i in range(len(step2)):
    probabilities.append(step3[i][1]/step2[i][1])

print(probabilities)

# The probabilities for all the scheduling are
# [0.0014897236153862004, 0.0016651736250952215, 0.002602749797808985, 0.010275224566534364] to mupltiply by 100 to get the percentage
#It was tough



#example = scheduled_events.filter( lambda x: x[0][0] == 3418309 and x[0][1] == 1 ).collect()



# scheduling_and_eventtype = entries.map(lambda x: (x[7], x[5])).cache()
# evicted_events = scheduling_and_eventtype.filter(lambda x: x[0] == "2").cache()

# count_number_tasks_per_scheduling_class = evicted_events.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y).cache()
# count_events_per_scheduling = evicted_events.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y).cache()

elapsed_time = time.time() - start_time

#map result into a list


# plt.bar(cpu_capabilities, quantities, align='center', alpha=0.5, color='green')
# # plt.hist(cpus_mapped, bins ='auto', alpha=0.7, color='b', label = 'CPU capabilities')
# plt.xlabel('CPU capabilities')
# plt.ylabel('Frequency')
# plt.title('CPU capabilities histogram')
# # plt.legend(loc='upper right')
# plt.savefig('./results/question1/cpu_capabilities.png')


sc.stop()







input("Press enter to exit ;)")

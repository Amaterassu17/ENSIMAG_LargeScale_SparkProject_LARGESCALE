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

def function1(x,y):

    timestamp_0 = x[0]
    timestamp_1 = y[0]
    event_type_0 = x[1]
    event_type_1 = y[1]
    count_0 = x[2]
    count_1 = y[2]

    if(timestamp_1 > timestamp_0 and event_type_0 == 1 and event_type_1 == 0):
        return (timestamp_1, event_type_1, count_1+ (timestamp_1 - timestamp_0))
    else:
        return (timestamp_1, event_type_1, count_1)
    

def function2(x,y):

    #print(x)
    #print(y)

    timestamp_0 = x[0]
    timestamp_1 = y[0]
    event_type_0 = x[1]
    event_type_1 = y[1]
    count_0 = x[2]
    count_1 = y[2]
    cpu_0 = x[3]
    cpu_1 = y[3]

    if(timestamp_1 > timestamp_0 and event_type_0 == 1 and event_type_1 == 0):
        return (timestamp_1, event_type_1, count_1+ (timestamp_1 - timestamp_0)* cpu_1, cpu_1)
    else:
        return (timestamp_1, event_type_1, count_1, cpu_1)


#### Driver program
    
localVar = 10
#localVar = *
sc = SparkContext("local[" + str(localVar) + "]")
sc.setLogLevel("ERROR")

#remove output file if it already exists
os.system("rm -rf ./results/question2")
os.system("mkdir ./results/question2")

#Depends on the file, we load the CSV file
wholeFile = sc.textFile("./data/machine_events/*.csv")

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

# count the total number of machines






# #this code supposes that every line after the first 12K zeros is with a unique timestamp.
# #At the beginning of the file, there are 12K lines with 0 as timestamp
# #supposedly, we can say that the computational power of the machines is 100% at the beginning of the file
# #After that we go through the RDD by ordered timestamps and we count the number of machines that are active at each timestamp


#step1 is used by timestamps and by machine count so it's normal to split with cache()

start1=time.time()
step1 = entries.map(lambda x : ((int(x[0])), (int(x[1]),int(x[2])))).cache()
timestamps = step1.filter(lambda x: x[0] != 0).sortBy(lambda x: x[0]).cache()
timestamps_maximum = timestamps.sortBy(lambda x: x[0], False).first()[0]


machine_count = step1.filter(lambda x: x[0] == 0).map(lambda x: x[1][0]).distinct().count()

total_timestamps = timestamps_maximum * machine_count
#print(total_timestamps)
machine_and_timestamps = timestamps.map(lambda x: (x[1][0], (x[0], x[1][1], 0))).cache()
something = machine_and_timestamps.reduceByKey(function1).map(lambda x: (None, x[0])).reduceByKey(lambda x,y: x+y).map(lambda x: x[1]).collect()[0]
downtime_percentage = 100 * something / total_timestamps
end1=time.time()



start2 = time.time()
step1_alt = entries.filter(lambda x: x[4] != '').map(lambda x : ((int(x[0])), (int(x[1]),int(x[2]),float(x[4])))).cache()
timestamps_1 = step1_alt.filter(lambda x: x[0] != 0).sortBy(lambda x: x[0]).cache()
#distribution of machine power computation as computed in question 1  ('' -> 32, '0.25' -> 123, '0.5' --> 11632, '1' -> 796)
total_timestamps_weighted = timestamps_maximum * 32 * 0 + timestamps_maximum * 0.25 * 123 + timestamps_maximum * 0.5 * 11632 + timestamps_maximum * 1 * 796
machine_and_timestamps_2 = timestamps_1.map(lambda x: (x[1][0], (x[0], x[1][1], 0, x[1][2]))).cache()
something_2 = machine_and_timestamps_2.reduceByKey(function2).map(lambda x: (None, x[0])).reduceByKey(lambda x,y: x+y).map(lambda x: x[1]).collect()[0]
downtime_percentage_2 = 100 * something_2 / total_timestamps
end2 = time.time()


duration1 = end1-start1
duration2 = end2-start2

print(downtime_percentage)
print(downtime_percentage_2)

with open("./results/question2/DownTimePercentage", 'w') as f:
    f.write("DowntimePercentage without CPU weights: " + str(downtime_percentage)+ "\n")
    f.write("DowntimePercentage with CPU weights: " + str(downtime_percentage_2) + "\n\n" )
    f.write("Times of 2 computations: " + str(duration1) + "s, " + str(duration2) + "s")



sc.stop()







input("Press enter to exit ;)")

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
    
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

#remove output file if it already exists
os.system("rm -rf ./results/question2")

#Depends on the file, we load the CSV file
wholeFile = sc.textFile("./data/machine_events/part-00000-of-00001.csv").cache()

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

# machines = wholeFile.map(lambda x: int(extract_column(x,1))).distinct().cache()
# count_machines= machines.count()
# on_off_dict = {key: 0 for key in machines.collect()}

# print("count_machines: ", count_machines)





# #this code supposes that every line after the first 12K zeros is with a unique timestamp.
# #At the beginning of the file, there are 12K lines with 0 as timestamp
# #supposedly, we can say that the computational power of the machines is 100% at the beginning of the file
# #After that we go through the RDD by ordered timestamps and we count the number of machines that are active at each timestamp



# accum_count_maintenance_machines = sc.accumulator(0)

# timestamps = wholeFile.map(lambda x : ((int(extract_column(x, 0))), (int(extract_column(x,1)),extract_column(x,2)))).filter(lambda x: x[0] != 0).sortBy(lambda x: x[0], False).cache()

# def function1(x):
#     timestamp = x[0]
#     machine_id = x[1][0]
#     event = x[1][1]
#     if(event != 0 and on_off_dict[machine_id] == 0):
#         on_off_dict[machine_id] = 1
#     elif(event == 0 and on_off_dict[machine_id] == 1):
#         on_off_dict[machine_id] = 0
       
    

#     return (timestamp, list(on_off_dict.values()).count(1)/count_machines)


# timestamps_and_ratio = timestamps.map(lambda x: function1(x)).saveAsTextFile("./results/question2/timestamps_and_ratio")

# timestamps_distinct = timestamps.count()


# print("timestamps_count: ", timestamps_count)
# print("timestamps_distinct: ", timestamps_distinct)

# start_time = time.time()
# cpus_mapped = wholeFile.map(lambda x: (extract_column(x,1),extract_column(x, 4))).reduceByKey(lambda x,y: x).map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[0]).cache();
# elapsed_time = time.time() - start_time

#map result into a list
# cpus_mapped.saveAsTextFile("./results/question1/cpus_mapped")
# cpus_mapped = cpus_mapped.collect()

# cpu_capabilities, quantities = zip(*cpus_mapped)

# print(cpus_mapped)

#accumulator for total downtime

# we filter the '' values since they don't bring any value to the computation
step1 = wholeFile.map(lambda x : ((int(extract_column(x, 0))), (int(extract_column(x,1)),int(extract_column(x,2))))).cache()
step1_alt = wholeFile.filter(lambda x: extract_column(x, 4) != '').map(lambda x : ((int(extract_column(x, 0))), (int(extract_column(x,1)),int(extract_column(x,2)),float(extract_column(x,4))))).cache()

timestamps = step1.filter(lambda x: x[0] != 0).sortBy(lambda x: x[0]).cache()
timestamps_1 = step1_alt.filter(lambda x: x[0] != 0).sortBy(lambda x: x[0]).cache()
machine_count = step1.filter(lambda x: x[0] == 0).map(lambda x: x[1][0]).distinct().count()

#distribution of machine power computation as computed in question 1
# '' -> 32, '0.25' -> 123, '0.5' --> 11632, '1' -> 796)


timestamps_maximum = timestamps.sortBy(lambda x: x[0], False).first()[0]
total_timestamps = timestamps_maximum * machine_count

total_timestamps_weighted = timestamps_maximum * 32 * 0 + timestamps_maximum * 0.25 * 123 + timestamps_maximum * 0.5 * 11632 + timestamps_maximum * 1 * 796
print(total_timestamps)

timestamps_minimum = 0 #we can modify this
machine_and_timestamps = timestamps.map(lambda x: (x[1][0], (x[0], x[1][1], 0))).cache()
machine_and_timestamps_2 = timestamps_1.map(lambda x: (x[1][0], (x[0], x[1][1], 0, x[1][2]))).cache()

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

    print(x)
    print(y)

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



something = machine_and_timestamps.reduceByKey(function1).map(lambda x: (None, x[0])).reduceByKey(lambda x,y: x+y).map(lambda x: x[1]).collect()[0]

something_2 = machine_and_timestamps_2.reduceByKey(function2).map(lambda x: (None, x[0])).reduceByKey(lambda x,y: x+y).map(lambda x: x[1]).collect()[0]



#something_2

downtime_percentage = 100 * something / total_timestamps
downtime_percentage_2 = 100 * something_2 / total_timestamps
print(downtime_percentage)
print(downtime_percentage_2)




sc.stop()







input("Press enter to exit ;)")

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

count_jobs = entries.map(lambda x: (x[2], 1)).reduceByKey(lambda x,y: x+y)
print(count_jobs)

job_and_machines = entries.map(lambda x: ((x[2], x[4]), 1)).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0][0], x[1])).reduceByKey(lambda x,y: max(x,y)).map(lambda x: (x[0], (x[1], 1))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])).map(lambda x: (x[0], int(x[1][0]/x[1][1]))).sortByKey().take(10)
print(job_and_machines)


sc.stop()







input("Press enter to exit ;)")

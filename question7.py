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

#The first line of the file defines the name of each column in the cvs file
#We store it as an array in the driver program


#WE HAVE TO CHANGE SOMETHING HERE ETI ;)
#firstLine =wholeFile.filter(lambda x: "RecID" in x).collect()[0].replace('"', '').split(',')

#filter out the first line from the initial RDD
# entries = wholeFile.filter(lambda x: not ("RecID" in x))

#split each line into an array of items
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

#### HERE THE FUN BEGINS
# 0 -> time
# 1 -> machine ID
# 2 -> event type
# 3 -> platform ID
# 4 -> capacity: number of CPUs
# 5 -> memory




sc.stop()







input("Press enter to exit ;)")

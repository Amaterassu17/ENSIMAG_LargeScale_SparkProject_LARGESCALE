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
    
sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

#remove output file if it already exists
os.system("rm -rf ./results/question5")

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
entries1 = wholeFile1.map(lambda x: x.split(','))
entries2 = wholeFile2.map(lambda x: x.split(','))

table1 = entries1.filter(lambda x: x[9]!='').map(lambda x: ((x[2], x[3], x[4]), (x[9], x[10], x[11])))
table2 = entries2.map(lambda x: ((x[2], x[3], x[4]), (x[5], x[6], x[12])))
table = table1.join(table2).map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][0][2], x[1][1][0], x[1][1][1], x[1][1][2]))).sortBy(lambda x: x[1][0], ascending=False)


#keep the RDD in memory
entries1.cache()


#### HERE THE FUN BEGINS
# 0 -> time
# 1 -> machine ID
# 2 -> event type
# 3 -> platform ID
# 4 -> capacity: number of CPUs
# 5 -> memory




sc.stop()







input("Press enter to exit ;)")

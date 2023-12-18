import sys
from pyspark import SparkContext
import time
import re


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

#Depends on the file, we load the CSV file
wholeFile = sc.textFile("./data/machine_events.csv")

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

cpus_mapped = entries.map(lambda x: extract_column(x, 4)).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).sortByKey()
cpus_mapped.saveAsTextFile("./results/question1/cpus_mapped")








input("Press enter to exit ;)")

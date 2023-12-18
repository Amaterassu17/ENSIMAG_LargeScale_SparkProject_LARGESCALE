import sys
from pyspark import SparkContext
import time
import re
import os
import matplotlib.pyplot as plt


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
os.system("rm -rf ./results/question1")

#Depends on the file, we load the CSV file
wholeFile = sc.textFile("./data/machine_events/part-00000-of-00001.csv")

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

cpus_mapped = wholeFile.map(lambda x: extract_column(x, 4)).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[0]).cache();

#map result into a list
cpus_mapped.saveAsTextFile("./results/question1/cpus_mapped")
cpus_mapped = cpus_mapped.collect()

cpu_capabilities, quantities = zip(*cpus_mapped)

print(cpus_mapped)

plt.bar(cpu_capabilities, quantities, align='center', alpha=0.5, color='green')
# plt.hist(cpus_mapped, bins ='auto', alpha=0.7, color='b', label = 'CPU capabilities')
plt.xlabel('CPU capabilities')
plt.ylabel('Frequency')
plt.title('CPU capabilities histogram')
# plt.legend(loc='upper right')
plt.savefig('./results/question1/cpu_capabilities.png')


sc.stop()







input("Press enter to exit ;)")

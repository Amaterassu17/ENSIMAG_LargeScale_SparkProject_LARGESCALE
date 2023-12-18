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
os.system("rm -rf ./results/question3/task")

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
entries.cache()

start = time.time()
number_per_scheduling = entries.map(lambda x: (int(x[7]),1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[0]).cache()
end= time.time()


print("Time for the first part: {}".format(end-start))
number_per_scheduling.saveAsTextFile("./results/question3/task/answer")
number_collected = number_per_scheduling.collect()


scheduling_class, frequency = zip(*number_collected)

print(scheduling_class)
print(frequency)

plt.bar(scheduling_class, frequency, align='center', alpha=0.5, color='green')
plt.xlabel('Scheduling class')
plt.ylabel('Number of tasks')
plt.title('Number of tasks per scheduling class')
plt.savefig('./results/question3/task/number_of_tasks_per_scheduling_class.png')
plt.show()




#count lines

#keep the RDD in memory
sc.stop()








input("Press enter to exit ;)")

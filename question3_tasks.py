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
os.system("rm -rf ./results/question3/task")
os.system("mkdir ./results/question3/task")
#Depends on the file, we load the CSV file
wholeFile = sc.textFile("./data/task_events/*.csv")

# 0 -> timestamp,
# 1 -> missing info,
# 2 -> job ID,
# 3 -> task index,
# 4 -> machine ID,
# 5 -> event type,
# 6 -> user name,
# 7 -> scheduling class,
# 8 -> priority,
# 9 -> resource request for CPU cores,
# 10 -> resource request for RAM,
# 11 -> resource request for local disk space,
# 12 -> different-machine constraint

#split each line into an array of items
entries = wholeFile.map(lambda x: x.split(','))
entries.cache()

start = time.time()
number_per_scheduling = entries.map(lambda x: (int(x[7]),1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[0]).cache()
end= time.time()

duration = end - start

print("Time for the first part: {}".format(end-start))
number_per_scheduling.saveAsTextFile("./results/question3/task/splitResults")
number_collected = number_per_scheduling.collect()

with open("./results/question3/task/time_computation.txt", 'w') as f:
    f.write(str(duration) + "s")

scheduling_class, frequency = zip(*number_collected)

print(scheduling_class)
print(frequency)

plt.bar(scheduling_class, frequency, align='center', alpha=0.5, color='green')
plt.xlabel('Scheduling class')
plt.ylabel('Number of tasks')
plt.title('Number of tasks per scheduling class')
plt.xticks(range(len(scheduling_class)), range(len(scheduling_class)))
plt.savefig('./results/question3/task/number_of_tasks_per_scheduling_class.png')
plt.show()




#count lines

#keep the RDD in memory
sc.stop()








input("Press enter to exit ;)")

import sys

import findspark
findspark.init()

from pyspark import SparkContext
import time
import re
import os
import matplotlib.pyplot as plt
import time
import math

#Utility functions
    

#### Driver program
    
sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

#remove output file if it already exists
os.system("rm -rf ./results/question8")
os.system("mkdir ./results/question8")

#Depends on the file, we load the CSV file
wholeFile1 = sc.textFile("./data/machine_events/*.csv")
wholeFile2 = sc.textFile("./data/task_events/*.csv")
#The first line of the file defines the name of each column in the cvs file
#We store it as an array in the driver program


#WE HAVE TO CHANGE SOMETHING HERE ETI ;)
#firstLine =wholeFile.filter(lambda x: "RecID" in x).collect()[0].replace('"', '').split(',')

#filter out the first line from the initial RDD
# entries = wholeFile.filter(lambda x: not ("RecID" in x))

#split each line into an array of items
entries1 = wholeFile1.map(lambda x: x.split(',')).cache()
entries2 = wholeFile2.map(lambda x: x.split(',')).cache()
# randomMachine = entries1.map(lambda x: x[1]).take(1)

# print(randomMachine)

# step1 = entries1.filter(lambda x: x[1] == randomMachine).map(lambda x: (x[0],x[1], x[2], x[3], x[4], x[5])).collect()

# print(step1)

# with open("./results/question8/machineEvents.csv", "w") as f:
#     for line in step1:
#         f.write(line[0]+","+line[1]+","+line[2]+","+line[3]+","+line[4]+","+line[5]+"\n")

def functionMap(x):
    uptime = 0
    downtime = 0
    previousTime = 0
    previousEvent = 0
    totalFailures = 0

    MTTF = None
    MTTR = None
    availability = 100

    for i in range(len(x[1])):
        currentEventTime = int(x[1][i][0])
        currentEvent = int(x[1][i][1])

        if i == 0:
            previousTime = currentEventTime
            previousEvent = currentEvent
            continue
        else:
            if previousEvent == 0:
                if currentEvent == 1:
                    uptime += currentEventTime - previousTime
                elif currentEvent == 2:
                    # Include event type 2 in uptime calculation
                    uptime += currentEventTime - previousTime
            elif previousEvent == 1 and currentEvent == 0:
                downtime += currentEventTime - previousTime

            previousTime = currentEventTime
            previousEvent = currentEvent
            if currentEvent == 1:
                totalFailures += 1

    if totalFailures != 0:
        MTTF = uptime / totalFailures
        MTTR = downtime / totalFailures
        availability = 100 * uptime / (uptime + downtime)

    return (x[0], (uptime, downtime, totalFailures, MTTF, MTTR, availability))


#step1 = entries1.map(lambda x: ((x[1]), (int(x[0]), int(x[2])))).sortBy(lambda x: x[1][0], ascending=True).groupByKey().map(lambda x: (x[0], list(x[1]))).map(lambda x: functionMap(x)).cache()

step1 = entries1.map(lambda x: ((x[1]), (int(x[0]), int(x[2])))).sortBy(lambda x: x[1][0], ascending=True).groupByKey().map(lambda x: (x[0], list(x[1]))).map(lambda x: functionMap(x)).cache()


step2 = entries2.map(lambda x: ((x[4]),1)).reduceByKey(lambda x,y: x+y).cache()



#
tableJoin = step1.join(step2).map(lambda x: (x[0], x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3], x[1][0][4], x[1][0][5], x[1][1])).sortBy(lambda x: x[6], ascending = False)

#
tableJoin.saveAsTextFile("./results/question8/machineAvailabilityTasks")

availability_tasks = tableJoin.map(lambda x: (math.floor(x[6]), (x[7], 1))).sortBy(lambda x: x[0], ascending = False).cache()


#list of points from 0 to 100 initialized to 0

availability100points = []

availability100points = availability_tasks.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])).map(lambda x: (x[0], x[1][0]/x[1][1])).sortBy(lambda x: x[0]).collect()
print(availability100points)

# for i in range(100):
#     availability100points.append((i,0))

#     if len(result) > 0:
#         availability100points[i] = result[0]

# # avail100 = availability_tasks.filter(lambda x: x[0] == 100).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])).map(lambda x: (x[0], x[1][0]/x[1][1])).collect()

# # with open("./results/question8/availability100.csv", "w") as f:
# #     for line in avail100:
# #         f.write(str(line[0])+","+str(line[1])+"\n")

# # avail95 = availability_tasks.filter(lambda x: x[0] < 100 and x[0]> 95).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])).map(lambda x: (x[0], x[1][0]/x[1][1])).collect()

# # with open("./results/question8/availability95.csv", "w") as f:
# #     for line in avail95:
# #         f.write(str(line[0])+","+str(line[1])+"\n")

# print(availability100points)

with open("./results/question8/availability100points.csv", "w") as f:
    for i in range(len(availability100points)):
        f.write(str(availability100points[i][0])+","+str(availability100points[i][1])+"\n")


# plotting a graph with availability100points as a list of (x,y) points
plt.plot(*zip(*availability100points), marker='', color='r', ls='-')
plt.xlabel("Availability")
plt.ylabel("Average number of tasks")
plt.title("Availability vs Average number of tasks")
plt.savefig("./results/question8/availability100points.png")
plt.show()


sc.stop()







input("Press enter to exit ;)")

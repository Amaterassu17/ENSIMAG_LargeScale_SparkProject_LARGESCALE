import sys

import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
import time
import re
import os
import matplotlib.pyplot as plt
import time


#Utility functions
    

#### Driver program
    
localVar = 10


conf = SparkConf().setAppName("question6").setMaster("local["+ str(localVar) + "]").setAppName("question6").set("spark.driver.memory", "6g")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

#remove output file if it already exists
os.system("rm -rf ./results/question6")
os.system("mkdir ./results/question6")
#Depends on the file, we load the CSV file
wholeFile1 = sc.textFile("./data/task_events/part-0000*-of-00500.csv")
wholeFile2 = sc.textFile("./data/task_usage/part-0000*-of-00500.csv")

#The first line of the file defines the name of each column in the cvs file
#We store it as an array in the driver program


#filter out the first line from the initial RDD
# entries = wholeFile.filter(lambda x: not ("RecID" in x))

#split each line into an array of items
entries1 = wholeFile1.map(lambda x: x.split(',')).cache()
entries2 = wholeFile2.map(lambda x: x.split(',')).cache()

start = time.time()

#reqCPU, reqMem, reqDisk, CPUsampled useless, Mem, Disk, meanCPUcomputation

table1 = entries1.filter(lambda x: x[9]!='').map(lambda x: ((x[2], x[3], x[4]), (x[9], x[10], x[11])))
table2 = entries2.map(lambda x: ((x[2], x[3], x[4]), (float(x[19]), float(x[6]), float(x[12]), float(x[5]))))
table = table1.join(table2).map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][0][2], x[1][1][0], x[1][1][1], x[1][1][2], x[1][1][3]))).cache()

#CPU req
print("cpustep")
cpu_step = table.map(lambda x: (float(x[1][0]), (x[1][3], 1))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])).map(lambda x: (x[0], x[1][0]/x[1][1])).sortBy(lambda x: x[0]).collect()
# print(cpu_step)
#mem
print("memstep")
mem_step = table.map(lambda x: (float(x[1][1]), (x[1][4], 1))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])).map(lambda x: (x[0], x[1][0]/x[1][1])).sortBy(lambda x: x[0]).collect()
#print(mem_step)
#disk
print("diskstep")
disk_step = table.map(lambda x: (float(x[1][2]), (x[1][5], 1))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])).map(lambda x: (x[0], x[1][0]/x[1][1])).sortBy(lambda x: x[0]).collect()


end = time.time()

duration = end - start

#print(table)

table = table.take(200)

#print in a csv file as a table with the columns name: jobId, taskIndex, machineId, ReqCPU, ReqMem, ReqDisk, CPU, Mem, Disk

with open("./results/question6/joinedTabledSortedByCPU.csv", "w") as f:
    f.write("jobId,taskIndex,machineId,ReqCPU,ReqMem,ReqDisk,samCPU,Mem,Disk,meanCPUcomputation\n")
    for line in table:
        f.write(line[0][0]+","+line[0][1]+","+line[0][2]+","+line[1][0]+","+line[1][1]+","+line[1][2]+","+str(line[1][3])+","+str(line[1][4])+","+str(line[1][5])+","+ str(line[1][6]) + "\n")

with open("./results/question6/maxCPU.csv", "w") as f:
    f.write("reqCPU,meanCPU\n")
    for line in cpu_step:
        f.write(str(line[0])+","+str(line[1])+"\n")


with open("./results/question6/reqMem.csv", "w") as f:
    f.write("reqMem,meanMem\n")
    for line in mem_step:
        f.write(str(line[0])+","+str(line[1])+"\n")


with open("./results/question6/reqDisk.csv", "w") as f:
    f.write("reqDisk,meanDisk\n")
    for line in disk_step:
        f.write(str(line[0])+","+str(line[1])+"\n")

with open("./results/question6/computationTime.txt", "w") as f1:
    f1.write(str(duration))
#save the RDD as a text file


plt.locator_params(axis='x', nbins=10)
# plot results in a 3 different line graphs with requested CPU, CPU

cpu_req, cpu_mean = zip(*cpu_step)


plt.plot(cpu_req, cpu_mean, marker='', color='r', ls='-')
plt.xlabel('Requested CPU')
plt.ylabel('Max CPU')
plt.title('Max CPU usage for different requested CPU')
plt.savefig("./results/question6/reqCPU.png")
plt.show()

    
# plot results in a 3 different line graphs with requested Mem, Mem

mem_req, mem_mean = zip(*mem_step)
print(mem_req)

plt.plot(mem_req, mem_mean, marker='', color='r', ls='-')
plt.xlabel('Requested Mem')
plt.ylabel('Mean Mem')
plt.title('Mean Mem usage for different requested Mem')
plt.savefig("./results/question6/reqMem.png")
plt.show()
    
# plot results in a 3 different line graphs with requested Disk, Disk

disk_req, disk_mean = zip(*disk_step)
print(disk_req)

plt.plot(disk_req, disk_mean, marker='', color='r', ls='-')
plt.xlabel('Requested Disk')
plt.ylabel('Mean Disk')
plt.title('Mean Disk usage for different requested Disk')
plt.savefig("./results/question6/reqDisk.png")
plt.show()


sc.stop()
input("Press enter to exit ;)")

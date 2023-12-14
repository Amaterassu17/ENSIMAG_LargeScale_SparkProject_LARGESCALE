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
    

#### Driver program
    
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

#Depends on the file, we load the CSV file
wholeFile = sc.textFile("./data/xxx.csv")

#The first line of the file defines the name of each column in the cvs file
#We store it as an array in the driver program


#WE HAVE TO CHANGE SOMETHING HERE ETI ;)
firstLine = wholeFile.filter(lambda x: "RecID" in x).collect()[0].replace('"', '').split(',')

#filter out the first line from the initial RDD
entries = wholeFile.filter(lambda x: not ("RecID" in x))

#split each line into an array of items
entries = entries.map(lambda x: x.split(','))

#keep the RDD in memory
entries.cache()


#### HERE THE FUN BEGINS









input("Press enter to exit ;)")

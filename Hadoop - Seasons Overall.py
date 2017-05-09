#! /usr/bin/env python

# load libraries
import pyspark
import sys

def mapper1(index,data):
    # skip header row
    if index==0:
        data.next()
    import csv
    reader = csv.reader(data)
    for row in reader:
        if row[1].split('/')[2] != '2010':
            month = row[1].split('/')[0]
            station = row[3]
            numPeople = row[5].split('.')[0]
            if month[0] == '0':
                month = month[1]

            if month == '3' or month == '4' or month == '5':
                yield (('Spring', station), int(numPeople))
            elif month == '6' or month == '7' or month == '8':
                yield (('Summer', station), int(numPeople))
            elif month == '9' or month == '10' or month == '11':
                yield (('Fall', station), int(numPeople))
            else:
                yield (('Winter', station), int(numPeople))

if __name__=='__main__':

    # to run the program on cluster
    # spark-submit --name "projSeasons" \
    #             hdfs:///user/vfung000/project/Hadoop - Seasons Overall.py \
    #             hdfs:///user/vfung000/project/clean-mta-data/clean-mta-data.csv \
    #             season

    if len(sys.argv) < 3:
        print "Usage: <clean mta loc> <output filename>"
        sys.exit(-1)

    # load pyspark
    sc = pyspark.SparkContext()
    mtaData = sc.textFile(sys.argv[1],use_unicode=False).cache()

    # sum up all the entry by weather
    # returns (event, total_people)
    rdd1 = mtaData.mapPartitionsWithIndex(mapper1)    \
                        .reduceByKey(lambda x,y: x+y) \
                        .sortBy(lambda x: x[1],ascending=False)

    rdd2 = rdd1.filter(lambda x: 'Spring' in x[0]) \
                .take(10)
    rdd2.saveAsTextFile(str(sys.argv[-1])+'Spring')

    rdd3 = rdd1.filter(lambda x: 'Summer' in x[0]) \
                .take(10)
    rdd3.saveAsTextFile(str(sys.argv[-1])+'Summer')

    rdd4 = rdd1.filter(lambda x: 'Fall' in x[0]) \
                .take(10)
    rdd4.saveAsTextFile(str(sys.argv[-1])+'Fall')

    rdd5 = rdd1.filter(lambda x: 'Winter' in x[0]) \
                .take(10)
    rdd5.saveAsTextFile(str(sys.argv[-1])+'Winter')

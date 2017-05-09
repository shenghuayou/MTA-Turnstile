#! /usr/bin/env python

# load libraries
import pyspark

def mapper1(index,data):
    # skip header row
    if index==0:
        data.next()
    import csv
    reader = csv.reader(data)
    for row in reader:
        if row[1].split('/')[2] != '2010':
            date = row[1]
            event = row[4]
            numPeople = row[5].split('.')[0]
            if event == 'Normal':
                yield ((date,'Clear Skies'), int(numPeople))
            else:
                yield ((date,event), int(numPeople))

def mapper2(index,data):
    # skip header row
    if index==0:
        data.next()
    import csv
    reader = csv.reader(data)
    for row in reader:
        if row[1].split('/')[2] != '2010':
            date = row[1]
            event = row[4]
            if event == 'Normal':
                yield ((date, 'Clear Skies'), 1)
            else:
                yield ((date, event), 1)

if __name__=='__main__':

    # to run the program on cluster
    # spark-submit --name "projWeatherOverall" \
    #             hdfs:///user/vfung000/project/HadoopWO.py \

    # load pyspark
    sc = pyspark.SparkContext()
    mtaData = sc.textFile('hdfs:///user/vfung000/project/clean-mta-data/clean-mta-data.csv',use_unicode=False).cache()

    # sum up all the entry by weather
    # returns (event, total_people)
    rdd1 = mtaData.mapPartitionsWithIndex(mapper1) \
                    .reduceByKey(lambda x,y: x+y) \
                    .map(lambda ((d,e),s): (e,s)) \
                    .reduceByKey(lambda x,y: x+y)

    # count how many days that have certain weather
    # d = date; e = event; c = count;
    # returns (event, count)
    rdd2 = mtaData.mapPartitionsWithIndex(mapper2) \
                   .distinct() \
                   .map(lambda ((d,e),c): (e,c)) \
                   .reduceByKey(lambda x,y: x+y)

    # get average Entry in day for weather
    # d = date; e = event; c = count; s = summation
    # returns (event, average_peple_per_weather)
    rdd3 = rdd1.join(rdd2) \
               .map(lambda (e, (s, c)): (e, s/c)) 

    # output
    rdd3.coalesce(1,True).saveAsTextFile("hdfs:///user/vfung000/project/projWeatherOverall")

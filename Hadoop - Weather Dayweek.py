#! /usr/bin/env python

# load libraries
import pyspark

def mapper3(index,data):
    # skip header row
    if index==0:
        data.next()
    import csv
    import datetime
    reader = csv.reader(data)
    for row in reader:
        if row[1].split('/')[2] != '2010':
            date = datetime.datetime.strptime(row[1], '%m/%d/%Y').strftime("%A")
            event = row[4]
            numPeople = row[5].split('.')[0]
            if event == 'Normal':
                yield ((date, 'Clear Skies'), int(numPeople))
            else:
                yield ((date,event), int(numPeople))

def mapper4(index,data):
    # skip header row
    if index==0:
        data.next()
    import csv
    import datetime
    reader = csv.reader(data)
    for row in reader:
        if row[1].split('/')[2] != '2010':
            date = datetime.datetime.strptime(row[1], '%m/%d/%Y').strftime("%A")
            event = row[4]
            if event == 'Normal':
                yield ((date, 'Clear Skies'), 1)
            else:
                yield ((date, event), 1)

if __name__=='__main__':

    # to run the program on cluster
    # spark-submit --name "projWeatherDayweek" \
    #             hdfs:///user/vfung000/project/HadoopWD.py \

    # load pyspark
    sc = pyspark.SparkContext()
    mtaData = sc.textFile('hdfs:///user/vfung000/project/clean-mta-data/clean-mta-data.csv',use_unicode=False).cache()

    # get the amount of people using the mta on each day of the week for different weathers
    # returns ((weekday, event), summation_of_passengers)
    rdd4 = mtaData.mapPartitionsWithIndex(mapper3)  \
                    .reduceByKey(lambda x,y: x+y)

    # get the number of days by each day of the week (hour) for different weathers
    # returns ((weekday, event), summation_of_day_hour)
    rdd5 = mtaData.mapPartitionsWithIndex(mapper4) \
                    .distinct() \
                    .reduceByKey(lambda x,y: x+y) \

    # get the average number of people using the MTA on different days of the week and weather condition
    # returns ((weekday, event), avg)
    rdd6 = rdd4.join(rdd5) \
               .map(lambda (e, (s, c)): (e, s/c)) \
               .sortByKey(True) 

    # output
    rdd6.coalesce(1,True).saveAsTextFile("hdfs:///user/vfung000/project/projWeatherDayweek")

#! /usr/bin/env python

# load libraries
import pyspark
import sys
import datetime as dt

def mapper7(index,data):
    # skip header row
    if index==0:
        data.next()
    import csv
    import datetime as dt
    reader = csv.reader(data)
    secndPriceHike = dt.datetime.strptime('03/03/2013','%m/%d/%Y')
    thirdPriceHike = dt.datetime.strptime('03/22/2015','%m/%d/%Y')
    for row in reader:
        date = dt.datetime.strptime(row[1],'%m/%d/%Y')
        dateName = date.strftime("%A")
        numPeople = row[5].split('.')[0]
        if (date < secndPriceHike):
            yield ((dateName,'A'),int(numPeople))
        if (date >= secndPriceHike and date < thirdPriceHike):
            yield ((dateName,'B'),int(numPeople))
        if (date >= thirdPriceHike):
            yield ((dateName,'C'),int(numPeople))

def mapper8(index,data):
    # skip header row
    if index==0:
        data.next()
    import csv
    import datetime as dt
    reader = csv.reader(data)
    secndPriceHike = dt.datetime.strptime('03/03/2013','%m/%d/%Y')
    thirdPriceHike = dt.datetime.strptime('03/22/2015','%m/%d/%Y')
    for row in reader:
        date = dt.datetime.strptime(row[1],'%m/%d/%Y')
        if (date < secndPriceHike):
            yield ((date,'A'),1)
        if (date >= secndPriceHike and date < thirdPriceHike):
            yield ((date,'B'),1)
        if (date >= thirdPriceHike):
            yield ((date,'C'),1)

if __name__=='__main__':

    # to run the program on cluster
    # spark-submit --name "projWeatherDayweek" \
    #             hdfs:///user/vfung000/project/Hadoop - Price Hike Dayweek.py \
    #             hdfs:///user/vfung000/project/clean-mta-data/clean-mta-data.csv \
    #             weatherDayweek

    if len(sys.argv) < 3:
        print "Usage: <clean mta loc> <output filename>"
        sys.exit(-1)

    # load pyspark
    sc = pyspark.SparkContext()
    mtaData = sc.textFile(sys.argv[1],use_unicode=False).cache()

    # Gather the number of passengers by weekday,period
    # returns ((weekday, period), num_passengers)
    rdd7 = mtaData.mapPartitionsWithIndex(mapper7) \
                    .reduceByKey(lambda x,y: x+y)

    # Gather the occurance for each weekday,period
    # returns ((weekday, period), occurance_each_weekday)
    rdd8 = mtaData.mapPartitionsWithIndex(mapper8) \
                    .distinct() \
                    .map(lambda (x,y): ((x[0].strftime("%A"), x[1]),y)) \
                    .reduceByKey(lambda x,y: x+y)

    # Join the rdds on weekday,period
    # d = date, p = period, s = number_passengers_per_weekday, c = total_occurance_per_weekday
    # Get the average number of people per weekday
    # returns ((weekday,period), avg)
    rdd9 = rdd7.join(rdd8) \
                .map(lambda ((d,p),(s,c)): ((d,p), s/c)) \
                .sortByKey(True) \
                .collect()

    rdd9.saveAsTextFile(sys.argv[-1])

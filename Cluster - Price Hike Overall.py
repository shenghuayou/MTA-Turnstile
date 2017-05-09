#! /usr/bin/env python

# load libraries
import pyspark

def mapper1(index, data):
    if index == 0:
        data.next()
    import csv
    import datetime as dt
    reader = csv.reader(data)
    secndPriceHike = dt.datetime.strptime('03/03/2013','%m/%d/%Y')
    thirdPriceHike = dt.datetime.strptime('03/22/2015','%m/%d/%Y')
    for row in reader:
        date = dt.datetime.strptime(row[1],'%m/%d/%Y')
        numPeople = row[5].split('.')[0]
        if (date < secndPriceHike):
            yield ('A',int(numPeople))
        if (date >= secndPriceHike and date < thirdPriceHike):
            yield ('B',int(numPeople))
        if (date >= thirdPriceHike):
            yield ('C',int(numPeople))

def mapper2(index,data):
    if index == 0:
        data.next()
    import csv
    import datetime as dt
    reader = csv.reader(data)
    secndPriceHike = dt.datetime.strptime('03/03/2013','%m/%d/%Y')
    thirdPriceHike = dt.datetime.strptime('03/22/2015','%m/%d/%Y')
    for row in reader:
        date = dt.datetime.strptime(row[1],'%m/%d/%Y')
        if (date < secndPriceHike):
            yield (date,('A',1))
        if (date >= secndPriceHike and date < thirdPriceHike):
            yield (date,('B',1))
        if (date >= thirdPriceHike):
            yield (date,('C',1))

if __name__=='__main__':

    # load pyspark
    sc = pyspark.SparkContext()
    mtaData = sc.textFile('hdfs:///user/vfung000/project/clean-mta-data/clean-mta-data.csv',use_unicode=False).cache()

    # Gather the number of passengers for each price hike
    # category => A == firstPriceHike, B == SecondPriceHike, C == ThirdPriceHike
    # return (category, num_passengers)
    rdd1 = mtaData.mapPartitionsWithIndex(mapper1) \
                    .reduceByKey(lambda x,y: x+y)


    # Categorize by price hikes then get unique days
    # Re-format the data to (category, c)
    # Perform summation to get number of days in each category
    # return (category, num_days)
    rdd2 = mtaData.mapPartitionsWithIndex(mapper2) \
                    .distinct() \
                    .map(lambda (d,(cat,c)): (cat, c)) \
                    .reduceByKey(lambda x,y: x+y)

    # Join the rdd's by category
    # Get the average of number of people over the number of days
    # return (category, avg)
    rdd3 = rdd1.join(rdd2) \
                .map(lambda (cat,(s,c)): (cat, s/c)) \
                .sortByKey()

    # output
    rdd3.coalesce(1,True).saveAsTextFile("hdfs:///user/vfung000/project/priceHikeOverall")

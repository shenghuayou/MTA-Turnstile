#! /usr/bin/env python

# load libraries
import pyspark
import datetime as dt

def mapper4(index,data):
    # skip header row
    if index==0:
        data.next()
    import csv
    reader = csv.reader(data)
    for row in reader:
        date = row[1].split('/')[0] + '/' + row[1].split('/')[2]
        numPeople = row[5].split('.')[0]
        yield (date, int(numPeople))

def mapper5(index,data):
    # skip header row
    if index==0:
        data.next()
    import csv
    reader = csv.reader(data)
    for row in reader:
        date = row[1]
        yield date

if __name__=='__main__':

    # to run the program on cluster
    # spark-submit --name "projWeatherMonthly" \
    #             hdfs:///user/vfung000/project/HadoopPHM.py \

    # load pyspark
    sc = pyspark.SparkContext()
    mtaData = sc.textFile('hdfs:///user/vfung000/project/clean-mta-data/clean-mta-data.csv',use_unicode=False).cache()

    # For each month gather the number of passengers
    # return (%m/%Y, num_passengers)
    rdd4 = mtaData.mapPartitionsWithIndex(mapper4) \
                    .reduceByKey(lambda x,y: x+y)

    # Gather the number of days for each month (map+distinct)
    # Reformat the %m/%d/%Y to %m/%Y (map)
    # Perform a summation to get the total days per month/year (reduce)
    # returns (%m/%Y, number of days in month)
    rdd5 = mtaData.mapPartitionsWithIndex(mapper5) \
                    .distinct() \
                    .map(lambda x: (dt.datetime.strptime(x,'%m/%d/%Y').strftime('%m/%Y'),1)) \
                    .reduceByKey(lambda x,y: x+y)


    # Join the rdds on date
    # d = date, s = number_passengers_per_month, c = total_days_per_month
    # Get the average number of people per day
    # returns (date, avg)
    rdd6 = rdd4.join(rdd5) \
                .map(lambda (d, (s, c)): (d, s/c)) \
                .sortByKey(True) \
                .collect()

    rdd6.saveAsTextFile("projWeatherMonthly")

# MTA-Turnstile
*Final Project for Big Data Management, Spring 2017*

## Group Members
* [Shenghua You](https://github.com/shenghuayou), shenghuayou@gmail.com
* [Victor Fung](https://github.com/VictorFung1), victor.fung122@gmail.com
* [Simon Ling](https://github.com/simonvista), simon-ling@hotmail.com

## Dependencies
* Languages: Python, Javascript
* Framework: Spark
* Cluster: Hadoop, HDFS (HUE)

## Datasets
* MTA Turnstile Dataset - http://web.mta.info/developers/turnstile.html
* Wunderground - https://www.wunderground.com/

## About this repository
* For the analysis results, view the ipython notebooks:
  * [Analysis - MTA Base Fare.ipynb](https://github.com/shenghuayou/MTA-Turnstile/blob/master/Analysis%20-%20MTA%20Price%20Hike.ipynb)
  * [Analysis - MTA Popular Place.ipynb](https://github.com/shenghuayou/MTA-Turnstile/blob/master/Analysis%20-%20MTA%20Popular%20Places.ipynb)
  * [Analysis - MTA Weather Effect.ipynb](https://github.com/shenghuayou/MTA-Turnstile/blob/master/Analysis%20-%20MTA%20Weather%20Effect.ipynb)

* To view the pysparks code used to obtain the results in the analysis, view the ipython notebooks:
  * [Sparks - MTA Popular Place.ipynb](https://github.com/shenghuayou/MTA-Turnstile/blob/master/Sparks%20-%20MTA%20Popular%20Place.ipynb)
  * [Sparks - MTA Price Hike.ipynb](https://github.com/shenghuayou/MTA-Turnstile/blob/master/Sparks%20-%20MTA%20Price%20Hike.ipynb)
  * [Sparks - MTA Weather Effect.ipynb](https://github.com/shenghuayou/MTA-Turnstile/blob/master/Sparks%20-%20MTA%20Weather%20Effect.ipynb)

* The code used on Hadoop, view the following code:
  * [Hadoop - Price Hike Dayweek.py](https://github.com/shenghuayou/MTA-Turnstile/blob/master/Hadoop%20-%20Weather%20Dayweek.py)
  * [Hadoop - Price Hike Monthly.py](https://github.com/shenghuayou/MTA-Turnstile/blob/master/Hadoop%20-%20Price%20Hike%20Monthly.py)
  * [Hadoop - Price Hike Overall.py](https://github.com/shenghuayou/MTA-Turnstile/blob/master/Hadoop%20-%20Price%20Hike%20Overall.py)
  * [Hadoop - Seasons Overall.py](https://github.com/shenghuayou/MTA-Turnstile/blob/master/Hadoop%20-%20Seasons%20Overall.py)
  * [Hadoop - Weather Dayweek.py](https://github.com/shenghuayou/MTA-Turnstile/blob/master/Hadoop%20-%20Weather%20Dayweek.py)
  * [Hadoop - Weather Overall.py](https://github.com/shenghuayou/MTA-Turnstile/blob/master/Hadoop%20-%20Weather%20Overall.py)


## Running on the Hadoop with Sparks and accessing files in HDFS
* Make the python file executable
```
Command:
$ hadoop fs -chmod +x <your python file location>

Example:
$ hadoop fs -chmod +x /user/vfung000/project/python-code.py
```

* Executing on the [cluster](http://spark.apache.org/docs/latest/submitting-applications.html)
```
Command:
$ spark-submit --name <name of job> \
                <python code location>

Example:
$ spark-submit --name "projWeatherDayweek" \
              hdfs:///user/vfung000/project/HadoopPHD.py \
```

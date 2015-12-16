import datetime
from pyspark.sql import functions as sqlfuncs
from ast import literal_eval
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,Row,DataFrame
from collections import Counter

from pprint import pprint
import numpy as np
from pyspark.mllib.linalg import Vectors



intervals=((0,4),(4,8),(8,12),(12,16),(16,20),(20,24))
dayInterval=list( (iii,a,b) for a,b in intervals for iii in range(7))
intervalIndDict={}
maxInd=len(dayInterval)
for iii,(day,a,b) in enumerate(dayInterval):
    for jjj in range(a,b):
        intervalIndDict[(day,jjj)]=iii




def parseEntry(xx):

    mindate=datetime.datetime(datetime.MINYEAR, 1, 1,1,1)
    xx=xx.split('\t')
    a_virtual=xx[0]
    browser=xx[1]
    referrer=xx[2]
    a_user_key=xx[3]
    try:
        birthyear=int(xx[4])
        age=2015-birthyear
    except Exception as _:
        birthyear=xx[4]
        age=-1
    gender=xx[5]
    #print(xx)
    #print(xx[6])
    if xx[6]!='NAN':
        reg_date=datetime.datetime.strptime(xx[6],'%Y-%m-%d')
    else:
        reg_date=mindate
    device=xx[7]
    date=datetime.datetime.strptime(xx[8],'%d-%m-%Y')
    tdiff=datetime.timedelta(hours=int(xx[9]))
    date=date+tdiff
    year=date.year
    month=date.month
    day=date.day
    hour=int(xx[9])
    weekday=date.weekday()

    if reg_date>mindate:
        days_since_registration=(date-reg_date).days
    else:
        days_since_registration=-1

    metrics=list([int(x.replace(',0','')) for x in xx[10:]])
    visits=metrics[0]
    visits_betalt=metrics[1]
    pageviews=metrics[2]
    pageview_nothome=metrics[3]
    pageview_betalt=metrics[4]

    timegroup_pvs=Vectors.sparse(maxInd,[(intervalIndDict[(weekday,hour)],pageviews)])
    timegroup_visit=Vectors.sparse(maxInd,[(intervalIndDict[(weekday,hour)],1.)])

    return Row(browser=browser,a_user_key=a_user_key,age=age,\
               day=day,hour=hour,date=date,weekday=weekday,pv=pageviews,\
               pv_nh=pageview_nothome,pv_bet=pageview_betalt,referrer=referrer,\
               device=device,gender=gender,days_since_registration=days_since_registration,\
               reg_date=reg_date,timegroup_pvs=timegroup_pvs,timegroup_visit=timegroup_visit,\
               a_virtual=a_virtual)


if __name__ == "__main__":

    print(intervalIndDict)
    conf=SparkConf().setAppName('konsumprofiler').setMaster("local[8]").set('spark.app.id','200')

    sc=SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    period=2. #2 weeks of data

    konsum=sc.textFile('/home/erlenda/data/konsum/amedia_mainsite_20151124-20151207_15145.tsv').map(parseEntry)
    konsum_reg_user=konsum.filter(lambda x:(x.a_user_key!='NAN') and (x.a_user_key!='') )
    konsum_user=sqlContext.createDataFrame(konsum_reg_user)
    pprint(konsum_user.take(5))
    tt=konsum_user.groupBy('a_user_key').agg(sqlfuncs.sum('timegroup_pvs'))

    

    pprint(tt.take(5))

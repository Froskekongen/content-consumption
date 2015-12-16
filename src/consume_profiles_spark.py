import datetime
from pyspark.sql import functions as sqlfuncs
from ast import literal_eval
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,Row,DataFrame
from collections import Counter

from pprint import pprint
import numpy as np
from pyspark.mllib.linalg import Vectors
import plotly.graph_objs as go
import plotly.plotly as py

import sys


intervals=((0,4),(4,8),(8,12),(12,16),(16,20),(20,24))
dayInterval=list( (iii,a,b) for a,b in intervals for iii in range(7))



def split_seq(seq, size):
        newseq = []
        splitsize = 1.0/size*len(seq)
        for i in range(size):
                newseq.append(seq[int(round(i*splitsize)):int(round((i+1)*splitsize))])

        dd={}
        for kk in newseq:
            mm,MM=np.min(kk),np.max(kk)
            for ll in kk:
                dd[ll]=(mm,MM)
        return dd,newseq

splDict,newseq=split_seq(list(range(24)),6)

groupToInd={}
groupList=[]
for iii in range(7):
    for ss in newseq:
        groupList.append((iii,(ss[0],ss[-1])))
groupList=list(sorted(groupList))
for iii,entry in enumerate(groupList):
    groupToInd[entry]=iii






def counter_to_fraction(cc,doSort=True):
    totalCount=0.
    for key,x in cc.items():
        totalCount+=x
    dd=[]
    for key,x in cc.items():
        dd.append((key,x/totalCount))
    if doSort:
        dd=list(sorted(dd))
    return list(sorted(dd))

def dict_to_list_of_tuples(cc,doSort=True):
    dd=[]
    for key,val in cc.items():
        dd.append((key,val))
    if doSort:
        dd=list(sorted(dd))
    return dd



def a_user_konsum_to_row(uu):
    dd={}
    dd['a_user_key']=uu[0]
    konsumFrac=counter_to_fraction(uu[1][0])
    for key,val in konsumFrac:
        dd[str(key)]=val

    return Row(**dd)



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

    timegroup=str( (weekday,splDict[hour]) )
    timegroup_ind=groupToInd[(weekday,splDict[hour])]

    return Row(browser=browser,a_user_key=a_user_key,age=age,\
               day=day,hour=hour,date=date,weekday=weekday,pv=pageviews,\
               pv_nh=pageview_nothome,pv_bet=pageview_betalt,referrer=referrer,\
               device=device,gender=gender,days_since_registration=days_since_registration,\
               reg_date=reg_date,timegroup=timegroup,a_virtual=a_virtual,visits=1,\
               timegroup_ind=timegroup_ind)


if __name__ == "__main__":
    # pprint(groupToInd)
    # pprint(splDict)
    # sys.exit(1)

    conf=SparkConf().setAppName('konsumprofiler').setMaster("local[8]").set('spark.app.id','200')

    sc=SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    period=2. #2 weeks of data

    # konsum=sc.textFile('/home/erlenda/data/konsum/pvs_visits/*').map(parseEntry)
    # konsum_reg_user=konsum.filter(lambda x:(x.a_user_key!='NAN') and (x.a_user_key!=''))
    #
    # konsum_user_DF=sqlContext.createDataFrame(konsum)
    # #konsum_reg_user=konsumDF.filter((konsumDF.a_user_key!='NAN') & (konsumDF.a_user_key!='') )
    #
    #
    # konsum_user_DF.write.save('/home/erlenda/data/konsum/a_user_konsum_parquet')
    #
    # konsum_anon=konsum.filter(lambda x: ('-' not in x.a_user_key) and (x.a_user_key!='')  )
    # konsum_anon_df=sqlContext.createDataFrame(konsum_anon)
    #
    # konsum_anon_df.write.save('/home/erlenda/data/konsum/anon_konsum_parquet')

    # ##########################
    konsum_user=sqlContext.read.parquet('/home/erlenda/data/konsum/a_user_konsum_parquet')
    user_info=sqlContext.read.parquet('/home/erlenda/data/konsum/a_users_parquet')
    pprint(konsum_user.take(2))
    print()


    # konsum_user_agg=konsum_user.groupBy('a_user_key').agg(sqlfuncs.max('reg_date').alias('reg_date'),\
    #     sqlfuncs.avg('age').alias('age'), sqlfuncs.max('gender').alias('gender'),sqlfuncs.max('date').alias('last_consume'),\
    #     sqlfuncs.min('date').alias('first_consume')  )
    # konsum_user_agg.registerTempTable('user_agg')
    #
    #
    # print(konsum_user_agg.first())
    # konsum_user_agg.write.save('/home/erlenda/data/konsum/a_users_parquet')
    #
    #
    #
    # #reg_late=konsum_user.filter(konsum_user.reg_date<datetime.datetime(2015,11,16,0,0))
    #
    pvs=konsum_user.groupBy('a_virtual','a_user_key','timegroup','device').agg(sqlfuncs.sum(konsum_user.pv).alias("pvs"),\
                                                  sqlfuncs.sum(konsum_user.pv_bet).alias("pvs_bet"),\
                                                  sqlfuncs.max('date').alias('last_consume'),\
                                                  sqlfuncs.min('date').alias('first_consume'),\
                                                  sqlfuncs.sum(konsum_user.visits).alias("visits"))

    pprint(pvs.take(10))
    print()
    #print(pvs.take(100)[55])
    pvs_tot1=pvs.agg(sqlfuncs.sum(pvs.pvs)).first()
    print('Total after basic aggregation',pvs_tot1)

    pvs_mapped=pvs.rdd.map(lambda x:((x.a_user_key,x.a_virtual), (Counter({literal_eval(x.timegroup):x.pvs}),\
        Counter({literal_eval(x.timegroup):1}),\
        x.pvs,\
        x.pvs_bet,\
        Counter({x.device:x.pvs}) ) )  )

    pvs_reduced=pvs_mapped.reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1],a[2]+b[2],a[3]+b[3],a[4]+b[4])   )

    print()
    pprint(pvs_reduced.take(3))




    user_map=user_info.rdd.map(lambda x:(x.a_user_key,x.asDict())).join(pvs_reduced.map(lambda x:(x[0][0],(x[0][1],*x[1]))))
    print()
    pprint(user_map.take(3))
    user_map=user_map.map(lambda x:Row(**x[1][0],a_virtual=x[1][1][0],pv_groups=x[1][1][1],pvs=x[1][1][3],\
        pvs_bet=x[1][1][4],devices=x[1][1][5]))

    user_map.saveAsPickleFile('/home/erlenda/data/konsum/konsumprofil-rdd',32)

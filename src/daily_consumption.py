from pyspark.mllib import fpm
import datetime
from pyspark.sql import SQLContext, Row
import plotly.plotly as py
import plotly.graph_objs as go
from pyspark.sql.functions import explode
from pyspark.sql import functions as sqlfuncs
import itertools

def til_kategorier(dayinweek,hour):
    def inint(a,interval):
        return interval[0]<=a<=interval[1]

    morgen_hverdag=[5,11]
    kveld_hverdag=[20,23]
    ettermiddag_hverdag=[16,19]
    midt_hverdag=[12,15]

    morgen_helg=[7,12]
    kveld_helg=[19,23]
    ettermiddag_helg=[15,18]
    midt_helg=[13,14]

    if (dayinweek>4) or ( (dayinweek==4) and (hour>15) ):
        helg=True
    else:
        helg=False

    if helg:
        morg_h=inint(hour,morgen_helg)
        kveld_h=inint(hour,kveld_helg)
        etterm_h=inint(hour,ettermiddag_helg)
        midt_h=inint(hour,midt_helg)

        morg_v=False
        kveld_v=False
        etterm_v=False
        midt_v=False
    else:
        morg_v=inint(hour,morgen_hverdag)
        kveld_v=inint(hour,kveld_hverdag)
        etterm_v=inint(hour,ettermiddag_hverdag)
        midt_v=inint(hour,midt_hverdag)

        morg_h=False
        kveld_h=False
        etterm_h=False
        midt_h=False

    return helg,morg_h,midt_h,etterm_h,kveld_h,morg_v,midt_v,etterm_v,kveld_v





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


    helg,morg_h,midt_h,etterm_h,kveld_h,morg_v,midt_v,etterm_v,kveld_v=til_kategorier(weekday,hour)

    metrics=list([int(x.replace(',0','')) for x in xx[10:]])
    visits=metrics[0]
    visits_betalt=metrics[1]
    pageviews=metrics[2]
    pageview_nothome=metrics[3]
    pageview_betalt=metrics[4]
    return Row(a_virtual=a_virtual,browser=browser,referrer=referrer,a_user_key=a_user_key,age=age,\
              gender=gender,reg_date=reg_date,device=device,date=date,visits=visits,visits_betalt=visits_betalt,\
              pageviews=pageviews,pageview_nothome=pageview_nothome,pageview_betalt=pageview_betalt,year=year,month=month,\
              day=day,weekday=weekday,hour=hour,dayse_since_registration=days_since_registration,\
              helg=helg,morgen_helg=morg_h,midt_helg=midt_h,ettermiddag_helg=etterm_h,kveld_helg=kveld_h,\
              morgen_hverdag=morg_v,midt_hverdag=midt_v,ettermiddag_hverdag=etterm_v,kveld_hverdag=kveld_v)


if __name__ == "__main__":
    from pyspark import SparkContext,SparkConf
    conf = SparkConf().setAppName('konsumanalyse').setMaster("local[*]")
    sc = SparkContext(conf=conf)
    konsum=sc.textFile('/mnt/konsum/amedia_rb_20150801-20151124_15145.tsv').map(parseEntry).cache()
    tt=konsum.take(10)
    print(tt)

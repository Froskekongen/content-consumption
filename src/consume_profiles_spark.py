import datetime
from pyspark.sql import functions as sqlfuncs
from ast import literal_eval
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,Row
from collections import Counter

if __name__ == "__main__":
    conf=SparkConf().setAppName('konsumprofiler').setMaster("local[8]").set('spark.app.id','200')

    sc=SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    period=2. #2 weeks of data

    dfRead=sqlContext.read
    konsum_user=dfRead.load('/home/erlenda/data/konsum/amedia_mainsite_20151124-20151207_15145_parquet')
    print(konsum_user.first())

    # konsum_user_agg=konsum_user.groupBy('a_user_key').agg(sqlfuncs.max('reg_date').alias('reg_date'),\
    #     sqlfuncs.avg('age').alias('age'), sqlfuncs.max('gender').alias('gender')  )
    #
    # print(konsum_user_agg.first())
    # konsum_user_agg.write.save('/home/erlenda/data/konsum/a_users_parquet')


    reg_late=konsum_user.filter(konsum_user.reg_date>datetime.datetime(2015,11,16,0,0))
    #reg_late_persona=reg_late.groupBy('a_user_key','age','gender').agg()
    #print(reg_late_persona.first())

    # pvs=reg_late.groupBy('a_user_key','timegroup','device','age','gender').agg(sqlfuncs.sum(reg_late.pv).alias("pvs"),\
    #                                               sqlfuncs.sum(reg_late.pv_bet).alias("pvs_bet"))

    pvs=reg_late.groupBy('a_user_key','timegroup').agg(sqlfuncs.sum(reg_late.pv).alias("pvs"),\
                                                  sqlfuncs.sum(reg_late.pv_bet).alias("pvs_bet"))
    pvs_mapped=pvs.rdd.map(lambda x:(x.a_user_key, (Counter({literal_eval(x.timegroup):float(x.pvs)}),\
        Counter({literal_eval(x.timegroup):1. if x.pvs>0 else 0.}),float(x.pvs),float(x.pvs_bet)) )  )
    pvs_reduced=pvs_mapped.reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1],a[2]+b[2],a[3]+b[3])   )

    pvs_map2=pvs_reduced.map(lambda x:Row(a_user_key=x[0],pvs_groups=x[1][0], visits_groups=x[1][1], pvs_total=x[1][2], pvs_bet=x[1][3]   )    )



    print(pvs_map2.first())

    # pvsLocal=pvs.collect()
    # print(pvsLocal[0])
    #
    #
    # userDD={}
    # for r in pvsLocal:
    #     if r.a_user_key not in userDD:
    #         userDD[r.a_user_key]={'pv':Counter(),'pv_nh':Counter(),'pv_bet':Counter(),\
    #           'visit_bet':Counter(),'visit':Counter(),'device':Counter()}
    #
    #     userDD[r.a_user_key]['age']=r.age
    #     userDD[r.a_user_key]['gender']=r.gender
    #     userDD[r.a_user_key]['pv'][literal_eval(r.timegroup)]+=r.pvs/period
    #     userDD[r.a_user_key]['pv_bet'][literal_eval(r.timegroup)]+=r.pvs_bet/period
    #     if r.pvs_bet>1.e-10:
    #         userDD[r.a_user_key]['visit_bet'][literal_eval(r.timegroup)]+=1.
    #     if r.pvs>1.e-10:
    #         userDD[r.a_user_key]['visit'][literal_eval(r.timegroup)]+=1.
    #     userDD[r.a_user_key]['device'][r.device]+=1.
    #
    # indToKey=[]
    # tdicts=[]
    # tdicts_bet=[]
    #
    # for iii,(user_key,vals) in enumerate(userDD.items()):
    #     indToKey.append([iii,user_key,vals['age'],vals['gender']])
    #     tdicts.append(dict(vals['pv']))
    #     tdicts_bet.append(dict(vals['pv_bet']))

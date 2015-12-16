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
from plotly import tools as pytools

import pickle as pkl


intervals=((0,4),(4,8),(8,12),(12,16),(16,20),(20,24))
dayInterval=list(sorted([ (iii,(a,b)) for a,b in intervals for iii in range(7)]))
dayInterval2=list([(d,(e[0],e[1]-1)) for d,e in dayInterval])

agegroups=((-1,12),(12,20),(20,30),(30,40),(40,50),(50,60),(60,70),(70,140),(140,2000))
ageToGroup={}
for gr in agegroups:
    for jjj in range(gr[0],gr[1]):
        if (gr==(-1,12) or gr==(140,2000)):
            gr='ukjent'
        ageToGroup[jjj]=gr

def trimSmallPercentages(labels,vals,cutPercent=0.04):
    ss=np.sum(vals)
    vals2=[val/ss for val in vals]
    ol,oo=[],[]
    other=0
    for label,val2,val in zip(labels,vals2,vals):
        if val2<cutPercent:
            other+=val
            continue
        ol.append(label)
        oo.append(val)
    ol.append('other')
    oo.append(other)
    return ol,oo


def groupToPlot(cc,toFraction=True):
    dayList=('Mandag','Tirsdag','Onsdag','Torsdag','Fredag','Lørdag','Søndag')
    tt=[]

    totalVal=0

    for key in dayInterval2:
        if key in cc:
            tt.append((key,cc[key]))
            totalVal+=cc[key]
        else:
            tt.append((key,0.))

    tt=list(sorted(tt,key=lambda x:x[0]))
    labels=[]
    vals=[]
    for iii,t in enumerate(tt):
        labels.append( str((dayList[t[0][0]] ,t[0][1])))
        val=t[1]
        if toFraction:
            if totalVal>0.:
                val=val/totalVal
        vals.append(val)

    return labels,vals


if __name__ == "__main__":
    #print(ageToGroup)
    print(dayInterval)

    aviser=['an','ba','dt','fb','havis','oa','rb',\
        'ta','tb','nordlys','firda','glomdalen',\
        'mossavis','ringblad','sb','sa',\
        'tk','op','ostlendingen']

    conf=SparkConf().setAppName('konsumprofiler').setMaster("local[8]").set('spark.app.id','200')

    sc=SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    user_map=sc.pickleFile('/home/erlenda/data/konsum/konsumprofil-rdd')
    print(user_map.first())

    total_pvs=user_map.map(lambda x:x.pvs).collect()
    #total_visits=user_map.map(lambda x:(x.a_user_key,1.)).collect()
    #print(total_pvs[:1000])
    percs_pageviews=np.percentile(total_pvs,[20.,40.,60.,80.])
    #percs_visits=np.percentile(total_visits,[20.,40.,60.,80.])

    print('Quantiles pageviews:',percs_pageviews)
    percs_pageviews_top=np.percentile(total_pvs,[95.,98.,99.])
    #print('Quantiles visits:',percs_visits)






    ############ Agegroups and gender
    topp=np.percentile(total_pvs,[60,97])
    bottom=np.percentile(total_pvs,[20,40])
    alluser=np.percentile(total_pvs,[10,97])

    #filterdict={'bottom':bottom,'topp':topp,'alle':alluser}
    filterdict={'bottom':bottom,'topp':topp,'alle':alluser}
    for key,rrs in filterdict.items():
        gender_users=user_map.filter(lambda x:(x.pvs>rrs[0]) and (x.pvs<rrs[1]))\
            .map(lambda x:((x.a_virtual,x.gender),x.pvs))\
            .reduceByKey(lambda a,b:a+b).map(lambda x:Row(a_virtual=x[0][0],gender=x[0][1],pageviews=x[1]))\
            .cache()

        age_group_users=user_map.filter(lambda x:(x.pvs>rrs[0]) and (x.pvs<rrs[1]))\
            .map(lambda x:((x.a_virtual,ageToGroup[int(x.age)]),x.pvs))\
            .reduceByKey(lambda a,b:a+b).map(lambda x:Row(a_virtual=x[0][0],agegroup=x[0][1],pageviews=x[1]))\
            .cache()

        for avis in aviser:
            gender_user_avis=gender_users.filter(lambda x:x.a_virtual==avis).collect()
            age_group_avis=age_group_users.filter(lambda x:x.a_virtual==avis).collect()

            gender={}
            gender['labels']=[g.gender for g in gender_user_avis]
            gender['values']=[g.pageviews for g in gender_user_avis]
            gender['type']='pie'
            #gender['domain']={'x': [0.53, 1.],'y': [0., 1.]}

            agegroup={}
            agegroup['labels']=[g.agegroup for g in age_group_avis]
            agegroup['values']=[g.pageviews for g in age_group_avis]
            agegroup['type']='pie'
            #agegroup['domain']={'x': [0., .47],'y': [0., 1.]}


            fig1={'data':[gender],\
                'layout':{'title':'aID, kjønnsfordeling i antall sidevisninger, {0}'.format(avis)}\
                }
            fig2={'data':[agegroup],\
                'layout':{'title':'aID, aldergruppering i antall sidevisninger, {0}'.format(avis)}\
                }

            #pprint(fig)
            fn1="pics/"+avis+"_gender.png"
            fn2="pics/"+avis+"_agegroup.png"
            #fn="pics/gender{0}.png".format(avis)
            py.image.save_as(fig1,fn1,format='png',width=500,height=400,scale=3)
            py.image.save_as(fig2,fn2,format='png',width=500,height=400,scale=3)

    ##################


    konsum_user=sqlContext.read.parquet('/home/erlenda/data/konsum/a_user_konsum_parquet')

    pprint(konsum_user.take(5))

    pvs=konsum_user.groupBy('a_virtual','device').agg(sqlfuncs.sum(konsum_user.pv).alias("pvs"),\
                                                  sqlfuncs.sum(konsum_user.pv_bet).alias("pvs_bet")).cache()

    pprint(pvs.take(5))

    for avis in aviser:
        pvs_avis=pvs.filter(pvs.a_virtual==avis).collect()


        labels=[pa.device for pa in pvs_avis]
        pageviews_devices=[g.pvs for g in pvs_avis]
        olabels,opvs=trimSmallPercentages(labels,pageviews_devices)

        device={}
        device['labels']=olabels
        device['values']=opvs
        device['type']='pie'
        fig1={'data':[device],\
            'layout':{'title':'aID, fordeling av enheter i antall sidevisninger, {0}'.format(avis)}\
            }
        fn="pics/{0}_devices.png".format(avis)
        py.image.save_as(fig1,fn,format='png',width=500,height=400,scale=3)





    # pprint(some_users)
    #
    # user_info=sqlContext.read.parquet('/home/erlenda/data/konsum/a_users_parquet')
    # tag_profiles=sqlContext.read.parquet('/home/erlenda/data/konsum/tag_profiler_parquet')
    #
    #
    #
    #
    # traces=[]
    # for user in some_users:
    #     age=user.age if 12<=user.age<=140 else 'ukjent'
    #     labs,vals=groupToPlot(user.pv_groups)
    #     traces.append(go.Bar(x=labs,y=vals,name="Alder: {0}, Kjønn: {1}, Sidevisninger: {2}".format(user.age,user.gender,user.pvs)))
    #
    # ticks=go.XAxis(tickangle=-20,nticks=14)
    # layout = go.Layout(
    #     barmode='group',
    #     xaxis=ticks
    # )
    # fig2=go.Figure(data=traces,layout=layout)
    #
    #
    # gender_users=user_map.map(lambda x:((x.a_virtual,x.gender),x.pv_groups))\
    #     .reduceByKey(lambda a,b:a+b)
    #
    #
    #
    #
    #
    #
    # papers=set(['rb'])
    #
    # some_papers=gender_users.filter(lambda x:x[0][0] in papers).collect()
    # paper_traces=[]
    # for paper in some_papers:
    #     labs,vals=groupToPlot(paper[1],toFraction=False)
    #     paper_traces.append(go.Bar(x=labs,y=vals,name=paper[0]))
    #
    # fig3=go.Figure(data=paper_traces,layout=go.Layout(barmode='stack',xaxis=ticks))
    #
    # some_papers=age_group_users.filter(lambda x:x[0][0] in papers).collect()
    # paper_traces=[]
    # for paper in some_papers:
    #     labs,vals=groupToPlot(paper[1],toFraction=False)
    #     paper_traces.append(go.Bar(x=labs,y=vals,name=paper[0]))
    #
    # fig4=go.Figure(data=paper_traces,layout=go.Layout(barmode='stack',xaxis=ticks))
    #
    #
    # datadict={}
    # datadict['labels']=list(one_user.devices.keys())
    # datadict['values']=list(one_user.devices.values())
    # datadict['type']='pie'
    # datadict['domain']={'x': [0., .48],'y': [0., 1.]}
    #
    # datadict2={'labels':['Sidevisninger, ikke betalt','Sidevisninger, betalt'],\
    #     'values':[one_user.pvs-one_user.pvs_bet,one_user.pvs_bet],\
    #     'type':'pie',\
    #     'domain': {'x': [.52, 1],'y': [0., 1.]} \
    #     }
    #
    # fig={'data':[datadict,datadict2],\
    #     'layout':{'title':'Andel betalt innhold og andel ulike enheter. Alder: {0}, Kjønn: {1}, Sidevisninger: {2}'.\
    #         format(one_user.age,one_user.gender,one_user.pvs) }\
    #     }
    #
    # one_tag_profile=tag_profiles.filter(tag_profiles.a_user_key==one_user.a_user_key).first()
    # print()
    # print(one_tag_profile)
    # print()
    # with open('/home/erlenda/data/konsum/countvec_vocabulary.pkl',mode='rb') as ff:
    #     vocabulary=pkl.load(ff)
    #
    #
    #
    #
    # # fig = pytools.make_subplots(rows=1, cols=2, subplot_titles=('Konsum, ulike enheter', 'Andel betalt innhold'))
    # # fig.append_trace(datadict,1,1)
    # # fig.append_trace(datadict2,1,2)
    #
    #
    # # url=py.plot(fig,filename='user-devices',sharing='secret')
    # # url=py.plot(fig2,filename='Konsumtidspunkt',sharing='secret')
    # # url=py.plot(fig3,filename='Konsumtidspunkt, kjønn, avis',sharing='secret')
    # # url2=py.plot(fig4,filename='Konsumtidspunkt, aldersgruppe, avis',sharing='secret')
    #
    # py.image.save_as(fig,'test.png',format='png',width=800,height=500,scale=3)

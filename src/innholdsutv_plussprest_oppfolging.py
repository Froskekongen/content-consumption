import GetReports
import os
import elasticsearch as es
import datetime
from elasticsearch import helpers as eshelper
import json
from collections import defaultdict
from itertools import groupby

if __name__ == "__main__":
    reportnumber=15437
    startdate='20151215'
    enddate='20160106'
    site='amediatotal'
    index='innhold999'
    doc_type='user'

    fw=GetReports.StreamFileWriter(outputdir='../data/')
    gr=GetReports.GetReports()
    fname='../data/amedia_'+site+'_'+startdate+'-'+enddate+'_'+str(reportnumber)+'.tsv'
    if not os.path.exists(fname):
        try:
            gr.get_large_report(reportnumber,startdate,enddate,fw,site=site)
        except OSError as e:
            fn=e.filename
            print(e)
        except Exception as e:
            print(e)

    esconn=es.Elasticsearch()
    #esconn.indices.delete(index=index,ignore=[400, 404])
    #esconn.indices.create(index=index, ignore=400, body=mapping)
    #mapping = "{\"mappings\":{\"logs_june\":{\"_timestamp\": {\"enabled\": \"true\"},\"properties\":{\"logdate\":{\"type\":\"date\",\"format\":\"dd/MM/yyy HH:mm:ss\"}}}}}"
#     request_body = {
#     "settings" : {
#         "number_of_shards": 1,
#         "number_of_replicas": 0
#     },
#     "mappings" : {
#         "_default_":{
#             "timestamp":{
#                  "enabled":"true",
#                  "store":"true",
#                  "path":"plugins.time_stamp.string",
#                  "format":"yyyy-MM-dd HH:m:ss"
#              }
#          }
#     }
# }
#     esconn.indices.create(index=index, ignore=400, body=request_body)
    actions=[]
    with open(fname) as ff:
        ts1=defaultdict(set)
        ts2=defaultdict(set)
        for iii,line in enumerate(ff):
            doc={}
            line=line.strip()
            line=line.split('\t')
            doc['a_virtual']=line[0]
            dtl=line[1].split('-')[::-1]
            dtl.append(line[2])
            dtl=[int(d) for d in dtl]
            dt=datetime.datetime(*dtl)

            stringtime=dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

            doc['timestamp']=stringtime
            ts1[dt].add(line[3])
            #doc['timestamp']=dt
            #print(line[-2:])
            metrics=[int(bool(int(float(k)))) for k in line[-2:]]
            doc['paidcont']=metrics[-1]
            doc['cont']=metrics[-2]
            doc['a_user_key']=line[3]

            if doc['paidcont']:
                ts2[dt].add(line[3])

            action={}
            action['_op_type']='index'
            action['_index']=index
            action['_type']=doc_type
            action['_source']=doc
            actions.append(action)
            # if len(actions)>10000:
            #     eshelper.bulk(esconn,actions)
            #     actions.clear()
            # tt=esconn.create(index="test234", doc_type="articles", body=doc)
            if iii%10000==0:
                print(iii)
            #actions.append(json.dumps(action))
    # print(actions[:5])
    # if len(actions)>0:
    #     eshelper.bulk(esconn,actions)
    #     actions.clear()
    #esconn.index(index='innh',doc_type='user',body=doc)
    templist=[]
    for ts,users in ts1.items():
        templist.append([ts,ts.day,ts.hour,users,ts2[ts]])
    #esconn.indices.delete(index="test888",ignore=[400, 404])
    templist2=sorted(templist,key=lambda x:(x[1],x[2]))
    groups=groupby(templist2,key=lambda x:x[1])
    iii=0
    for group in groups:
        print(group)
        a={}
        tempset1=set()
        tempset2=set()
        for line in group[1]:
            a['timestamp']=line[0]
            tempset1.update(line[3])
            tempset2.update(line[4])
            a['n_users']=len(tempset1)
            a['n_users_paid']=len(tempset2)
            tt=esconn.create(index="test888",id=iii, doc_type="articles", body=a)
            iii+=1
    #     a.clear()





    # pprint(groupToInd)
    # pprint(splDict)
    # sys.exit(1)

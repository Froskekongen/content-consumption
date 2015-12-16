import datetime
from pyspark.sql import functions as sqlfuncs
from ast import literal_eval
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,Row
from collections import Counter
import numpy as np
from pprint import pprint

import pickle as pkl

from pyspark.ml.feature import OneHotEncoder,StringIndexer,\
    VectorAssembler,CountVectorizer,Normalizer


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
        return dd

splDict=split_seq(list(range(24)),6)
filterSet=set(['nyheter','nyhet','pluss','anpluss','nyheiter','nyheit'])

def parseTags(xx):
    a_virtual=xx[0]
    browser=xx[1]
    a_user_key=xx[2]
    date=datetime.datetime.strptime(xx[3],'%d-%m-%Y')
    hour=int(xx[4])
    weekday=date.weekday()
    tags=xx[5].lower().split('|')
    n_tags=len(tags)
    posts_with_tags=1.

    tags=[tag for tag in tags if tag not in filterSet]
    timegroup=str( (weekday,splDict[hour]) )
    return Row(a_user_key=a_user_key,browser=browser,a_virtual=a_virtual,\
        date=date,weekday=weekday,timegroup=timegroup,tags=tags,n_tags=n_tags,\
        posts_with_tags=posts_with_tags)





if __name__ == "__main__":
    conf=SparkConf().setAppName('tagprofiler').setMaster("local[8]").set('spark.app.id','200')

    sc=SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    period=2. #2 weeks of data

    #filterSet=set(['nyheter','nyhet','pluss','anpluss','nyheiter','nyheit'])

    rawdata=sc.textFile('/home/erlenda/data/konsum/amedia_mainsite_20151124-20151207_15134.tsv')\
        .map(lambda x:x.split('\t'))\
        .map(parseTags)\
        .filter(lambda x:x.tags!=[''])\
        .filter(lambda x:x.tags!=[])

    a_user_tag_konsum=rawdata\
        .filter(lambda x:x.a_user_key!='NAN')\
        .filter(lambda x:x.a_user_key!='')
    #a_user_tag_konsum=sc.parallelize(a_user_tag_konsum.take(1000))
    #browser_tag_konsum=rawdata.filter(lambda x:x.a_user_key=='NAN').map(lambda x:(x.browser,x.tags)) ## dataset without browsers #FIXME
    print(a_user_tag_konsum.take(2))



    # totalTags=a_user_tag_konsum.map(lambda x:x.tags).collect()
    # print(totalTags[:6])
    #
    # cc=Counter()
    # for t in totalTags:
    #     for tag in t:
    #         cc[tag]+=1
    #
    #
    # print(cc.most_common(20))





    tags_users=a_user_tag_konsum.map(lambda x:((x.a_user_key,x.a_virtual),(x.tags,x.n_tags,x.posts_with_tags)))\
        .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1],a[2]+b[2]))\
        .map(lambda x:Row(a_user_key=x[0][0],a_virtual=x[0][1],tags=x[1][0],n_tags=x[1][1],posts_with_tags=x[1][2]))#\

    #print(tags_users.take(10))

    # alltags=tags_users.map(lambda x:Counter(x.tags)).reduce(lambda a,b:a+b)
    # print(alltags.most_common(10))
        #.filter(lambda x:len(x.tags)>100) # filtering to get smaller dataset

    # print(tags_users.count())
    # print(tags_users.first())

    ## Filtered for testing

    tags_users_df=sqlContext.createDataFrame(tags_users)
    print(tags_users_df.take(2))
    #
    #
    # print('Indexing strings')
    cVec = CountVectorizer(inputCol='tags', outputCol="tag_features",minDF=10.)
    model=cVec.fit(tags_users_df)
    td=model.transform(tags_users_df)

    with open('/home/erlenda/data/konsum/countvec_vocabulary.pkl',mode='wb') as ff:
        pkl.dump(model.vocabulary,ff)



    normalizer=Normalizer(p=1.,inputCol='tag_features',outputCol='tags_normalized')
    tdNorm=normalizer.transform(td)
    print(tdNorm.take(5))

    tdNorm.write.save('/home/erlenda/data/konsum/tag_profiler_parquet')

    samples=tdNorm.filter(tdNorm.posts_with_tags>10).take(10)
    #pprint(samples)




    # stringIndexer = StringIndexer(inputCol="tags", outputCol="indexed_tags")
    # model=stringIndexer.fit(tags_users_df)
    # td=model.transform(tags_users_df)
    # print('Retrieving indices')
    #
    # print('OneHotEncoding')
    # encoder=OneHotEncoder(inputcol='stringindices',outputcol='features')



    # tags_browsers=browser_tag_konsum.reduceByKey(lambda a,b:a+b)
    # print(tags_users.take(5))
    # print(tags_browsers.take(5))

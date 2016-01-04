import numpy as np
import pickle as pkl

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LassoCV, LassoLarsCV, LassoLarsIC
from sklearn.cross_validation import StratifiedKFold,train_test_split
from sklearn.metrics import classification_report
from sklearn.linear_model import LogisticRegression

import matplotlib.pyplot as plt
from scipy.sparse import csr_matrix

if __name__ == "__main__":
    aviser=['an','ba','dt','fb','havis','oa','rb',\
        'ta','tb','nordlys','firda','glomdalen',\
        'mossavis','ringblad','sb','sa',\
        'tk','op','ostlendingen']

    aviser=set(['rb'])
    dev=set(['Windows Desktop','Macintosh Desktop','Linux Desktop'])
    with open('../data/amedia_mainsite_20151001-20151213_15331.tsv') as ff:
        noset=set(['','nan'])
        feats=[]
        targets=[]
        over5=[]
        targets5=[]
        for iii,line in enumerate(ff):
            line=line.split('\t')
            avis=line[0]
            if line[4] not in dev:
                continue
            if avis not in aviser:
                continue
            line=[l.strip() for l in line]

            konvrate=float(line[-1])
            totalkjop=float(line[7])
            konvrate=0. if konvrate==float('nan') else konvrate
            if totalkjop>0:
                target1=np.log10(totalkjop+0.5)*konvrate
            else:
                target1=0.
            if target1==np.float('inf'):
                # print(line)
                # print(konvrate,totalkjop)
                continue
                #input()

            tags=line[5].lower()
            if tags in noset:
                continue

            tags=tags.split('|')
            byline=line[6].lower()
            if byline.startswith('av '):
                byline=byline[3:]

            byline=byline.split(',')
            byline=[bl.split(' og ') for bl in byline]
            byline=[item for sublist in byline for item in sublist]
            if byline==['nan']:
                byline=[]

            features=tags+byline
            features=[f for f in features if f not in noset]
            if features==[]:
                continue
            if totalkjop>4:
                print(target1,konvrate,totalkjop,features)
                print(line)


            feats.append(features)
            if totalkjop>4.5:
                targets.append([konvrate,target1,1])
            else:
                targets.append([konvrate,target1,0])

    targets=np.asarray(targets)
    with open('../data/konvdata_rb.pkl',mode='wb') as ff:
        pkl.dump([feats,targets],ff,protocol=4)

    with open('../data/konvdata_rb.pkl',mode='rb') as ff:
        feats,targets=pkl.load(ff)
    print(targets.shape)

    tt=targets[:,2]
    print(tt.min(),tt.max())

    cVec=CountVectorizer(analyzer=lambda x:x,min_df=10,max_df=0.5)
    M=cVec.fit_transform(feats)
    print(M.max(),M.min(),M.shape)


    M=csr_matrix(M,dtype=np.float64)
    logRegParams={'n_jobs':-1,'class_weight':'balanced','penalty':'l1','C':0.5}
    classifier=LogisticRegression
    skf = StratifiedKFold(tt, n_folds=4)
    for train_index,test_index in skf:
        clf=classifier(**logRegParams)
        X_train, X_test = M[train_index,:], M[test_index,:]
        y_train, y_test = tt[train_index], tt[test_index]
        clf.fit(X_train,y_train)
        y_pred=clf.predict(X_test)
        print(classification_report(y_test,y_pred))


import pickle as pkl
import numpy as np
from collections import Counter
from sklearn.feature_extraction import DictVectorizer
from sklearn.decomposition import NMF,TruncatedSVD
from sklearn.preprocessing import StandardScaler, RobustScaler,Normalizer,StandardScaler
from pandas.tools.plotting import scatter_matrix
from pandas import DataFrame as paDataFrame
import matplotlib.pyplot as plt

from sklearn.cluster import MeanShift, estimate_bandwidth

from sklearn.cluster import DBSCAN
from sklearn import metrics

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

if __name__ == "__main__":
    # with open('/home/erlenda/notebook/ErlendA/konsum_rb_user.pickle',mode='br') as ff:
    #     konsum_user=pkl.load(ff)
    #
    # splDict=split_seq(list(range(24)),6)
    # userDD={}
    # for r in konsum_user:
    #     if r.a_user_key not in userDD:
    #         userDD[r.a_user_key]={'pv':Counter(),'pv_nh':Counter(),'pv_bet':Counter()}
    #
    #     userDD[r.a_user_key]['age']=r.age
    #     userDD[r.a_user_key]['gender']=r.gender
    #     userDD[r.a_user_key]['pv'][(r.weekday,splDict[r.hour])]+=r.pv/3
    #     userDD[r.a_user_key]['pv_nh'][(r.weekday,splDict[r.hour])]+=r.pv_nh/3
    #     userDD[r.a_user_key]['pv_bet'][(r.weekday,splDict[r.hour])]+=r.pv_bet/3
    #
    # with open('users_dd.pkl',mode='wb') as ff:
    #     pkl.dump(userDD,ff)

    with open('users_dd.pkl',mode='rb') as ff:
        userDD=pkl.load(ff)

    indToKey=[]
    tdicts=[]
    tdicts_bet=[]

    for iii,(user_key,vals) in enumerate(userDD.items()):
        indToKey.append([iii,user_key,vals['age'],vals['gender']])
        tdicts.append(dict(vals['pv']))
        tdicts_bet.append(dict(vals['pv_bet']))

    teenageInds=np.array([iii for iii,_,age,_ in indToKey if 11<age<20])


    dVec=DictVectorizer(sparse=False)
    featMatrix=dVec.fit_transform(tdicts)
    print(featMatrix.shape)
    percentiles=[]
    perc_print=[]
    very_active_inds=[]
    badInds=set()

    totalPVs=featMatrix.sum(axis=1)
    very_active=np.percentile(totalPVs,80)
    active=np.percentile(totalPVs,60)
    bad_perc=np.percentile(totalPVs,95)
    bad_perc1=np.percentile(totalPVs,10)


    va_inds=np.where(  (totalPVs<very_active) & (totalPVs>=active)  )[0]
    va_inds=np.intersect1d(va_inds,teenageInds)
    print(bad_perc,bad_perc1)



    bad_inds=np.where(totalPVs>bad_perc)[0]
    bad_inds1=np.where(totalPVs<=5)[0]
    bad_inds=np.union1d(bad_inds,bad_inds1)

    very_active_inds=np.setdiff1d(va_inds,bad_inds)
    print(va_inds.shape,bad_inds.shape,very_active_inds.shape)

    featMatrix=featMatrix[very_active_inds,:]
    print('Teenagers',featMatrix.sum(axis=0))


    featMatrixNormalized=Normalizer(norm='l2').fit_transform(featMatrix)
    featMatrixSTD=StandardScaler().fit_transform(featMatrix)
    featMatrixSTD=featMatrixSTD#+np.abs(featMatrixSTD.min())+1.e-15
    print(featMatrixSTD.min())
    #featMatrix=RobustScaler(with_centering=False).fit_transform(featMatrix)

    nmfTrf=TruncatedSVD(n_components=10)
    nmfFeats=nmfTrf.fit_transform(featMatrixSTD)
    dfTest=paDataFrame(featMatrixSTD[:,:10])

    corr=np.dot(featMatrix,featMatrix.T)
    print(corr.shape)

    bandwidth = estimate_bandwidth(featMatrix, quantile=0.2, n_samples=500)
    ms = MeanShift(bandwidth=bandwidth*0.7, bin_seeding=True)
    print('bandwidth',bandwidth)
    labels=ms.fit_predict(featMatrix)


    # db = DBSCAN(eps=0.2, min_samples=10,metric='precomputed')
    # dMat=1.-corr
    # labels=db.fit_predict(dMat)
    print(np.unique(labels))
    sorted_labels=np.argsort(labels)
    print(sorted_labels)
    corrSorted=corr[sorted_labels,:]
    corrSorted=corrSorted[:,sorted_labels]
    print(corr.shape,corrSorted.shape)


    lab1=np.where(labels==1)[0]
    lab2=np.where(labels==2)[0]


    #scatter_matrix(dfTest)
    #plt.imshow(corrSorted)

    #plt.show()




print(corr)

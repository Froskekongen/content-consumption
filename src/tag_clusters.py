from collections import defaultdict,Counter
from sklearn.feature_extraction import DictVectorizer
from sklearn.decomposition import TruncatedSVD,NMF


def count_tags(taglist,observations):
    countVisit=Counter()
    countViews=Counter()
    countBetalt=Counter()

    for tag in taglist:
        countVisit[tag]+=observations[0]
        countViews[tag]+=observations[1]
        countBetalt[tag]+=observations[2]
    return countVisit,countViews,countBetalt
if __name__ == "__main__":
    with open('/mnt/tagkonsum/tag_konsum_rb.tsv') as ff:
        tagkonsum=[]
        for iii,line in enumerate(ff):
            ll=line.split('\t')
            ll=[l.lower().strip() for l in ll]
            ll[-3:]=[float(l.replace(',','.')) for l in ll[-3:]]

            ll[-4]=ll[-4].split('|')
            if ll[-4]==['']:
                continue
            countVisit,countViews,countBetalt=count_tags(ll[-4],ll[-3:])
            # if ll[-1]>0:
            #     countBetalt

            tagkonsum.append({'key':ll[2],'visits':countVisit,'views':countViews,'betalt':countBetalt})
            if iii%100000==0:
                print(tagkonsum[-1])

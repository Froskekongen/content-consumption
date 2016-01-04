
import numpy as np
import plotly.plotly as py
from collections import Counter,defaultdict
import plotly.graph_objs as go
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
    if other>0:
        ol.append('andre')
        oo.append(other)
    return ol,oo

agegroups=((-1,12),(12,20),(20,30),(30,40),(40,50),(50,60),(60,70),(70,140),(140,2000))
ageToGroup={}
for gr in agegroups:
    for jjj in range(gr[0],gr[1]):
        if (gr==(-1,12) or gr==(140,2000)):
            gr='ukjent'
        ageToGroup[jjj]=gr

if __name__ == "__main__":

    width=600
    height=350
    font=dict(family='Courier New, monospace', size=19, color='#7f7f7f')
    domain={'x': [0.,0.85 ]}
    #print(ageToGroup)
    with open('../data/konsum_alder.csv') as ff:
        avis=[]
        nbrowsers=[]
        pvs=[]
        listAG=[]
        cdicts=defaultdict(Counter)
        for iii,line in enumerate(ff):
            line=line.split('\t')
            line=[l.strip() for l in line]
            browsers=int(line[2].replace(',0','').replace(',',''))
            pageviews=int(line[3].replace(',0','').replace(',',''))
            # if iii%10==0:
            #     print(line)
            #     print(pageviews)
            av=line[0]


            try:
                age=2015-int(line[1])
            except:
                age=-1
            agegr=ageToGroup[age]
            cdicts[av][agegr]+=pageviews




    aviser=['an','ba','dt','fb','havis','oa','rb',\
        'ta','tb','nordlys','firda','glomdalen',\
        'mossavis','ringblad','sb','sa',\
        'tk','op','ostlendingen']
    for av in aviser:
        cc=cdicts[av]
        labels=list(cc.keys())
        vals=list(cc.values())
        plotdict={}
        plotdict['labels']=labels
        plotdict['values']=vals
        plotdict['type']='pie'
        plotdict['domain']=domain
        layout=go.Layout(font=font,title='aID, aldergruppering i <br>antall sidevisninger, {0}'.format(av))
        fig2={'data':[plotdict],\
            'layout':layout,\
            'font':font\
            }
        fn2="pics/"+av+"_agegroup.png"
        py.image.save_as(fig2,fn2,format='png',width=width,height=height,scale=3)
    cdicts.clear()
    with open('../data/konsum_device.csv') as ff:
        avis=[]
        nbrowsers=[]
        pvs=[]
        listAG=[]
        cdicts=defaultdict(Counter)
        for iii,line in enumerate(ff):
            line=line.split('\t')
            line=[l.strip() for l in line]
            pvs=int(line[2].replace(',0','').replace(',',''))
            avis=line[0]
            device=line[1]
            cdicts[avis][device]+=pvs

    for av in aviser:
        cc=cdicts[av]
        labels=list(cc.keys())
        vals=list(cc.values())
        ol,ov=trimSmallPercentages(labels,vals)
        plotdict={}
        plotdict['labels']=ol
        plotdict['values']=ov
        plotdict['type']='pie'
        plotdict['domain']=domain
        layout=go.Layout(font=font,title='aID, enhet i <br>antall sidevisninger, {0}'.format(av))
        fig2={'data':[plotdict],\
            'layout':layout,\
            'font':font\
            }
        fn2="pics/"+av+"_device.png"
        py.image.save_as(fig2,fn2,format='png',width=width,height=height,scale=3)

    cdicts.clear()
    with open('../data/konsum_gender.csv') as ff:
        avis=[]
        nbrowsers=[]
        pvs=[]
        listAG=[]
        cdicts=defaultdict(Counter)
        for iii,line in enumerate(ff):
            line=line.split('\t')
            line=[l.strip() for l in line]
            pvs=int(line[2].replace(',0','').replace(',',''))
            avis=line[0]
            gender=line[1] if (('m' in line[1].lower()) or ('f' in line[1].lower())) else 'ukjent'
            cdicts[avis][gender]+=pvs

    for av in aviser:
        cc=cdicts[av]
        labels=list(cc.keys())
        vals=list(cc.values())
        #ol,ov=trimSmallPercentages(labels,vals)
        plotdict={}
        plotdict['labels']=labels
        plotdict['values']=vals
        plotdict['type']='pie'
        plotdict['domain']=domain
        layout=go.Layout(font=font,title='aID, kjønn i <br>antall sidevisninger, {0}'.format(av))
        fig2={'data':[plotdict],\
            'layout':layout,\
            'font':font\
            }
        fn2="pics/"+av+"_gender.png"
        py.image.save_as(fig2,fn2,format='png',width=width,height=height,scale=3)

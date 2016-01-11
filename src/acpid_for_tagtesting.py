
from itertools import groupby
import numpy as np
from pprint import pprint

import requests

import json
from pprint import pprint

import re
import matplotlib.pyplot as plt

import html


PAR_RE = re.compile(r'</p>')
TAG_RE = re.compile(r'<[^>]+>')
REPEATING_NEWLINES_RE=re.compile(r'(\s)\1{2,}',re.UNICODE)
EXPLICIT_NEWLINE_RE=re.compile(r'\n')
BRODTEKST=re.compile('Dette er brødtekst')

GET_ACPID=re.compile(r'/([0-9]+\-[0-9]+\-[0-9]+)\.')

def process_body(txt):
    re_txt=EXPLICIT_NEWLINE_RE.sub(' ',txt)
    re_txt=PAR_RE.sub('\n\n',re_txt)
    re_txt=TAG_RE.sub('',re_txt)
    re_txt=re_txt.strip()
    re_txt=REPEATING_NEWLINES_RE.sub(r'\1\1',re_txt)
    return html.unescape(re_txt)

def get_body(txt_raw):
    txt=process_body( txt_raw )
    return txt

with open('../data/amedia_mainsite_20151101-20151231_15431.tsv') as ff:
    artlist=[]
    for line in ff:
        ff=line.split('\t')
        ff[-2:]=[float(k.strip()) for k in ff[-2:]]
        artlist.append(ff)

#print(artlist[:5])

s_artlist=sorted(artlist,key=lambda x:(x[0],x[1],x[-1],x[-2]))[::-1]

acpids=[]
for key,group in groupby(s_artlist,key=lambda x:(x[0],x[1])):
    gg=list(group)
    if key[1]=='external-content':
        continue
    elif key[1]!='story':
        acpids.append((key,gg[0][2]))
    else:
        wcs=np.asarray([g[-2] for g in gg])
        percs=np.percentile(wcs,[10,90])
        art1=np.where(wcs<percs[0]+1.e-6)[0][0]
        art2=np.where(wcs>percs[1]-1.e-6)[0][0]
        art1=gg[art1]
        art2=gg[art2]
        acpids.append(((art1[0],art1[1]),art1[2]))
        acpids.append(((art2[0],art2[1]),art2[2]))
        print(percs)

print(len(acpids))
acpids.append((('tk','edge'),'5-51-128354'))
acpids.append((('rb','edge'),'5-43-214886'))
acpids.append((('ranablad','edge'),'5-42-84385'))
acpids.append((('nordlys','edge'),'5-34-341782'))
acpids.append((('oa','edge'),'5-35-219837'))
acpids.append((('fb','edge'),'5-59-348842'))
acpids.append((('sa','edge'),'5-47-49967'))
acpids.append((('tb','edge'),'5-76-226834'))
acpids.append((('op','edge'),'5-36-152718'))
acpids.append((('lofotposten','edge'),'5-29-158742'))










baseurl="http://bed.api.no/api/acpcomposer/v1/content/"

stories=[]
iii=0
jjj=0
for acpid in acpids:
    dd={}
    try:
        cont=requests.get(baseurl+acpid[1]).json()
    except Exception as e:
        jjj=jjj+1
        print(e)

    try:
        txt=process_body(cont["body"])
        brodtekst=BRODTEKST.findall(txt)
        if len(txt)==0:
            continue
        if len(brodtekst)>1:
            continue
        print(txt)
        dd['publication']=acpid[0][0]
        dd['type']=acpid[0][1]
        dd['acpid']=acpid[1]
        dd['text']=txt
        dd['length']=len(txt)
        stories.append(dd)
        iii+=1
    except KeyError as e:
        pprint(cont)
        pprint(cont['model'])
        #input()
    print(iii,jjj)
    print()
print(iii,jjj)
for st in stories:
    print(st['type'])
print(len(stories))

with open('../data/test_saker.json',mode='w') as ff:
    json.dump(stories,ff,ensure_ascii=False,indent=2)

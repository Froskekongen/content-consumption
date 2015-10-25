from boto.s3.connection import S3Connection
from boto.s3.key import Key
import glob
import sys

fn=sys.argv[3]

conn=S3Connection(sys.argv[1],sys.argv[2])
bucket=conn.get_bucket('s3-acpcontent')

with open(fn) as ff:
    dumpdata=[]
    n_lines=0
    lines_per_file=5000000
    for iii,line in ff:
        n_lines+=1
        dumpdata.append(line)
        if (n_lines%lines_per_file)==0:
            print(line)

            dumpdata.clear()

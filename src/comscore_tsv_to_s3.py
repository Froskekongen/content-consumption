from boto.s3.connection import S3Connection
from boto.s3.key import Key
import glob
import sys

fn=sys.argv[3]

conn=S3Connection(sys.argv[1],sys.argv[2])
bucket=conn.get_bucket('s3-comscore-consumption-daily')

with open(fn) as ff:
    key_in_bucket=fn.split('/')[-1]
    dumpdata=''
    n_lines=0
    lines_per_file=5000000
    for iii,line in enumerate(ff):
        n_lines+=1
        dumpdata+=line
        if (n_lines%lines_per_file)==0:
            print(line)

            output=''.join(dumpdata)
            dumpdata.clear()
            k=Key(bucket)
            k.key=key_in_bucket+'/'+str(n_lines).zfill(11)+'.tsv'
            k.set_contents_from_string(output)

    if len(dumpdata)>0:
        output=''.join(dumpdata)
        dumpdata.clear()
        k=Key(bucket)
        k.key=key_in_bucket+'/'+str(n_lines).zfill(11)+'.tsv'
        k.set_contents_from_string(output)

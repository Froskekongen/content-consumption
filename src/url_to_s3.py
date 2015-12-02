from boto.s3.connection import S3Connection
import glob
import sys
import requests
#print(sys.argv)

conn=S3Connection(sys.argv[1],sys.argv[2])
bucket=conn.get_bucket('s3-comscore-consumption-daily')


def download_file(url):
    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024*10):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
    return local_filename

cs_url=sys.argv[3]

dlfile = download_file(cs_url)

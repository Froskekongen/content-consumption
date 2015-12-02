
import sqlalchemy as sa
from datetime import datetime
import sys

DB='brukerkl.csqsod8x7dsm.eu-west-1.rds.amazonaws.com:5432/brukerkl'
url_to_get="""https://dax-rest.comscore.eu/v1/reportitems.xml?itemid={0}&startdate={1}&enddate={2}&site=amediatotal&format=csv&client=amedia&parameters=Page:*&user={3}&password={4}"""


from datetime import datetime,timedelta

import requests


if __name__ == "__main__":
    import sys

    today=datetime.today()
    yesterday=today-timedelta(days=1)
    enddate=datetime.strftime(yesterday,'%Y%m%d')
    sd=yesterday-timedelta(days=1)
    startdate=datetime.strftime(sd,'%Y%m%d')

    reportItems=[15010]


    for ri in reportItems:
        # remember to add credentials to command line argument
        url=url_to_get.format(ri,startdate,enddate,sys.argv[1],sys.argv[2])
        print(url)
        # r=requests.get(url)
        # print(r.status_code)
        # print(r.text)
        # print(r.json())



    creds=sys.argv[1]
    engine_txt='postgresql://'+creds+'@'+DB

    engine = sa.create_engine(engine_txt)


    meta=sa.MetaData(bind=engine)


    testTable=sa.Table('testing123',meta,sa.Column('id', sa.Integer, primary_key=True),\
        sa.Column('name', sa.String),\
        sa.Column('fullname', sa.String) )


    csTable=sa.Table('PlussKonsum',meta,\
        sa.Column('avis',sa.String),\
        sa.Column('uke',sa.Integer),\
        sa.Column('dato',sa.DateTime) )

    meta.create_all()




    ## test connection
    #print(meta.tables.keys())

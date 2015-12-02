import sys

def define_numeric_fields(x):
    numfields=[2,16,30,31,32]
    nan=float('nan')
    for ind in numfields:
        try:
            f=x[ind].replace(',','.')
            x[ind]=float(f)
        except Exception as e:
            x[ind]=nan
    return x

ip = sys.argv[1]

rdd=sc.textFile(ip).map(lambda x:x.split('\t')).map(define_numeric_fields)
rdd_a_user=rdd.map(lambda x: (x[0],x[1:4])).filter(lambda x: (x[1][0] != '') and (x[1][0] != 'NA') )

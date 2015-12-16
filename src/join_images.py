
from PIL import Image
from glob import glob

if __name__ == "__main__":
    aviser=['an','ba','dt','fb','havis','oa','rb',\
        'ta','tb','nordlys','firda','glomdalen',\
        'mossavis','ringblad','sb','sa',\
        'tk','op','ostlendingen']

    for avis in aviser:
        picfns=glob('./pics/{0}*'.format(avis))
        res=Image.new("RGBA",(500*3,1200*3))
        y=0
        for fn in picfns:
            imm=Image.open(fn,mode='r')
            res.paste(imm,(0,y))
            y+=imm.size[1]
        res.save('./pics/joined/{0}_all.png'.format(avis))

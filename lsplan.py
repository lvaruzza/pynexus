from  pynexus import *

def lsplan(gnx,args):
    xs=gnx.plans()
    print("#shortId","samples","planName")
    for x in xs:
        print(x['planShortId'],x['noOfLibraries'],x['planName'])
        #print(json.dumps(x, indent=4, sort_keys=True))

main(lsplan)
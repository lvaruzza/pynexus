from  pynexus import *

with open('genexus.json') as f:
    cfg = json.load(f)

gnx = Genexus(cfg['server'],cfg['username'],cfg['password'])

xs=gnx.plans()
for x in xs:
    print(x['planShortId'],x['noOfLibraries'],x['planName'])
    #print(json.dumps(x, indent=4, sort_keys=True))

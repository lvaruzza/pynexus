import argparse
import pynexus
from lsplan import *
import os

## changed directory to where the config files are stored and 
## where the downloaded files should be stored
# os.chdir('../outputs')


def main(argparser=None):
    """
    Function  to download a files selected by file extensions
    for all samples in runs named in 'planName' file. From a selected
    Genexus.
    """


    if not argparser:
        argparser=argparse.ArgumentParser()
    
    print(argparser)
    
    argparser.add_argument('-c','--config',default='genexus.json')
    args = argparser.parse_args()
    with open(args.config) as f:
        cfg = json.load(f)
    with open(cfg['planNamesFile']) as f:
        plans = f.read().strip().split()
    gnx = pynexus.Genexus(cfg['server'],cfg['username'],cfg['password'])
    #bai files are bam index files small and confirm that the download is working since bai files should,
    #in most cases, be present even if a vcf was not produced.
    downloadSampleFilesFromPlan(gnx, plans, extensions=['bai','vcf'],pageSize=600,
                                signedOff=False,return_files_with_ext=True )

main()
    

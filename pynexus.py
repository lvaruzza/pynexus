import requests
import json
import zipfile
import itertools as iter
import argparse
#requests must be installed using pip

from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

def parse_checksum(file):
    file_list= [ line.decode("UTF-8").rstrip().split()[1] for line in file.readlines()] 
    return file_list
    
class PagingResults:
    def __init__(self,server,request,params):
        self.server = server
        self.i=0
        self.request = request
        self.params = params
        self.objects = []
        self.page_size=32
        self.page=0

    def load(self):
        #print("Loading...")
        self.params['pageSize']=self.page_size
        self.params['pageNumber']=self.page
        resp = self.server._api_call_(self.request,self.params)
        if (resp.ok):
            rj = json.loads(resp.text)
            #print(json.dumps(rj['meta'], indent=4, sort_keys=True))

            self.objects = rj["objects"]
            self.objects.reverse()
            return True
        else:
            return False


    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def reload(self):
        self.page = self.page + 1        
        #print("============> REALOD page " + str(self.page))
        self.load()
        if len(self.objects)==0:
            raise StopIteration()


    def next(self):
        self.i = self.i +1
        if not self.objects:
            self.reload()
        return self.objects.pop()


class Genexus:
    def __init__(self,server,username,password):
        self.server=server
        self.username=username
        self.password=password
        self.headers = {'username':self.username,'password':self.password}
        self.api_url = "/genexus/api/lims/v2/"

    def _api_call_(self,request,params={}):
        url = self.server + self.api_url + request
        #print("Request: " + url + " params=" + str(params))
        r = requests.get(url,headers=self.headers,params=params,verify=False)
        #print(r.url)
        return(r)

    def getJson(self,request,params={}):
        r = self._api_call_(request,params)
        rj = json.loads(r.text)
        return rj['objects']

    def request(self,request,params={}):
        return PagingResults(self,request,params)

    def _download_(self,result,file):
        print("Saving file to " + file)
        with open(file, 'wb') as f:
            for chunk in result.iter_content(chunk_size=512 * 1024): 
                if chunk: # filter out keep-alive new chunks
                    print(".",end='')
                    f.write(chunk)
            print()

    def _final_files_(self,file_list,extensions,return_files_containing_ext=False):
        """
        params:
        file_list: list containing all files available for download
        extension:  list containing extensions to match
        return_files_containing_ext: boolean If False (default) any file that has an extension contained in extension 
                                     list is not returned. If True only files that have extension contained in list 
                                     are returned.
        
        """
        _final_files = []
        _exclude_files = []
        for file in file_list:
            _ext = file.split(".")[-1]
            if _ext not in extensions:
                _final_files.append(file)
            else:
                _exclude_files.append(str(file))
 
        if return_files_containing_ext is False:
            _exclude_file_length = len(_exclude_files)
            _exclude_files_str = '\n\t\t'.join(_exclude_files)
            print(f'{_exclude_file_length} files excluded:\n{_exclude_files_str}\n----------------------')
            return _final_files
        
        elif return_files_containing_ext is True:
            _exclude_file_length = len(_final_files)
            _exclude_files_str = '\n\t\t'.join(_final_files)
            print(f'{_exclude_file_length} files excluded:\n{_exclude_files_str}\n----------------------')
            return _exclude_files




    def download(self,sample,extensions=['bai','bam','fastq'],return_files_with_ext=False):
        """
        params:
        sample: dictionary containing key:value pair 'base_url` : target url to append to API request.
        extensions: list of extensions to be included or excluded in final download. Typical extensions include:
                    'bam, bai, fastq, txt, log, png, xls, tsv'. 
        return_files_with_ext:  boolean If True only files that have extensions matching an element of the extensions
                                list are returned. If False (default) no with an extension matching the an element of 
                                the extensions list is returned.

        """
        r=self._api_call_("download",{'file_list':"CHECKSUM",
                                "path":sample['base_url']})

        #print(r.headers)
        chksum_zip=sample['base_url'] + "_CHKSUM.zip"
        self._download_(r,chksum_zip)
        with zipfile.ZipFile(chksum_zip) as zip:
            with zip.open('CHECKSUM') as file:
                file_list=parse_checksum(file)
                #final_files = [file for file in file_list if not (file.endswith(".bam") or file.endswith('.bai') or file.endswith('fastq'))]
                final_files =  self._final_files_(file_list,extensions,return_files_containing_ext=return_files_with_ext)
                print(final_files)
                r=self._api_call_("download",{'file_list':",".join(final_files),
                                    "path":sample['base_url']})
            
                self._download_(r,sample['base_url'] + ".zip") 

    def signedOffSamples(self):
        return self.getJson("signedOffSamples",{"signedOff":False})
    
    #
    # API
    #

    def plans(self):
        return self.request("plans")


    def results(self):
        return self.request("results")
    def result(self,planName):
        return self.request("results",{'planName':planName})

    def samples(self):
        return self.request("samples")


class Sample:
    def __init__(self,planShortId,planName,sampleId):
        self.planShortId = planShortId
        self.planName = planName
        self.sampleId = sampleId
    
    def __str__(self):
        return f'self.planShortId={self.planShortId} self.planName={self.planName} self.sampleId={self.sampleId}\n'
    
def main(tool,argparser=None):
    if not argparser:
        argparser=argparse.ArgumentParser()
    
    print(argparser)
    
    argparser.add_argument('-c','--config',default='genexus.json')
    args = argparser.parse_args()
    with open(args.config) as f:
        cfg = json.load(f)

    gnx = Genexus(cfg['server'],cfg['username'],cfg['password'])
    tool(gnx,args)

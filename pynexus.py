import requests
import json
import zipfile

from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

def parse_checksum(file):
    file_list= [ line.decode("UTF-8").rstrip().split()[1] for line in file.readlines()] 
    return file_list
    
class Genexus:
    def __init__(self,server,username,password):
        self.server=server
        self.username=username
        self.password=password
        self.headers = {'username':self.username,'password':self.password}
        self.api_url = "/genexus/api/lims/v2/"

    def _api_call_(self,request,params={}):
        url = self.server + self.api_url + request
        print("Request: " + url + " params=" + str(params))
        r = requests.get(url,headers=self.headers,params=params,verify=False)
        return(r)

    def getJson(self,request,params={}):
        r = self._api_call_(request,params)
        rj = json.loads(r.text)
        #print(json.dumps(rj, indent=4, sort_keys=True))
        return rj['objects']

    def results(self):
        return self.getJson("results")

    def _download_(self,result,file):
        print("Saving file to " + file)
        with open(file, 'wb') as f:
            for chunk in result.iter_content(chunk_size=512 * 1024): 
                if chunk: # filter out keep-alive new chunks
                    print(".",end='')
                    f.write(chunk)
            print()

    def download(self,sample):
        r=self._api_call_("download",{'file_list':"CHECKSUM",
                                "path":sample['base_url']})

        #print(r.headers)
        chksum_zip=sample['base_url'] + "_CHKSUM.zip"
        self._download_(r,chksum_zip)
        with zipfile.ZipFile(chksum_zip) as zip:
            with zip.open('CHECKSUM') as file:
                file_list=parse_checksum(file)
                final_files = [file for file in file_list if not (file.endswith(".bam") or file.endswith('.bai') or file.endswith('fastq'))]
                r=self._api_call_("download",{'file_list':",".join(final_files),
                                    "path":sample['base_url']})
            
                self._download_(r,sample['base_url'] + ".zip") 

    def signedOffSamples(self):
        return self.getJson("signedOffSamples",{"signedOff":False})
    

with open('genexus.json') as f:
    cfg = json.load(f)

print(cfg)

gnx = Genexus(cfg['server'],cfg['username'],cfg['password'])
samples = gnx.signedOffSamples()
for sample in samples:
    files=gnx.download(sample['sample'])

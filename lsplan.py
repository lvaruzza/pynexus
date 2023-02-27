from collections import namedtuple
from  pynexus import *

def lsplan(gnx,args=None):
    plans = []
    Plan = namedtuple("Plan",["planShortID","noOfLibraries","planName"])
    xs=gnx.plans()
    for x in xs:
        plans.append(Plan(x['planShortId'],x['noOfLibraries'],x['planName']))
    return plans

def lsSampleFromPlan(gnx,args=None):
    samples_list = []
    xs=gnx.plans()
    for x in xs:
        print(x.keys())
        for sinfo in x['samples']:
            print(sinfo.keys())
            sample =Sample(planShortId=x['planShortId'],
                           planName=x['planName'],
                           sampleId=sinfo['sampleId']) 
            samples_list.append(sample)
    return samples_list

def updatePlanMatch(plan_id,include_list,match):
    if match is False:
        if plan_id in include_list:
            match = True
    return match

def getSamplesFromPlans(plans,samples):
    return_samples = []
    for sample in samples:
        match = False
        match = updatePlanMatch(sample.planShortId,plans,match)
        match = updatePlanMatch(sample.planName,plans,match)
        if match is True:
            return_samples.append(sample)
    return return_samples

def downloadSampleFilesFromPlan(gnx, plans, extensions=['bai','bam','vcf'],pageSize=600,
                                signedOff=False,return_files_with_ext=True ):
    """
    params:
    gnx:    Genexus Class Object
    plans: Iterable that can be searched with `in`.
    pageSize: number of samples to return with query. This number should be large enough to pull entire catalog of samples.
              It may be optimizable with pagination, though this will require future investigation.
    signedOff: is the param that is past into the Genexus.getJSON() argument.Can be True or False
    extensions: list of extensions to be included or excluded in final download. Typical extensions include:
                    'bam, bai, fastq, txt, log, png, xls, tsv'. 
    return_files_with_ext:  boolean If True only files that have extensions matching an element of the extensions
                                list are returned. If False no with an extension matching the an element of 
                                the extensions list is returned.

    Notes: signedOffSamples returns the base_url for the download. There may be other ways to get this, but I am unaware of them. 
           watch paging when making Genexus.getJson() requests.  
 
    """
    samples = gnx.getJson(f'signedOffSamples', params={'signedOff':signedOff, 'pageSize': pageSize})
    for sample in samples:
        if sample['sample']['planName'] in plans:
            gnx.download(sample=sample['sample'],
                         extensions=extensions,
                         return_files_with_ext=return_files_with_ext)

      
def sampleIdsFromPlans(plan_results, sample_ids=[]):
    """
    params:
    plan_results: Json object returned by Genexus.getJson('plans') or a more specific query e.g.
                  Genexus.getJson(f'plans?planName=FSETraining31Aug21&pageSize=500')

    sample_ids: list to be returned. Sample ids will be appended to list if one is given.
    """
    for result in plan_results[0]['samples']:
        sid = result['sampleId']
        sample_ids.append(sid)
    return sample_ids

def sampleIdsFromResults(results, sample_ids=[]):
    """
    params:
    results: Json object returned by Genexus.getJson('results') or a more specific query e.g.
                  Genexus.getJson(f'results?planName=FSETraining31Aug21&pageSize=500')

    sample_ids: list to be returned. Sample ids will be appended to list if one is given.
    """
    for result in results:
        sample_ids.append(result['sampleId'])
    return sample_ids

def plansTsvReport(plans,outfile,plan_fields=None,sample_fields=None):
    """
    params:
    plans: JSON object returned by Genexus.getJson('plans') or a more specific query e.g.
                  Genexus.getJson(f'plans?planName=FSETraining31Aug21&pageSize=500')
    outfile: file name to write results to
    plan_fields: Fields from the plan section of the JSON to include in the results. Default is All.
                Available Fields:
                 'id', 'planName', 'planShortId', 'type', 
                 'createdBy', 'updatedOn', 'noOfSamples', 
                 'noOfLibraries', 'samples', 'runAssays', 
                 'reportTemplateName'
    sample_fields: Fields from the sample section of the JSON to include in report. Default is All.
                 Available Fields:
                 'id', 'sampleId', 'assayName', 'dnaBarcodes', 
                 'rnaBarcodes', 'libraryBatchId', 'libraryPrepType', 
                 'pauseForReview'

    
    """
    if plan_fields is None:
        plan_fields = ['id', 'planName', 'planShortId', 'type', 
                    'createdBy', 'updatedOn', 'noOfSamples', 
                    'noOfLibraries', 'samples', 'runAssays', 
                    'reportTemplateName']
    if sample_fields is None:
        sample_fields = ['id', 'sampleId', 'assayName', 'dnaBarcodes', 'rnaBarcodes', 'libraryBatchId', 'libraryPrepType', 'pauseForReview']
   # check for type
    assert type(plan_fields) == list, f'plan_fields must be a list'
    assert type(sample_fields) == list, f'plan_fields must be a list'
    sample_index = plan_fields.index('samples')
    plan_fields.pop(sample_index)
    out_outer = plan_fields
    out_inner = sample_fields
    header  = out_outer + out_inner
    out = []
    out.append('\t'.join(header))
    for plan in plans:
        #get outer row
        plan_row = []
        for key in out_outer:
            if key in plan:
                if type(plan[key]) is list:
                    plan_row.append('|'.join(plan[key]))
                else:
                    plan_row.append(str(plan[key]))
            else:
                plan_row.append('.')
        for sample in plan['samples']:
            total_row = plan_row
            for key in out_inner:
                if key in sample:
                    if type(sample[key]) is list:
                        plan_row.append('|'.join(sample[key]))
                    else:
                        plan_row.append(str(sample[key]))
                else:
                    total_row.append('.')
            out.append('\t'.join(total_row))
    with open(outfile,'w') as f:
        f.write('\n'.join(out))

if __name__ == '__main__':
    main(lsplan)
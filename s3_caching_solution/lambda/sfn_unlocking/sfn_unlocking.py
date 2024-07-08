import boto3, os, copy
from datetime import datetime
from boto3.dynamodb.conditions import Attr, Key
from aws_lambda_powertools import Logger

logger = Logger(log_uncaught_exceptions=True)

ddb = boto3.resource('dynamodb')
ddb_table = ddb.Table(os.environ['SFN_JOBS_TABLE'])

def ddb_query_concurrent_jobs(pref_lookup, start_time):
    result = ddb_table.query(
        KeyConditionExpression=Key('pk').eq(pref_lookup),
        FilterExpression=Attr('creation').lt(start_time)
    )
    return result

@logger.inject_lambda_context
def lambda_handler(event, context):
    logger.set_correlation_id(event['main_workflow'])
    logger.debug(event)
    sfn_status = copy.deepcopy(event)
    bucket = event['bucket']
    directory_bucket = event['directory_bucket']
    prefixes = event['prefixes']
    start_time = int(datetime.fromisoformat(event['start_time']).timestamp()) + 10 # Adding 10 seconds to be safe and accommodate any race condition
    sfn_status['WaitingForJobs'] = False
    if not 'FailedJobs' in sfn_status:
        sfn_status['FailedJobs'] = []
    
    dict_pref = {}

    for prefix in prefixes:
        full_prefix = f'{bucket}/{directory_bucket}/{prefix}'

        # First Check if other job is working on it.
        # This is checking if someone is working on the same prefix or any higher level prefix that includes the one requested here.
        pref_list = full_prefix.split('/')
        status_list = []
        dict_pref[prefix] = []

        for x in range(3, len(pref_list)):
            pref_lookup = '/'.join(pref_list[:x]) + '/'
            result = ddb_query_concurrent_jobs(pref_lookup, start_time)
            
            if 'Items' in result:
                sorted_list = sorted(result['Items'], key=lambda x: x['creation'], reverse=True)
                for x in sorted_list:
                    dict_pref[prefix].append(x)
                    logger.debug(f'Working on DDB Item: {x}')

                    if 'jobStatus' in x:
                        if x['jobStatus'] in ['Failed', 'Cancelled'] or (x['jobStatus'] == 'Complete' and x['numberOfTasksFailed'] > 0):
                            status_list.append('Failed') 
                        elif x['jobStatus'] == 'Complete':
                            status_list.append('Complete') 
                    else:
                        status_list.append('WaitingForJobs')
                        sfn_status['WaitingForJobs'] = True
                    if x['forceCopy']:
                        break
        logger.debug(f'Status List: {status_list}')
        if not 'WaitingForJobs' in status_list:
            if 'Failed' in status_list:
                for job in dict_pref[prefix]:
                    if job['jobStatus'] in ['Failed', 'Cancelled'] or (job['jobStatus'] == 'Complete' and job['numberOfTasksFailed'] > 0):
                        sfn_status['FailedJobs'].append({'prefix': prefix, 'numberOfTasksFailed': job['numberOfTasksFailed'], 'jobReport': job['jobReport'], 'sk': job['sk']})
            
            sfn_status['prefixes'].remove(prefix)

    return sfn_status
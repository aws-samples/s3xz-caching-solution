import boto3, os
from aws_lambda_powertools import Logger

logger = Logger(log_uncaught_exceptions=True)
ddb = boto3.resource('dynamodb')
ddb_table = ddb.Table(os.environ['SFN_JOBS_TABLE'])
working_bucket = os.environ['WORKING_BUCKET']

def ddb_update_own_record(full_prefix, execution_job_id, status, report, number_of_tasks_failed, deletion_update):
    result = ddb_table.update_item(
        Key={
            'pk': full_prefix,
            'sk': execution_job_id
        },
        UpdateExpression='SET #col1=:j, #col2=:r, #col3=:n, deletion = deletion + :d',
        ExpressionAttributeValues={':j': status, ':r': report, ':n': number_of_tasks_failed, ':d': deletion_update},
        ExpressionAttributeNames={'#col1': 'jobStatus', '#col2': 'jobReport', '#col3': 'numberOfTasksFailed'}
    )
    return result

@logger.inject_lambda_context
def lambda_handler(event, context):
    logger.set_correlation_id(event['main_workflow'])
    logger.debug(event)
    execution_job_id = event['workflow']
    bucket = event['bucket']
    prefix = event['prefix']
    directory_bucket = event['directory_bucket']
    full_prefix = f'{bucket}/{directory_bucket}/{prefix}'

    if 'Status' in event['payload']:
        if event['payload']['Status'] in ['Failed', 'Cancelled']:
            ddb_update_own_record(full_prefix, execution_job_id, 'Failed', event['payload']['report_location'], event['payload']['NumberOfTasksFailed'], 7200) # increasing TTL of the record, so other runs can discover that something failed in the past.
        elif event['payload']['Status'] == 'Complete' and event['payload']['NumberOfTasksFailed'] > 0:
            ddb_update_own_record(full_prefix, execution_job_id, 'PartialFail', event['payload']['report_location'], event['payload']['NumberOfTasksFailed'], 7200) # increasing TTL of the record, so other runs can discover that something failed in the past.
        else:
            ddb_update_own_record(full_prefix, execution_job_id, 'Complete', '', event['payload']['NumberOfTasksFailed'], 0)
        
        return event['payload']
    else:
        ddb_update_own_record(full_prefix, execution_job_id, 'Complete', '', 0, 0)
        return {
            'Status': 'Complete'
        }
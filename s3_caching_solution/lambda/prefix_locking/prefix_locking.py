import boto3, os, datetime, time
from aws_lambda_powertools import Logger

logger = Logger(log_uncaught_exceptions=True)
ddb = boto3.resource('dynamodb')
ddb_table = ddb.Table(os.environ['SFN_JOBS_TABLE'])
    
def ddb_update_prefix(full_prefix, sfn_job_id, datetime_now, expiryDateTime, force_copy, main_workflow):
    result = ddb_table.update_item(
        Key={
            'pk': full_prefix,
            'sk': sfn_job_id
        },
        UpdateExpression='set #col1=:j, #col2=:t, #col3=:f, #col4=:m',
        ExpressionAttributeValues={':j': datetime_now, ':t': expiryDateTime, ':f': force_copy, ':m': main_workflow},
        ExpressionAttributeNames={'#col1': 'creation', '#col2': 'deletion', '#col3': 'forceCopy', '#col4': 'mainWorkflow'}
    )
    return result

@logger.inject_lambda_context
def lambda_handler(event, context):
    logger.set_correlation_id(event['main_workflow'])
    logger.debug(event)
    
    bucket = event['bucket']
    sfn_job_id = event['workflow']
    directory_bucket = event['directory_bucket']
    prefix = event['prefix']
    force_copy = event['force_copy']
    main_workflow = event['main_workflow']
    ttl = event['ttl']
    datetime_now = datetime.datetime.now()
    ttl_future = datetime_now + datetime.timedelta(hours=ttl)
    expiryDateTime = int(time.mktime(ttl_future.timetuple()))

    full_prefix = f'{bucket}/{directory_bucket}/{prefix}'

    ddb_update_prefix(full_prefix, sfn_job_id, int(time.mktime(datetime_now.timetuple())), expiryDateTime, force_copy, main_workflow)
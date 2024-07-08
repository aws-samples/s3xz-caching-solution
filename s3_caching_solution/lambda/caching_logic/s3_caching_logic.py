import boto3, datetime, time, botocore, os, uuid, json
from botocore.client import Config
from aws_lambda_powertools import Logger

logger = Logger(log_uncaught_exceptions=True)

config = Config(
   retries = {
      'max_attempts': 1,
      'mode': 'standard'
   },
   connect_timeout=90,
   tcp_keepalive=True
)

ddb = boto3.resource('dynamodb', config=config)
ddb_table = ddb.Table(os.environ['CACHE_TABLE'])
s3 = boto3.resource('s3')
working_bucket = s3.Bucket(os.environ['WORKING_BUCKET'])

def ddb_update(bucket, prefix, expiryDateTime):
    result = ddb_table.update_item(
        Key={
            'bucket': bucket,
            'object': prefix
        },
        UpdateExpression='set deletion=:t',
        ConditionExpression='deletion < :t or attribute_not_exists(deletion)',
        ExpressionAttributeValues={':t': expiryDateTime},
        ReturnValues="UPDATED_OLD"
    )
    return result

@logger.inject_lambda_context
def lambda_handler(event, context):
    logger.set_correlation_id(event['BatchInput']['main_workflow'])
    logger.debug(event)

    bucket = event['BatchInput']['bucket']
    ttl = event['BatchInput']['ttl']
    ttl_future = datetime.datetime.now() + datetime.timedelta(hours=ttl)
    expiryDateTime = int(time.mktime(ttl_future.timetuple()))
    run_id = event['BatchInput']['workflow'].split(':')[-1]
    directory_bucket = event['BatchInput']['directory_bucket']

    obj_list = []
    failed_obj_list = []
    results = {}
    results['succeed'] = []
    int_obj_name = uuid.uuid4()
    
    # Check if this is an original request or a retry for failed objects:
    if 'Items' in event.keys():
        # This is an original request, so we have a plain list of objects
        prefixes = event['Items']
    else:
        # This is a retry, so we have get S3 objects and build a list of objects to reprocess
        for succeed in event['succeed']:
            results['succeed'].append(succeed)

        reprocess_count = 0
        prefixes = []
        failed_objects = working_bucket.Object(event['failed']).get()['Body'].read().decode("utf-8")[1:-1].split(', ')
        
        for line in failed_objects:
            prefixes.append({"object": line.replace('"','').replace(' ','')})
            reprocess_count += 1
    
    
    for prefix in prefixes:

        try:
            
            object_name = f'{directory_bucket}/{prefix["object"]}'
            result = ddb_update(bucket, object_name, expiryDateTime)

            if 'Attributes' not in result.keys() or event['BatchInput']['force_copy']:
                obj_list.append(f'{bucket},{prefix["object"]}')

        except botocore.exceptions.ClientError as err:
            if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
                # The object is already cached with a longer TTL, nothing to do.
                pass
            elif err.response["Error"]["Code"] in ["ThrottlingException","ProvisionedThroughputExceededException"]:
                # Persist object key in list for future execution
                failed_obj_list.append(prefix["object"])
            else:
                # Will still try to persist object key in list for further execution, but raise a CW log
                failed_obj_list.append(prefix["object"])
                logger.info(f'ERROR: we will retry, but something is wrong with bucket: {bucket} object: {object_name} ttl: {expiryDateTime} {err.response["Error"]}')
    
    if failed_obj_list:
        s3_object_failed = working_bucket.put_object(
            Key=f'{bucket}/{run_id}/failed/{int_obj_name}',
            Body=bytes(json.dumps(failed_obj_list).encode('UTF-8')),
        )
        results['failed'] = s3_object_failed.key
        
    if obj_list:
        s3_object = working_bucket.put_object(
            Key=f'{bucket}/{run_id}/succeed/{int_obj_name}',
            Body=bytes(json.dumps(obj_list).encode('UTF-8')),
        )
        results['succeed'].append(s3_object.key)

    results['BatchInput'] = event['BatchInput']

    logger.info(f'Finished processing a total of {len(prefixes) + len(obj_list) + len(failed_obj_list)} with the following results: ')
    logger.info(f'Cache miss: {len(obj_list)} objects were processed successfully and will be added to the copy manifest')
    logger.info(f'Cache hit: {len(prefixes) - len(obj_list) - len(failed_obj_list)} objects were processed successfully but copy is not needed')
    logger.info(f'Reprocess needed: {len(failed_obj_list)} objects failed to be processed and will be added to the retry strategy')
        
    if 'succeed' in results or 'failed' in results:
        return results
import boto3, datetime, time, botocore, os, uuid, json
from botocore.client import Config

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


def lambda_handler(event, context):

    bucket = event['BatchInput']['bucket']
    ttl = event['BatchInput']['ttl']
    ttl_future = datetime.datetime.now() + datetime.timedelta(hours=ttl)
    expiryDateTime = int(time.mktime(ttl_future.timetuple()))
    run_id = event['BatchInput']['workflow'].split(':')[-1]
    directory_bucket = event['BatchInput']['directory_bucket']
    prefixes = event['Items']

    obj_list = []
    failed_obj_list = []
    results = {}
    int_obj_name = uuid.uuid4()
    
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
                print(f'ERROR: we will retry, but something is wrong with bucket: {bucket} object: {object_name} ttl: {expiryDateTime} {err.response["Error"]}')
    
    if len(failed_obj_list) > 0:
        s3_object_failed = working_bucket.put_object(
            Key=f'{bucket}/{run_id}/failed/{int_obj_name}',
            Body=bytes(json.dumps(failed_obj_list).encode('UTF-8')),
        )
        results['failed'] = s3_object_failed.key
        
        
    if len(obj_list) > 0:
        s3_object = working_bucket.put_object(
            Key=f'{bucket}/{run_id}/succeed/{int_obj_name}',
            Body=bytes(json.dumps(obj_list).encode('UTF-8')),
        )
        results['succeed'] = s3_object.key

    print(
        f'''Finished processing a total of {len(prefixes) + len(obj_list) + len(failed_obj_list)} with the following results: 
        Cache miss: {len(obj_list)} objects were processed successfully and will be added to the copy manifest
        Cache hit: {len(prefixes) - len(obj_list) - len(failed_obj_list)} objects were processed successfully but copy is not needed
        Reprocess needed: {len(failed_obj_list)} objects failed to be processed and will be added to the retry strategy'''
    )
        
    if 'succeed' in results or 'failed' in results:
        return results
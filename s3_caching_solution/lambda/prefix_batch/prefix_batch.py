import os, boto3, uuid

working_bucket = os.environ['WORKING_BUCKET']
s3_control = boto3.client('s3control')
s3_client = boto3.client('s3')
batch_role = os.environ['BATCH_ROLE']

def lambda_handler(event, context):

    account_id = context.invoked_function_arn.split(":")[4]
    region = context.invoked_function_arn.split(":")[3]
    directory_bucket = event["directory_bucket"]
    consolidation_threshold = int(os.environ['CONSOLIDATION_THRESHOLD'])
    
    f = open('/tmp/manifest.csv', 'w')
    
    workflow_id = event['workflow'].split(':')[-1]
    bucket = event['bucket']
    
    failed_jobs = []
    succeed_jobs = []
    object_count = 0

    for map_result in event['payload']:
        if map_result is not None:

            if 'succeed' in map_result:
                data = s3_client.get_object(Bucket=working_bucket, Key=map_result['succeed'])
                content = data['Body'].read().decode("utf-8")[1:-1].split(", ")
                succeed_jobs.append(map_result['succeed'])
                
                for line in content:
                    f.write(line[1:-1] + '\n')
                    object_count += 1
                
            if 'failed' in map_result:
                failed_jobs.append(map_result['failed'])

    f.close()

    if object_count == 0:

        print(f'No objects to copy. Skipping S3 Batch Job')

        result = {}
        result['account_id'] = account_id
        result['JobId'] = []
        result['failed_jobs'] = failed_jobs
        return result
    
    elif object_count >= consolidation_threshold:

        manifest_id = str(uuid.uuid4())
        upload = s3_client.put_object(Body=open('/tmp/manifest.csv','rb'), Bucket=working_bucket, Key=f'{bucket}/{workflow_id}/{manifest_id}.csv')
        os.remove('/tmp/manifest.csv')
        
        result = {}
        
        manifest_arn = f'arn:aws:s3:::{working_bucket}/{bucket}/{workflow_id}/{manifest_id}.csv'
        
        batch_job = s3_control.create_job(
            AccountId=account_id,
            ClientRequestToken=manifest_id,
            ConfirmationRequired=False,
            # The operation is dropping Tags on Copy, since Directory Buckets doesn't support them. This can be reviewed in the future.
            Operation={
                'S3PutObjectCopy': {
                    'TargetResource': f'arn:aws:s3express:{region}:{account_id}:bucket/{directory_bucket}',
                    'NewObjectTagging': []
                }
            },
            Report={
                'Enabled': True,
                'Bucket': f'arn:aws:s3:::{working_bucket}',
                'Prefix': f'{bucket}/{workflow_id}/batch-reports',
                'ReportScope': 'FailedTasksOnly',
                'Format': 'Report_CSV_20180820'
            },
            Manifest={
                'Spec': {
                    'Format': 'S3BatchOperations_CSV_20180820',
                    'Fields': ['Bucket','Key']
                },
                'Location': {
                    'ObjectArn': manifest_arn,
                    'ETag': upload['ETag']
                }
            },
            Priority=6,
            RoleArn=batch_role
        )
        
        print(f'An S3 Batch Job has been executed with id {batch_job['JobId']}, to copy {object_count} objects')

        result['account_id'] = account_id
        result['JobId'] = [batch_job['JobId']]
        result['failed_jobs'] = failed_jobs
        return result
        
    else:

        print(
            f'''There are {object_count} objects to copy, but we skip the Batch to consolidate in next step and be more cost efficient. 
            You can change this behavior by modifying the CONSOLIDATION_THRESHOLD environment variable for this Lambda Function, currently set at {consolidation_threshold} objects'''
        )

        result = {}
        result['account_id'] = account_id
        result['JobId'] = []
        result['failed_jobs'] = failed_jobs
        result['succeed'] = succeed_jobs
        return result
import boto3, botocore

s3_client = boto3.client('s3')

def lambda_handler(event, context):

    for each in event['Records']:
        ddb_object = each['dynamodb']['Keys']['object']['S']
        key = ddb_object.split('/',1)[1:][0]
        bucket = ddb_object.split('/')[0]

        try:
            s3_client.delete_object(
                Bucket=bucket,
                Key=key
            )
        except botocore.exceptions.ClientError as err:
            # log an error that specific object was not deleted
            print(f'ERROR: Failed to delete object {key} in bucket {bucket}. Error is: {err.response["Error"]}')
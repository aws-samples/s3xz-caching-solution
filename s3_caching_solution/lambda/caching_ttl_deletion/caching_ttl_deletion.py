import boto3, botocore
from aws_lambda_powertools import Logger

from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.dynamo_db_stream_event import (
    DynamoDBRecord,
)

processor = BatchProcessor(event_type=EventType.DynamoDBStreams)
logger = Logger(log_uncaught_exceptions=True)
s3_client = boto3.client('s3')

def delete_object_handler(record: DynamoDBRecord):
    ddb_object = record.dynamodb['Keys']['object']['S']
    key = ddb_object.split('/', 1)[1:][0]
    bucket = ddb_object.split('/')[0]

    try:
        s3_client.delete_object(
            Bucket=bucket,
            Key=key
        )
    except botocore.exceptions.ClientError as err:
        # log an error that specific object was not deleted
        logger.info(f'ERROR: Failed to delete object {key} in bucket {bucket}. Error is: {err.response["Error"]}')

@logger.inject_lambda_context
def lambda_handler(event, context):
    logger.debug(event)

    return process_partial_response(event=event, record_handler=delete_object_handler, processor=processor, context=context)

    # for each in event['Records']:
    #     ddb_object = each['dynamodb']['Keys']['object']['S']
    #     key = ddb_object.split('/',1)[1:][0]
    #     bucket = ddb_object.split('/')[0]

    #     try:
    #         s3_client.delete_object(
    #             Bucket=bucket,
    #             Key=key
    #         )
    #     except botocore.exceptions.ClientError as err:
    #         # log an error that specific object was not deleted
    #         logger.info(f'ERROR: Failed to delete object {key} in bucket {bucket}. Error is: {err.response["Error"]}')
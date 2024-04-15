from os import path
import os, subprocess, sys
from aws_cdk import (
    Duration,
    Stack,
    Size,
    aws_lambda as lambda_,
    aws_lambda_event_sources as eventsources,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_iam as iam,
    aws_logs as logs,
    aws_sns as sns,
)
from cdk_monitoring_constructs import (
    MonitoringFacade,
    AlarmFactoryDefaults,
    SnsAlarmActionStrategy
)
from constructs import Construct

if not os.path.exists('s3_caching_solution/lambda/boto3_lambda_layer'):
    print('Installing some dependencies for Lambda layers')
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-q', 'boto3==1.34.78', '--target', 's3_caching_solution/lambda/boto3_lambda_layer/python/lib/python3.12/site-packages/', '--no-user'])

class S3CachingSolutionStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Monitoring services

        notification_topic = sns.Topic(self, 'NotificationTopic')

        # Grant CloudWatch permissions to publish to the SNS topic
        notification_topic.add_to_resource_policy(
            statement=iam.PolicyStatement(
                actions=['sns:Publish'],
                effect=iam.Effect.ALLOW,
                principals=[iam.ServicePrincipal('cloudwatch.amazonaws.com')],
                resources=[notification_topic.topic_arn],
            )
        )

        monitoring_metrics = MonitoringFacade(
            self, "S3CachingSolutionMonitoringMetrics",
            alarm_factory_defaults=AlarmFactoryDefaults(
                actions_enabled=True,
                alarm_name_prefix='s3-caching',
                action=SnsAlarmActionStrategy(on_alarm_topic=notification_topic),
            )
        )
        monitoring_logs = MonitoringFacade(
            self, "S3CachingSolutionMonitoringLogs",
            alarm_factory_defaults=AlarmFactoryDefaults(
                actions_enabled=True,
                alarm_name_prefix='s3-caching',
                action=SnsAlarmActionStrategy(on_alarm_topic=notification_topic),
            )
        )

        # IAM Role for S3 Batch operations
        batch_role = iam.Role(
            self, "BatchRole",
            assumed_by=iam.ServicePrincipal("batchoperations.s3.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")]
        )

        batch_role.add_to_policy(iam.PolicyStatement(
            actions=[
                's3express:CreateSession'
            ], 
            resources=['*'], 
            effect=iam.Effect.ALLOW)
        )

        # Create a DynamoDB caching table, on-demand and with TTL
        cache_table = dynamodb.TableV2(
            self, "CacheTable",
            sort_key=dynamodb.Attribute(name="bucket", type=dynamodb.AttributeType.STRING),
            partition_key=dynamodb.Attribute(name="object", type=dynamodb.AttributeType.STRING),
            dynamo_stream=dynamodb.StreamViewType.KEYS_ONLY,
            time_to_live_attribute="deletion"
        )
        monitoring_metrics.monitor_dynamo_table(table=cache_table)

        # Create a S3 bucket as a working place for temporary objects with lifecycle
        working_bucket = s3.Bucket(
            self, "WorkingBucket",
            lifecycle_rules=[s3.LifecycleRule(expiration=Duration.days(1))]
        )
        monitoring_metrics.monitor_s3_bucket(bucket=working_bucket)

        # Create a lambda function to manage the cache strategy
        cache_lambda = lambda_.Function(
            self, "CacheLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.AssetCode.from_asset(path.join(os.getcwd(), 's3_caching_solution/lambda/caching_logic')),
            handler="s3_caching_logic.lambda_handler",
            environment={
                "CACHE_TABLE": cache_table.table_name,
                "WORKING_BUCKET": working_bucket.bucket_name
            },
            timeout=Duration.seconds(900),
            memory_size=320,
            architecture=lambda_.Architecture.ARM_64,
            log_retention=logs.RetentionDays.THREE_MONTHS
        )
        monitoring_metrics.monitor_lambda_function(
            lambda_function=cache_lambda,
            add_fault_rate_alarm={'Warning': {'maxErrorRate': 1}},
            add_throttles_rate_alarm={'Warning': {'maxErrorRate': 1}}
        )
        monitoring_logs.monitor_log(
            log_group_name=cache_lambda.log_group.log_group_name,
            human_readable_name='Error logs',
            pattern='ERROR',
            alarm_friendly_name='error logs',
        )

        cache_reprocess_lambda = lambda_.Function(
            self, "CacheReprocessLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.AssetCode.from_asset(path.join(os.getcwd(), 's3_caching_solution/lambda/caching_logic_reprocess')),
            handler="s3_caching_logic_reprocess.lambda_handler",
            environment={
                "CACHE_TABLE": cache_table.table_name,
                "WORKING_BUCKET": working_bucket.bucket_name
            },
            timeout=Duration.seconds(900),
            memory_size=320,
            architecture=lambda_.Architecture.ARM_64,
            log_retention=logs.RetentionDays.THREE_MONTHS
        )
        monitoring_metrics.monitor_lambda_function(
            lambda_function=cache_reprocess_lambda,
            add_fault_rate_alarm={'Warning': {'maxErrorRate': 1}},
            add_throttles_rate_alarm={'Warning': {'maxErrorRate': 1}}
        )
        monitoring_logs.monitor_log(
            log_group_name=cache_reprocess_lambda.log_group.log_group_name,
            human_readable_name='Error logs',
            pattern='ERROR',
            alarm_friendly_name='error logs',
        )

        prefix_batch_lambda = lambda_.Function(
            self, "PrefixBatchLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.AssetCode.from_asset(path.join(os.getcwd(), 's3_caching_solution/lambda/prefix_batch')),
            handler="prefix_batch.lambda_handler",
            environment={
                "WORKING_BUCKET": working_bucket.bucket_name,
                "BATCH_ROLE": batch_role.role_arn,
                "CONSOLIDATION_THRESHOLD": "10000"
            },
            timeout=Duration.seconds(900),
            memory_size=512,
            architecture=lambda_.Architecture.ARM_64,
            ephemeral_storage_size=Size.mebibytes(5120),
            log_retention=logs.RetentionDays.THREE_MONTHS
        )
        monitoring_metrics.monitor_lambda_function(
            lambda_function=prefix_batch_lambda,
            add_fault_rate_alarm={'Warning': {'maxErrorRate': 1}},
            add_throttles_rate_alarm={'Warning': {'maxErrorRate': 1}}
        )
        monitoring_logs.monitor_log(
            log_group_name=prefix_batch_lambda.log_group.log_group_name,
            human_readable_name='Error logs',
            pattern='ERROR',
            alarm_friendly_name='error logs',
        )

        # Create a lambda function to consolidate data and run S3 Batch
        consolidation_batch_lambda = lambda_.Function(
            self, "ConsolidationBatchLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.AssetCode.from_asset(path.join(os.getcwd(), 's3_caching_solution/lambda/consolidation_batch')),
            handler="consolidation_batch.lambda_handler",
            environment={
                "WORKING_BUCKET": working_bucket.bucket_name,
                "BATCH_ROLE": batch_role.role_arn
            },
            timeout=Duration.seconds(900),
            memory_size=512,
            architecture=lambda_.Architecture.ARM_64,
            ephemeral_storage_size=Size.mebibytes(5120),
            log_retention=logs.RetentionDays.THREE_MONTHS
        )
        monitoring_metrics.monitor_lambda_function(
            lambda_function=consolidation_batch_lambda,
            add_fault_rate_alarm={'Warning': {'maxErrorRate': 1}},
            add_throttles_rate_alarm={'Warning': {'maxErrorRate': 1}}
        )
        monitoring_logs.monitor_log(
            log_group_name=consolidation_batch_lambda.log_group.log_group_name,
            human_readable_name='Error logs',
            pattern='ERROR',
            alarm_friendly_name='error logs',
        )

        # Create a lambda function to delete old objects from S3 Express One Zone
        boto3_lambda_layer = lambda_.LayerVersion(
            self, 'Boto3LambdaLayer',
            code=lambda_.AssetCode.from_asset(path.join(os.getcwd(), 's3_caching_solution/lambda/boto3_lambda_layer/')),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description='Boto3 Library version 1.34.78 for Python 3.12',
            layer_version_name='python3-12-boto3-v1-34-78'
        )
        
        caching_ttl_deletion = lambda_.Function(
            self, "CachingTtlDeletion",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.AssetCode.from_asset(path.join(os.getcwd(), 's3_caching_solution/lambda/caching_ttl_deletion')),
            handler="caching_ttl_deletion.lambda_handler",
            timeout=Duration.seconds(900),
            memory_size=384,
            architecture=lambda_.Architecture.ARM_64,
            layers=[boto3_lambda_layer],
            log_retention=logs.RetentionDays.THREE_MONTHS
        )
        monitoring_metrics.monitor_lambda_function(
            lambda_function=caching_ttl_deletion,
            add_fault_rate_alarm={'Warning': {'maxErrorRate': 1}},
            add_throttles_rate_alarm={'Warning': {'maxErrorRate': 1}}
        )
        monitoring_logs.monitor_log(
            log_group_name=caching_ttl_deletion.log_group.log_group_name,
            human_readable_name='Error logs',
            pattern='ERROR',
            alarm_friendly_name='error logs',
        )

        caching_ttl_deletion.add_event_source(
            eventsources.DynamoEventSource(
                cache_table,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=2000,
                max_batching_window=Duration.seconds(60),
                parallelization_factor=10,
                filters=[
                    lambda_.FilterCriteria.filter(
                        {
                            "userIdentity": {
                                "type": lambda_.FilterRule.is_equal("Service"),
                                "principalId": lambda_.FilterRule.is_equal("dynamodb.amazonaws.com")
                            }
                        }
                    )
                ]
            )
        )

        # Assign permissions to the lambda functions to access the cache table and working bucket
        cache_table.grant_read_write_data(cache_lambda)
        working_bucket.grant_read_write(cache_lambda)
        cache_table.grant_read_write_data(cache_reprocess_lambda)
        working_bucket.grant_read_write(cache_reprocess_lambda)
        working_bucket.grant_read_write(consolidation_batch_lambda)
        working_bucket.grant_read_write(prefix_batch_lambda)
        consolidation_batch_lambda.add_to_role_policy(iam.PolicyStatement(
            actions=[
                's3:CreateJob'
            ], 
            resources=['*'], 
            effect=iam.Effect.ALLOW)
        )
        consolidation_batch_lambda.add_to_role_policy(iam.PolicyStatement(
            actions=[
                'iam:GetRole',
                'iam:PassRole'
            ], 
            resources=[batch_role.role_arn], 
            effect=iam.Effect.ALLOW)
        )
        prefix_batch_lambda.add_to_role_policy(iam.PolicyStatement(
            actions=[
                's3:CreateJob'
            ], 
            resources=['*'], 
            effect=iam.Effect.ALLOW)
        )
        prefix_batch_lambda.add_to_role_policy(iam.PolicyStatement(
            actions=[
                'iam:GetRole',
                'iam:PassRole'
            ], 
            resources=[batch_role.role_arn], 
            effect=iam.Effect.ALLOW)
        )
        caching_ttl_deletion.add_to_role_policy(iam.PolicyStatement(
            actions=[
                's3express:CreateSession'
            ], 
            resources=['*'], 
            effect=iam.Effect.ALLOW)
        )
        caching_ttl_deletion.add_to_role_policy(iam.PolicyStatement(
            actions=[
                's3:DeleteObject'
            ], 
            resources=[f'arn:aws:s3express:{self.region}:{self.account}:bucket/*'], 
            effect=iam.Effect.ALLOW)
        )

        # Building Step function

        task_prefix_batch_lambda = sfn_tasks.LambdaInvoke(
            self, "TaskPrefixBatchLambda",
            lambda_function=prefix_batch_lambda,
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            output_path='$.Payload',
            payload=sfn.TaskInput.from_object({
                "bucket.$": "$$.Execution.Input.bucket",
                "directory_bucket.$": "$$.Execution.Input.directory_bucket",
                "workflow.$": "$$.Execution.Id",
                "payload.$": "$"
            })
        )

        task_consolidation_batch_lambda = sfn_tasks.LambdaInvoke(
            self, "TaskConsolidationBatchLambda",
            lambda_function=consolidation_batch_lambda,
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            output_path='$.Payload'
        )

        # Temp: to be implemented when Distributed Map add support to dynamic parameters
        # https://github.com/aws/aws-cdk/issues/29136
        # task_cache_lambda = sfn_tasks.LambdaInvoke(
        #     self, "TaskCacheLambda",
        #     lambda_function=cache_lambda,
        #     integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE
        # )

        task_cache_reprocess_lambda = sfn_tasks.LambdaInvoke(
            self, "TaskCacheReprocessLambda",
            lambda_function=cache_reprocess_lambda,
            output_path='$.Payload',
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE
        )

        prepare_redrive = sfn.Pass(
            self, "PrepareRedrive",
            parameters={
                "bucket.$": "$$.Execution.Input.bucket",
                "directory_bucket.$": "$$.Execution.Input.directory_bucket",
                "workflow.$": "$$.Execution.Id",
                "failed_jobs.$": "$.redrive.*.failed",
                "succeed.$": "$.redrive.*.succeed",
                "jobIds.$": "$.batch.jobIds"
            }
        )

        # Temporary custom state until full support for distributed maps is added
        # https://github.com/aws/aws-cdk/issues/29136

        s3_list_map = sfn.CustomState(
            self, "S3ListMap",
            state_json={
                "Type": "Map",
                "ItemProcessor": {
                    "ProcessorConfig": {
                        "Mode": "DISTRIBUTED",
                        "ExecutionType": "STANDARD"
                    },
                    "StartAt": "Caching-Strategy",
                    "States": {
                        "Caching-Strategy": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::lambda:invoke",
                            "OutputPath": "$.Payload",
                            "Parameters": {
                                "Payload.$": "$",
                                "FunctionName": cache_lambda.function_arn
                            },
                            "Retry": [
                                {
                                    "ErrorEquals": [
                                        "Lambda.ServiceException",
                                        "Lambda.AWSLambdaException",
                                        "Lambda.SdkClientException",
                                        "Lambda.TooManyRequestsException"
                                    ],
                                    "IntervalSeconds": 1,
                                    "MaxAttempts": 3,
                                    "BackoffRate": 2
                                }
                            ],
                            "End": True
                        }
                    }
                },
                "MaxConcurrency": 40,
                "ItemBatcher": {
                    "MaxItemsPerBatch": 4000,
                    "MaxInputBytesPerBatch": 245760,
                    "BatchInput": {
                        "bucket.$": "$.bucket",
                        "ttl.$": "$.ttl",
                        "workflow.$": "$$.Execution.Id",
                        "directory_bucket.$": "$$.Execution.Input.directory_bucket",
                        "force_copy.$": "$$.Execution.Input.force_copy"
                    }
                },
                "ItemReader": {
                    "Resource": "arn:aws:states:::s3:listObjectsV2",
                    "Parameters": {
                        "Bucket.$": "$.bucket",
                        "Prefix.$": "$.prefix"
                    }
                },
                "ItemSelector": {
                    "object.$": "$$.Map.Item.Value.Key"
                }
            }
        )

        s3_list_map.add_retry(
            backoff_rate=2,
            interval=Duration.seconds(1),
            max_attempts=3,
            errors=["States.ALL"],
            jitter_strategy=sfn.JitterType.FULL
        )

        map_state = sfn.Map(
            self, "MapPerPrefix",
            items_path="$.prefixes",
            item_selector={
                "ttl.$": "$.ttl",
                "bucket.$": "$.bucket",
                "prefix.$": "$$.Map.Item.Value"
            },
            result_selector={
                "workflow.$": "$$.Execution.Id",
                "bucket.$": "$$.Execution.Input.bucket",
                "directory_bucket.$": "$$.Execution.Input.directory_bucket",
                "jobIds.$": "$.[*]JobId[*]",
                "failed_jobs.$": "$.[*]failed_jobs[*]",
                "succeed.$": "$.[*]succeed[*]"
            }
        ).item_processor(s3_list_map.next(task_prefix_batch_lambda))

        # Distributed MAP for reprocessing Caching throttling 

        s3_cache_reprocess_map = sfn.DistributedMap(
            self, 's3CacheReprocessMap',
            item_batcher=sfn.ItemBatcher(
                batch_input={
                    "bucket.$": "$$.Execution.Input.bucket",
                    "ttl.$": "$$.Execution.Input.ttl",
                    "workflow.$": "$$.Execution.Id",
                    "directory_bucket.$": "$$.Execution.Input.directory_bucket",
                    "force_copy.$": "$$.Execution.Input.force_copy"
                },
                max_items_per_batch=10
            ),
            max_concurrency=500,
            items_path="$.failed_jobs"
        )

        s3_cache_reprocess_map.item_processor(task_cache_reprocess_lambda)

        parallel_processing = sfn.Parallel(
            self, 'ParallelProcessing',
            result_selector={
                "batch.$": "States.ArrayGetItem($,0)",
                "redrive.$": "States.ArrayGetItem($,1)"
            }
        ).branch(
            task_consolidation_batch_lambda
        ).branch(
            s3_cache_reprocess_map
        )

        s3_batch_job_describe_map = sfn.Map(
            self, 'S3BatchJobDescribeMap',
            items_path="$.batch.jobIds",
            item_selector={
                "AccountId.$": "$.batch.account_id",
                "JobId.$": "$$.Map.Item.Value"
            },
            result_selector={"FailedJobs.$": "$.[?(@.Status!='Complete')]"}
        )

        # Logic to wait for the S3 Batch job to finish. This could be improved in the future if we release an
        # optimized integration with S3Control that allows to call the job with .sync according to: 
        # https://docs.aws.amazon.com/step-functions/latest/dg/connect-to-resource.html#connect-sync

        job_success = sfn.Succeed(self, 'JobSuccess')
        job_fail = sfn.Pass(
            self, "JobFail",
            parameters={
                "Status": "JobFail",
                "JobId.$": "$.JobId",
                "Cause": "The Job failed to complete. Please check the Job report provided.",
                "ReportLocation.$": "$.ReportLocation"
            }
        )
        job_partial_fail = sfn.Pass(
            self, "JobPartialFail",
            parameters={
                "Status": "PartialFailure",
                "JobId.$": "$.JobId",
                "Cause": "Some of the objects failed to complete. This might be related to unsupported functionalities or an underlying issue. Please check the Job report provided.",
                "ReportLocation.$": "$.ReportLocation"
            }
        )

        workflow_success = sfn.Succeed(self, 'WorkflowSuccess')
        workflow_fail = sfn.Fail(
            self, 'WorkflowFail',
            error='Some of the Jobs failed',
            cause_path=sfn.JsonPath.string_at('States.JsonToString($.FailedJobs)')
        )

        wait_batch = sfn.Wait(
            self, 'WaitBatch',
            time=sfn.WaitTime.duration(Duration.seconds(30))
        )

        describe_job = sfn_tasks.CallAwsService(
            self, "DescribeJob",
            service="s3control",
            action="describeJob",
            parameters={
                "AccountId.$": "$.AccountId",
                "JobId.$": "$.JobId"
            },
            result_selector= {
                "JobId.$": "$.Job.JobId",
                "AccountId.$": "States.ArrayGetItem(States.StringSplit($.Job.JobArn, ':'), 4)",
                "Status.$": "$.Job.Status",
                "NumberOfTasksFailed.$": "$.Job.ProgressSummary.NumberOfTasksFailed",
                "ReportLocation.$": "States.Format('{}/{}/job-{}', States.ArrayGetItem(States.StringSplit($.Job.Report.Bucket, ':'), 3), $.Job.Report.Prefix, $.Job.JobId)"
            },
            iam_resources=["*"]
        )
        monitoring_metrics.monitor_step_function_service_integration(
            service_integration_resource_arn=describe_job.id,
            alarm_friendly_name='Describe-Batch-Job',
            add_failed_service_integrations_rate_alarm={'Warning': {'maxErrorRate': 1}},
            add_timed_out_service_integrations_count_alarm={'Warning': {'maxErrorCount': 1}}
        )

        describe_job.add_retry(
            backoff_rate=2,
            interval=Duration.seconds(1),
            max_attempts=3,
            errors=["States.ALL"],
            jitter_strategy=sfn.JitterType.FULL
        )

        choice_batch = sfn.Choice(self, 'ChoiceBatch')
        condition_batch_redrive = sfn.Condition.is_present('$.redrive[0]')
        condition_batch_without_job = sfn.Condition.is_not_present('$.batch.jobIds[0]')

        choice_ready = sfn.Choice(self, 'ChoiceReady')
        condition_ready_failed = sfn.Condition.or_(
            sfn.Condition.string_equals('$.Status', 'Failed'),
            sfn.Condition.string_equals('$.Status', 'Cancelled')
        )
        
        condition_ready_completed = sfn.Condition.string_equals('$.Status', 'Complete')

        condition_ready_partial_fail = sfn.Condition.and_(
            sfn.Condition.number_greater_than('$.NumberOfTasksFailed', 0),
            sfn.Condition.or_(
                sfn.Condition.string_equals('$.Status', 'Completing'),
                sfn.Condition.string_equals('$.Status', 'Complete')
            )
        )

        choice_ready.when(condition_ready_failed, job_fail).when(condition_ready_completed, job_success).when(condition_ready_partial_fail, job_partial_fail).otherwise(wait_batch)

        choice_workflow = sfn.Choice(self, 'ChoiceWorkflow')
        condition_workflow_failed = sfn.Condition.is_present('$.FailedJobs[0]')
        choice_workflow.when(condition_workflow_failed, workflow_fail).otherwise(workflow_success)

        s3_batch_job_chain = wait_batch.next(describe_job).next(choice_ready)

        s3_batch_job_describe_map.item_processor(s3_batch_job_chain)

        choice_batch.when(
            condition_batch_redrive, prepare_redrive.next(parallel_processing)
        ).when(
            condition_batch_without_job, workflow_success
        ).otherwise(
            s3_batch_job_describe_map.next(choice_workflow)
        )

        # End of Wait Logic

        chain_definition = map_state.next(parallel_processing).next(choice_batch)

        state_machine = sfn.StateMachine(
            self, "S3CachingSFN",
            definition_body=sfn.DefinitionBody.from_chainable(chain_definition)
        )
        monitoring_metrics.monitor_step_function(
            state_machine=state_machine,
            add_failed_execution_rate_alarm={'Warning': {'maxErrorRate': 1}},
        )

        state_machine.add_to_role_policy(iam.PolicyStatement(
            actions=[
                's3:List*',
                's3:DescribeJob',
                'states:StartExecution',
                'states:DescribeExecution',
                'states:StopExecution',
                'states:RedriveExecution'
            ], 
            resources=['*'], 
            effect=iam.Effect.ALLOW)
        )

        # Temporary permission until Distributed Map is implemented in full
        state_machine.add_to_role_policy(iam.PolicyStatement(
            actions=[
                'lambda:InvokeFunction'
            ], 
            resources=[cache_lambda.function_arn], 
            effect=iam.Effect.ALLOW)
        )
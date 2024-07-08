from os import path
import os, subprocess, sys
from aws_cdk import (
    Duration,
    Stack,
    Size,
    RemovalPolicy,
    aws_lambda as lambda_,
    aws_lambda_event_sources as eventsources,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_iam as iam,
    aws_logs as logs,
    aws_sns as sns
)
from cdk_monitoring_constructs import (
    MonitoringFacade,
    AlarmFactoryDefaults,
    SnsAlarmActionStrategy
)
from constructs import Construct

if not os.path.exists('s3_caching_solution/lambda/boto3_lambda_layer'):
    print('Installing some dependencies for Lambda layers')
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-q', 'boto3==1.34.134', '--target', 's3_caching_solution/lambda/boto3_lambda_layer/python/lib/python3.12/site-packages/', '--no-user'])

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
            time_to_live_attribute="deletion",
            removal_policy=RemovalPolicy.DESTROY
        )
        monitoring_metrics.monitor_dynamo_table(table=cache_table)

        # Create a DynamoDB table to store the state machine execution jobs for consistency locking
        sfn_jobs_table = dynamodb.TableV2(
            self, "SfnJobsTable",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            time_to_live_attribute="deletion",
            removal_policy=RemovalPolicy.DESTROY
        )
        monitoring_metrics.monitor_dynamo_table(table=sfn_jobs_table)

        # Create a S3 bucket as a working place for temporary objects with lifecycle
        working_bucket = s3.Bucket(
            self, "WorkingBucket",
            lifecycle_rules=[s3.LifecycleRule(expiration=Duration.days(1))],
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            enforce_ssl=True
        )
        monitoring_metrics.monitor_s3_bucket(bucket=working_bucket)

        # Create a lambda function to manage the cache strategy

        powertools_service_name = "s3xz-caching"
        powertools_log_level = "INFO"

        powertools_layer = lambda_.LayerVersion.from_layer_version_arn(
            self,
            id="lambda-powertools",
            layer_version_arn=f"arn:aws:lambda:{self.region}:017000801446:layer:AWSLambdaPowertoolsPythonV2-Arm64:75",
        )

        cache_lambda = lambda_.Function(
            self, "CacheLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.AssetCode.from_asset(path.join(os.getcwd(), 's3_caching_solution/lambda/caching_logic')),
            handler="s3_caching_logic.lambda_handler",
            environment={
                "CACHE_TABLE": cache_table.table_name,
                "WORKING_BUCKET": working_bucket.bucket_name,
                "POWERTOOLS_SERVICE_NAME": powertools_service_name,
                "POWERTOOLS_LOG_LEVEL": powertools_log_level
            },
            timeout=Duration.seconds(900),
            memory_size=384,
            architecture=lambda_.Architecture.ARM_64,
            log_retention=logs.RetentionDays.THREE_MONTHS,
            layers=[powertools_layer]
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

        prefix_batch_lambda = lambda_.Function(
            self, "PrefixBatchLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.AssetCode.from_asset(path.join(os.getcwd(), 's3_caching_solution/lambda/prefix_batch')),
            handler="prefix_batch.lambda_handler",
            environment={
                "WORKING_BUCKET": working_bucket.bucket_name,
                "BATCH_ROLE": batch_role.role_arn,
                "POWERTOOLS_SERVICE_NAME": powertools_service_name,
                "POWERTOOLS_LOG_LEVEL": powertools_log_level
            },
            timeout=Duration.seconds(900),
            memory_size=512,
            architecture=lambda_.Architecture.ARM_64,
            ephemeral_storage_size=Size.mebibytes(5120),
            log_retention=logs.RetentionDays.THREE_MONTHS,
            layers=[powertools_layer]
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

        # Create lambda functions to manage the state machine Locking tasks
        prefix_locking_lambda = lambda_.Function(
            self, "PrefixLockingLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.AssetCode.from_asset(path.join(os.getcwd(), 's3_caching_solution/lambda/prefix_locking')),
            handler="prefix_locking.lambda_handler",
            environment={
                "SFN_JOBS_TABLE": sfn_jobs_table.table_name,
                "POWERTOOLS_SERVICE_NAME": powertools_service_name,
                "POWERTOOLS_LOG_LEVEL": powertools_log_level
            },
            timeout=Duration.seconds(900),
            memory_size=320,
            architecture=lambda_.Architecture.ARM_64,
            log_retention=logs.RetentionDays.THREE_MONTHS,
            layers=[powertools_layer]
        )
        monitoring_metrics.monitor_lambda_function(
            lambda_function=prefix_locking_lambda,
            add_fault_rate_alarm={'Warning': {'maxErrorRate': 1}},
            add_throttles_rate_alarm={'Warning': {'maxErrorRate': 1}}
        )
        monitoring_logs.monitor_log(
            log_group_name=prefix_locking_lambda.log_group.log_group_name,
            human_readable_name='Error logs',
            pattern='ERROR',
            alarm_friendly_name='error logs',
        )

        prefix_unlocking_lambda = lambda_.Function(
            self, "PrefixUnlockingLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.AssetCode.from_asset(path.join(os.getcwd(), 's3_caching_solution/lambda/prefix_unlocking')),
            handler="prefix_unlocking.lambda_handler",
            environment={
                "SFN_JOBS_TABLE": sfn_jobs_table.table_name,
                "WORKING_BUCKET": working_bucket.bucket_name,
                "POWERTOOLS_SERVICE_NAME": powertools_service_name,
                "POWERTOOLS_LOG_LEVEL": powertools_log_level
            },
            timeout=Duration.seconds(900),
            memory_size=128,
            architecture=lambda_.Architecture.ARM_64,
            log_retention=logs.RetentionDays.THREE_MONTHS,
            layers=[powertools_layer]
        )
        monitoring_metrics.monitor_lambda_function(
            lambda_function=prefix_unlocking_lambda,
            add_fault_rate_alarm={'Warning': {'maxErrorRate': 1}},
            add_throttles_rate_alarm={'Warning': {'maxErrorRate': 1}}
        )
        monitoring_logs.monitor_log(
            log_group_name=prefix_unlocking_lambda.log_group.log_group_name,
            human_readable_name='Error logs',
            pattern='ERROR',
            alarm_friendly_name='error logs',
        )

        sfn_unlocking_lambda = lambda_.Function(
            self, "SfnUnlockingLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.AssetCode.from_asset(path.join(os.getcwd(), 's3_caching_solution/lambda/sfn_unlocking')),
            handler="sfn_unlocking.lambda_handler",
            environment={
                "SFN_JOBS_TABLE": sfn_jobs_table.table_name,
                "POWERTOOLS_SERVICE_NAME": powertools_service_name,
                "POWERTOOLS_LOG_LEVEL": powertools_log_level
            },
            timeout=Duration.seconds(900),
            memory_size=320,
            architecture=lambda_.Architecture.ARM_64,
            log_retention=logs.RetentionDays.THREE_MONTHS,
            layers=[powertools_layer]
        )
        monitoring_metrics.monitor_lambda_function(
            lambda_function=sfn_unlocking_lambda,
            add_fault_rate_alarm={'Warning': {'maxErrorRate': 1}},
            add_throttles_rate_alarm={'Warning': {'maxErrorRate': 1}}
        )
        monitoring_logs.monitor_log(
            log_group_name=sfn_unlocking_lambda.log_group.log_group_name,
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
            environment={
                "POWERTOOLS_SERVICE_NAME": powertools_service_name,
                "POWERTOOLS_LOG_LEVEL": powertools_log_level
            },
            memory_size=464,
            architecture=lambda_.Architecture.ARM_64,
            layers=[boto3_lambda_layer, powertools_layer],
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
                report_batch_item_failures=True,
                retry_attempts=3,
                max_record_age=Duration.hours(12),
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
        sfn_jobs_table.grant_read_write_data(prefix_locking_lambda)
        sfn_jobs_table.grant_read_write_data(prefix_unlocking_lambda)
        sfn_jobs_table.grant_read_write_data(sfn_unlocking_lambda)
        working_bucket.grant_read_write(prefix_batch_lambda)

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

        lambda_retry = [
            "Lambda.ClientExecutionTimeoutException",
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
        ]

        task_prefix_locking_lambda = sfn_tasks.LambdaInvoke(
            self, "TaskPrefixLockingLambda",
            lambda_function=prefix_locking_lambda,
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            output_path='$.Payload',
            payload=sfn.TaskInput.from_object({
                "prefix.$": "$.prefix",
                "workflow.$": "$$.Execution.Id",
                "bucket.$": "$.bucket",
                "directory_bucket.$": "$.directory_bucket",
                "force_copy.$": "$.force_copy",
                "ttl.$": "$.ttl",
                "main_workflow.$": '$.main_workflow'
            }),
            retry_on_service_exceptions=False
        )
        task_prefix_locking_lambda.add_retry(
            backoff_rate=2,
            interval=Duration.seconds(2),
            max_attempts=6,
            errors=lambda_retry,
            jitter_strategy=sfn.JitterType.FULL
        )

        task_prefix_batch_lambda = sfn_tasks.LambdaInvoke(
            self, "TaskPrefixBatchLambda",
            lambda_function=prefix_batch_lambda,
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            output_path='$.Payload',
            payload=sfn.TaskInput.from_object({
                "bucket.$": "$$.Execution.Input.bucket",
                "directory_bucket.$": "$$.Execution.Input.directory_bucket",
                "workflow.$": "$$.Execution.Id",
                "payload.$": "$",
                "prefix.$": "$$.Execution.Input.prefix",
                "main_workflow.$": "$$.Execution.Input.main_workflow"
            }),
            retry_on_service_exceptions=False
        )
        task_prefix_batch_lambda.add_retry(
            backoff_rate=2,
            interval=Duration.seconds(2),
            max_attempts=6,
            errors=lambda_retry,
            jitter_strategy=sfn.JitterType.FULL
        )

        task_prefix_unlocking_lambda = sfn_tasks.LambdaInvoke(
            self, "TaskPrefixUnlockingLambda",
            lambda_function=prefix_unlocking_lambda,
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            output_path='$.Payload',
            payload=sfn.TaskInput.from_object({
                "bucket.$": "$$.Execution.Input.bucket",
                "directory_bucket.$": "$$.Execution.Input.directory_bucket",
                "workflow.$": "$$.Execution.Id",
                "payload.$": "$",
                "prefix.$": "$$.Execution.Input.prefix",
                "main_workflow.$": "$$.Execution.Input.main_workflow"
            }),
            retry_on_service_exceptions=False
        )

        task_prefix_unlocking_lambda.add_retry(
            backoff_rate=2,
            interval=Duration.seconds(2),
            max_attempts=6,
            errors=lambda_retry,
            jitter_strategy=sfn.JitterType.FULL
        )

        task_sfn_unlocking_lambda = sfn_tasks.LambdaInvoke(
            self, "TaskSfnUnlockingLambda",
            lambda_function=sfn_unlocking_lambda,
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            output_path='$.Payload',
            retry_on_service_exceptions=False
        )

        task_sfn_unlocking_lambda.add_retry(
            backoff_rate=2,
            interval=Duration.seconds(2),
            max_attempts=6,
            errors=lambda_retry,
            jitter_strategy=sfn.JitterType.FULL
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
                                    "ErrorEquals": lambda_retry,
                                    "IntervalSeconds": 2,
                                    "MaxAttempts": 6,
                                    "BackoffRate": 2,
                                    "JitterStrategy": "FULL"
                                }
                            ],
                            "Next": "Choice"
                        },
                        "Choice": {
                            "Type": "Choice",
                            "Choices": [
                                {
                                "Variable": "$.failed",
                                "IsPresent": True,
                                "Next": "Caching-Strategy"
                                }
                            ],
                            "Default": "Pass"
                        },
                        "Pass": {
                            "Type": "Pass",
                            "End": True
                        }
                    }
                },
                "MaxConcurrency": 50,
                "ItemBatcher": {
                    "MaxItemsPerBatch": 4000,
                    "MaxInputBytesPerBatch": 245760,
                    "BatchInput": {
                        "bucket.$": "$$.Execution.Input.bucket",
                        "ttl.$": "$$.Execution.Input.ttl",
                        "workflow.$": "$$.Execution.Id",
                        "directory_bucket.$": "$$.Execution.Input.directory_bucket",
                        "force_copy.$": "$$.Execution.Input.force_copy",
                        "main_workflow.$": "$$.Execution.Input.main_workflow"
                    }
                },
                "ItemReader": {
                    "Resource": "arn:aws:states:::s3:listObjectsV2",
                    "Parameters": {
                        "Bucket.$": "$$.Execution.Input.bucket",
                        "Prefix.$": "$$.Execution.Input.prefix"
                    }
                },
                "ItemSelector": {
                    "object.$": "$$.Map.Item.Value.Key"
                }
            }
        )

        s3_list_map.add_retry(
            backoff_rate=2,
            interval=Duration.seconds(2),
            max_attempts=6,
            errors=["States.ALL"],
            jitter_strategy=sfn.JitterType.FULL
        )

        # Logic to wait for the S3 Batch job to finish. This could be improved in the future if we release an
        # optimized integration with S3Control that allows to call the job with .sync according to: 
        # https://docs.aws.amazon.com/step-functions/latest/dg/connect-to-resource.html#connect-sync

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
            interval=Duration.seconds(2),
            max_attempts=6,
            errors=["States.ALL"],
            jitter_strategy=sfn.JitterType.FULL
        )

        choice_batch = sfn.Choice(self, 'ChoiceBatch')
        condition_batch_with_job = sfn.Condition.is_present('$.JobId')

        choice_ready = sfn.Choice(self, 'ChoiceReady')
        condition_ready = sfn.Condition.or_(
            sfn.Condition.string_equals('$.Status', 'Complete'),
            sfn.Condition.string_equals('$.Status', 'Failed'),
            sfn.Condition.string_equals('$.Status', 'Cancelled'),
        )

        choice_ready.when(condition_ready, task_prefix_unlocking_lambda).otherwise(wait_batch)


        choice_workflow = sfn.Choice(self, 'ChoiceWorkflow')
        condition_workflow_failed = sfn.Condition.and_(
            sfn.Condition.boolean_equals('$.WaitingForJobs', False),
            sfn.Condition.is_present('$.FailedJobs[0]'),
        )
        condition_workflow_succeeded = sfn.Condition.and_(
            sfn.Condition.is_not_present('$.FailedJobs[0]'),
            sfn.Condition.boolean_equals('$.WaitingForJobs', False)
        )
        condition_workflow_locked = sfn.Condition.boolean_equals('$.WaitingForJobs', True)

        wait_sfn_locked = sfn.Wait(
            self, 'WaitSfnLocked',
            time=sfn.WaitTime.duration(Duration.seconds(30))
        )

        choice_workflow.when(
            condition_workflow_failed, workflow_fail
        ).when(
            condition_workflow_succeeded, workflow_success
        ).when(
            condition_workflow_locked, wait_sfn_locked.next(task_sfn_unlocking_lambda)
        )

        s3_batch_job_chain = wait_batch.next(describe_job).next(choice_ready)
        sfn_unlocking_chain = task_sfn_unlocking_lambda.next(choice_workflow)

        choice_batch.when(
            condition_batch_with_job, s3_batch_job_chain
        ).otherwise(
            task_prefix_unlocking_lambda
        )

        # End of Wait Logic
        map_state = sfn.Map(
            self, "MapPerPrefix",
            items_path="$$.Execution.Input.prefixes",
            item_selector={
                "ttl.$": "$$.Execution.Input.ttl",
                "bucket.$": "$$.Execution.Input.bucket",
                "prefix.$": "$$.Map.Item.Value",
                "force_copy.$": "$$.Execution.Input.force_copy",
                "directory_bucket.$": "$$.Execution.Input.directory_bucket",
                "main_workflow.$": "$$.Execution.Id"
            },
            result_selector={
                "bucket.$": "$$.Execution.Input.bucket",
                "directory_bucket.$": "$$.Execution.Input.directory_bucket",
                "prefixes.$": "$$.Execution.Input.prefixes",
                "force_copy.$": "$$.Execution.Input.force_copy",
                "start_time.$": "$$.Execution.StartTime",
                "main_workflow.$": "$$.Execution.Id"
            }
        ).item_processor(
            task_prefix_locking_lambda.next(s3_list_map).next(task_prefix_batch_lambda).next(choice_batch),
            mode=sfn.ProcessorMode.DISTRIBUTED,
            execution_type=sfn.ProcessorType.STANDARD
        )

        chain_definition = map_state.next(sfn_unlocking_chain)


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
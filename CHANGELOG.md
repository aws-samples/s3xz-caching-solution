# Changelog

All notable changes to this project will be documented in this file.

## 1.1.0 - 2024-07-08

This release includes a complete redesign of the Step Function workflow and added features.

- **Cross SFN run locking:** Basically protects against race conditions when first run of SFN is still working on a portion of data needed on second run of SFN. Up until now, if run 2 was requesting an intersection of prefixes with run 1, it would just test the cache and return a successful run even if run 1 was still working on the S3 Batch operation. There is now new logic locking each of the prefixes being actively worked on, so other runs can check the status, and also the results.
- **Workflow simplification:** Unifying responsibilities and tidying up the integrations between steps. This also required me to eliminate the `Consolidation Threshold` strategy to group together small prefixes to cost optimize the number of S3 Batch job operations being executed, massively helping to achieve change the Cross SFN run locking.

### Added features

- Added [Lambda PowerTools](https://docs.powertools.aws.dev/lambda/python/2.40.1/):
    - to simplify Logging and add more date for troubleshooting, including Context and Correlation ID based on the Main SFN Execution ID.
    - to better manage DynamoDB Stream batch operations and reprocess failed deletions from S3xz on expirations.
- Added a new DynamoDB table to manage locking strategy and job results.

### Updated

- Updated `boto3` from `1.34.78` to `1.34.134` for the Lambda Layer creation.
- Updated `aws-cdk-lib` from `2.135.0` to `2.147.0`
- Updated `cdk-monitoring-constructs` from `7.8.0` to `7.12.0`
- Updated some memory allocation in Lambda to better accommodate new strategies.
- Updated DynamoDB and S3 retention strategy when deleting the CDK Application, now all the resources will be destroy.
- Improved least privilege for all the created roles.

### Removed

- Remove multiple SFN Tasks, including:
    - `consolidation_batch` lambda function, this allowed the solution to group together small prefixes to cost optimize the number of S3 Batch job operations being executed, but also added a complexities to implement the Cross SFN run locking.
    - `caching_logic_reprocess` lambda function, this was the legacy retry method for DDB throttles. This was now included in the main `caching_logic` lambda and the retry flow in the main distributed MAP execution.

## 1.0.0 - 2024-04-15

:seedling: Initial release.
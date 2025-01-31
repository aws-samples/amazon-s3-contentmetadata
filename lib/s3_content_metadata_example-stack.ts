/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

import * as cdk from 'aws-cdk-lib';
import {
  Arn,
  ArnFormat,
  aws_dynamodb,
  aws_events,
  aws_events_targets,
  aws_iam,
  aws_s3,
  CfnOutput,
  Duration,
  RemovalPolicy
} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {AttributeType, StreamViewType, TableEncryptionV2} from "aws-cdk-lib/aws-dynamodb";
import {PythonFunction} from "@aws-cdk/aws-lambda-python-alpha";
import {Application, ApplicationCode, Runtime as FlinkRuntime} from "@aws-cdk/aws-kinesisanalytics-flink-alpha";
import {Runtime} from "aws-cdk-lib/aws-lambda";
import {NagSuppressions} from "cdk-nag";
import {LogGroup, RetentionDays} from "aws-cdk-lib/aws-logs";

export class S3ContentMetadataExampleStack extends cdk.Stack {
  static readonly TABLE_BUCKET_ARN_CONTEXT_KEY = "s3_tables_bucket_arn"
  static readonly TABLE_NAMESPACE_CONTEXT_KEY = "s3_tables_namespace"

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const table_bucket_arn = this.node.getContext(S3ContentMetadataExampleStack.TABLE_BUCKET_ARN_CONTEXT_KEY);
    // Minimal ARN validation check.
    Arn.split(table_bucket_arn, ArnFormat.SLASH_RESOURCE_NAME);
    const table_namespace = this.node.getContext(S3ContentMetadataExampleStack.TABLE_NAMESPACE_CONTEXT_KEY);

    const inputBucket = new aws_s3.Bucket(this, "input", {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      blockPublicAccess: {
        blockPublicAcls: true,
        blockPublicPolicy: true,
        ignorePublicAcls: true,
        restrictPublicBuckets: true
      },
      enforceSSL: true,
      publicReadAccess: false,
      encryption: aws_s3.BucketEncryption.S3_MANAGED,
      versioned: false,
      eventBridgeEnabled: true,
    });
    inputBucket.addLifecycleRule({
      id: 'Cleanup',
      abortIncompleteMultipartUploadAfter: Duration.days(1),
      expiration: Duration.days(31)
    });

    NagSuppressions.addResourceSuppressions(inputBucket, [
      { id: 'AwsSolutions-S1', reason: 'If extending, create a dedicated bucket for S3 Access Log delivery' }
    ])
    NagSuppressions.addResourceSuppressionsByPath(
        this,
        '/S3ContentMetadataExampleStack/BucketNotificationsHandler050a0587b7544547bf325f094a3db834/Role/Resource',
        [
          { id: 'AwsSolutions-IAM4', reason: 'Required construct used internally by the CDK to apply Notifications Configuration'},
        ]
    )
    NagSuppressions.addResourceSuppressionsByPath(
        this,
        '/S3ContentMetadataExampleStack/BucketNotificationsHandler050a0587b7544547bf325f094a3db834/Role/DefaultPolicy/Resource',
        [
          { id: 'AwsSolutions-IAM5', reason: 'Required construct used internally by the CDK to apply Notifications Configuration'}
        ]
    )


    const serializationTable = new aws_dynamodb.TableV2(this, "serialization", {
      partitionKey: {
        name: "s3key",
        type: AttributeType.STRING
      },
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      dynamoStream: StreamViewType.NEW_IMAGE,
      encryption: TableEncryptionV2.dynamoOwnedKey(),
      timeToLiveAttribute: "expire_at"
    })

    const functionRole = new aws_iam.Role(this, "executionRole", {
      assumedBy: new aws_iam.ServicePrincipal('lambda.amazonaws.com'),
    })
    functionRole.addToPolicy(new aws_iam.PolicyStatement({
      actions: [
        "dynamoDb:GetItem",
        "dynamoDb:PutItem",
        "dynamoDb:Query",
        "dynamoDb:Scan",
      ],
      resources: [
          serializationTable.tableArn,
      ]
    }))
    functionRole.addToPolicy(new aws_iam.PolicyStatement({
      actions: [
        "s3:GetObject",
        "s3:GetObjectVersion"
      ],
      resources: [
        inputBucket.bucketArn,
        inputBucket.arnForObjects("*")
      ]
    }))
    functionRole.addToPolicy(new aws_iam.PolicyStatement({
      actions: [
        "rekognition:DetectLabels",
      ],
      resources: [
          "*"
      ]
    }))
    NagSuppressions.addResourceSuppressions(functionRole,
        [
          {id: "AwsSolutions-IAM5", reason: "Rekognition's DetectLabels doesn't apply to a specific resource.", appliesTo: ["Resource::*"]},
          {id: "AwsSolutions-IAM5", reason: "The function is allowed to read any object uploaded to the bucket.", appliesTo: ["Resource::<inputD60CEF7A.Arn>/*"]},
        ],
        true
    )

    const functionLogGroup = new LogGroup(this, 'MetadataExtractionLambdaLogGroup', {
      retention: RetentionDays.ONE_MONTH,
      logGroupName: "MetadataExtractionLambdaLogGroup",
      removalPolicy: RemovalPolicy.DESTROY
    })
    functionLogGroup.grantWrite(functionRole)

    const metadataExtractionFunction = new PythonFunction(this, "metadataExtraction", {
      entry: "metadata",
      runtime: Runtime.PYTHON_3_12,
      index: "extraction/app.py",
      handler: "lambda_handler",
      bundling: {},
      role: functionRole,
      environment: {
        "LOG_LEVEL": "INFO",
        "METADATA_TABLE": serializationTable.tableName
      },
      timeout: Duration.minutes(1),
      logGroup: functionLogGroup
    })

    const rule = new aws_events.Rule(this, "objectNotifications", {
      eventPattern: {
        source: ["aws.s3"],
        detailType: [
            "Object Created",
            "Object Deleted"
        ],
        detail: {
           "bucket": {
             "name": [inputBucket.bucketName]
          }
        }
      }
    })
    rule.addTarget(new aws_events_targets.LambdaFunction(metadataExtractionFunction, {
      retryAttempts: 2
    }))

    const icebergProcessorFlinkApp = new Application(this, "IcebergProcessor", {
      runtime: FlinkRuntime.FLINK_1_19,
      code: ApplicationCode.fromAsset("IcebergProcessor/target/IcebergProcessor.jar"),
      checkpointingEnabled: true,
      checkpointInterval: Duration.seconds(60),
      parallelism: 2,
      parallelismPerKpu: 1,
      propertyGroups: {
        "sdk": {
          "region": this.region
        },
        "stream": {
          "arn": serializationTable.tableStreamArn!
        },
        "catalog": {
          "name": "S3",
          "database": table_namespace,
          "table": "s3_content_metadata",
          "warehousePath": table_bucket_arn,
          "impl": "software.amazon.s3tables.iceberg.S3TablesCatalog",
          "ioImpl": "org.apache.iceberg.aws.s3.S3FileIO"
        },
        "schema": {
          "include_raw_metadata": "true",
          "custom_metadata_fields": "labels, image_width, image_height",
          "field.labels.type": "ARRAY<STRING>",
          "field.labels.jpath": "$.labels[*].Name",
          "field.image_width.type": "INTEGER",
          "field.image_width.jpath": "$.image.image_width",
          "field.image_height.type": "INTEGER",
          "field.image_height.jpath": "$.image.image_height",
        }
      }
    })
    icebergProcessorFlinkApp.role?.addToPrincipalPolicy(new aws_iam.PolicyStatement({
      actions: [
        "dynamodb:DescribeStream",
        "dynamodb:GetRecords",
        "dynamodb:GetShardIterator",
      ],
      resources: [
        serializationTable.tableStreamArn!
      ]
    }))
    icebergProcessorFlinkApp.role?.addToPrincipalPolicy(new aws_iam.PolicyStatement({
      actions: [
        "s3tables:GetTableBucket",
        "s3tables:CreateNamespace",
        "s3tables:GetNamespace",
        "s3tables:ListNamespaces",

        "s3tables:CreateTable",
        "s3tables:GetTable",
        "s3tables:ListTables",
        "s3tables:UpdateTableMetadataLocation",
        "s3tables:GetTableMetadataLocation",
        "s3tables:GetTableData",
        "s3tables:PutTableData",
        "s3tables:DeleteTableData",
        "s3tables:AbortTableMultipartUpload",
        "s3tables:ListTableMultipartUploads",
        "s3tables:ListTableUploadParts",
        "s3tables:TagResource",
        "s3tables:UnTagResource",
        "s3tables:ListTagsForResource"
      ],
      resources: [
        table_bucket_arn,
        table_bucket_arn + "/table/*",
      ],
      conditions: {
        "StringLike": {
          "s3tables:namespace": table_namespace
        }
      }
    }))
    NagSuppressions.addResourceSuppressions(
      icebergProcessorFlinkApp,
      [
        {id:"AwsSolutions-IAM5", reason: "Amazon Managed Service for Flink needs a relatively large permission set to obtain the code & other resources."}
      ],
      true
    )

    new CfnOutput(this, 'ApplicationName', { value: icebergProcessorFlinkApp.applicationName });
    new CfnOutput(this, 'DynamoDbChangeStream', { value: serializationTable.tableStreamArn! });
    new CfnOutput(this, 'InputBucket', { value: inputBucket.bucketName });
    new CfnOutput(this, 'TableBucketArn', {value: table_bucket_arn});
    new CfnOutput(this, 'TableNamespace', { value: table_namespace });
  }
}

{
  "Comment": "Pipeline for data extration from CNPJ",
  "StartAt": "GetTables",
  "States": {
    "GetTables": {
      "Type": "Task",
      "Next": "GetCNPJDownloadInfo",
      "Parameters": {
        "DatabaseName": "cnpj"
      },
      "Resource": "arn:aws:states:::aws-sdk:glue:getTables",
      "ResultPath": "$.DBOutput"
    },
    "GetCNPJDownloadInfo": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "Payload.$": "$",
        "FunctionName": "arn:aws:lambda:us-east-1:598433695633:function:check_update2"
      },
      "OutputPath": "$.Payload.body",
      "Next": "TableLoop"
    },
    "TableLoop": {
      "Type": "Map",
      "ItemProcessor": {
        "ProcessorConfig": {
          "Mode": "INLINE"
        },
        "StartAt": "CheckIfTableExists",
        "States": {
          "CheckIfTableExists": {
            "Type": "Choice",
            "Choices": [
              {
                "Variable": "$.exists",
                "BooleanEquals": false,
                "Next": "BuildFetchLambdaInput"
              }
            ],
            "Default": "GetTablePartition"
          },
          "GetTablePartition": {
            "Type": "Task",
            "Parameters": {
              "DatabaseName": "cnpj",
              "TableName.$": "$.name"
            },
            "Resource": "arn:aws:states:::aws-sdk:glue:getPartitions",
            "ResultSelector": {
              "partitionValues.$": "$.Partitions[*].Values"
            },
            "ResultPath": "$.GetPartitionsOutput",
            "Next": "CheckLastPartition"
          },
          "CheckLastPartition": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {
              "Payload.$": "$",
              "FunctionName": "arn:aws:lambda:us-east-1:598433695633:function:download_test2"
            },
            "Next": "DownloadTest",
            "ResultSelector": {
              "ShouldUpdateTable.$": "$.Payload.body" 
            },
            "ResultPath": "$.CheckLastPartitionResult"
          },
          "DownloadTest": {
            "Type": "Choice",
            "Choices": [
              {
                "Variable": "$.CheckLastPartitionResult.ShouldUpdateTable",
                "BooleanEquals": false,
                "Next": "Success"
              }
            ],
            "Default": "BuildFetchLambdaInput"
          },
          "Success": {
            "Type": "Succeed"
          },
          "BuildFetchLambdaInput": {
            "Type": "Pass",
            "End": true
          }
        }
      },
      "InputPath": "$",
      "End": true,
      "ItemsPath": "$.Tables"
    }
  }
}
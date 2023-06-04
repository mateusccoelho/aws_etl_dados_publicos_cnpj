{
  "Comment": "Pipeline for data extration from CNPJ",
  "StartAt": "GetTables",
  "States": {
    "GetTables": {
      "Type": "Task",
      "Next": "GetCNPJDownloadInfo",
      "Parameters": {
        "DatabaseName": "||nome do database||"
      },
      "Resource": "arn:aws:states:::aws-sdk:glue:getTables",
      "ResultPath": "$.DBOutput"
    },
    "GetCNPJDownloadInfo": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "Payload.$": "$",
        "FunctionName": "||arn da função check_update||"
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
                "Next": "FileLoop"
              }
            ],
            "Default": "GetTablePartition"
          },
          "GetTablePartition": {
            "Type": "Task",
            "Parameters": {
              "DatabaseName": "||nome do database||",
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
              "FunctionName": "||arn do lambda download_test||"
            },
            "Next": "DownloadTest",
            "ResultSelector": {
              "ShouldUpdateTable.$": "$.Payload.body" 
            },
            "ResultPath": "$.CheckLastPartitionResult",
            "Retry": [
              {
                "ErrorEquals": [
                  "States.TaskFailed"
                ],
                "BackoffRate": 2,
                "IntervalSeconds": 60,
                "MaxAttempts": 2
              }
            ]
          },
          "DownloadTest": {
            "Type": "Choice",
            "Choices": [
              {
                "Variable": "$.CheckLastPartitionResult.ShouldUpdateTable",
                "BooleanEquals": false,
                "Next": "SuccessWithoutUpdate"
              }
            ],
            "Default": "FileLoop"
          },
          "SuccessWithoutUpdate": {
            "Type": "Succeed"
          },
          "FileLoop": {
            "Type": "Map",
            "ItemProcessor": {
              "ProcessorConfig": {
                "Mode": "INLINE"
              },
              "StartAt": "Fetch file",
              "States": {
                "Fetch file": {
                  "Type": "Task",
                  "Resource": "arn:aws:states:::lambda:invoke",
                  "Parameters": {
                    "Payload.$": "$",
                    "FunctionName": "||arn do lambda fetch_data||"
                  },
                  "End": true,
                  "Retry": [
                    {
                      "ErrorEquals": [
                        "States.TaskFailed"
                      ],
                      "BackoffRate": 2,
                      "IntervalSeconds": 60,
                      "MaxAttempts": 2
                    }
                  ]
                }
              }
            },
            "InputPath": "$",
            "End": true,
            "ItemsPath": "$.files"
          }
        }
      },
      "InputPath": "$",
      "Next": "CNPJCrawler",
      "ItemsPath": "$.Tables"
    },
    "CNPJCrawler": {
      "Type": "Task",
      "Parameters": {
        "Name": "||nome do crawler||"
      },
      "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
      "Next": "SuccessWithUpdate"
    },
    "SuccessWithUpdate": {
      "Type": "Succeed"
    }
  }
}
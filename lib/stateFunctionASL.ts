export function generateASLDefinition(DDBName: string, LambdaInvokeARN: string, LambdaSNSARN: string, crawlerName: string): any {
return{
  "Comment": "Scrapy code to run on AWS Batch",
  "StartAt": "Lambda with JSON",
  "States": {
    "Fan out batch jobs": {
      "Comment": "Start multiple executions of batch job depending on pre-processed data",
      "ItemsPath": "$.body.Payload.body.BatchJobsList",
      "Iterator": {
        "StartAt": "Submit Batch Job",
        "States": {
          "Failure Update": {
            "End": true,
            "Parameters": {
              "Item": {
                "Status": {
                  "S": "FAILED"
                },
                "job_name": {
                  "S.$": "$.JobName"
                }
              },
              "TableName": DDBName,
            },
            "Resource": "arn:aws:states:::dynamodb:putItem",
            "Type": "Task"
          },
          "Submit Batch Job": {
            "Catch": [
              {
                "ErrorEquals": [
                  "States.TaskFailed"
                ],
                "Next": "Failure Update",
              }
            ],
            "Next": "Sucess Update",
            "Parameters": {
              "JobDefinition.$": "$.JobDefinition",
              "JobName.$": "$.JobName",
              "JobQueue.$": "$.JobQueue"
            },
            "Resource": "arn:aws:states:::batch:submitJob.sync",
            "Type": "Task"
          },
          "Sucess Update": {
            "End": true,
            "Parameters": {
              "Item": {
                "Status": {
                  "S": "SUCCEEDED"
                },
                "job_name": {
                  "S.$": "$.JobName"
                }
              },
              "TableName": "ScrapyForBatchDDB"
            },
            "Resource": "arn:aws:states:::dynamodb:putItem",
            "Type": "Task"
          }
        }
      },
      "MaxConcurrency": 3,
      "Next": "Lambda Invoke",
      "Type": "Map"
    },
    "Lambda Invoke": {
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": LambdaSNSARN,
        "Payload.$": "$"
      },
      "Resource": "arn:aws:states:::lambda:invoke",
      "Retry": [
        {
          "BackoffRate": 2,
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3
        }
      ],
      "Type": "Task",
      "Next": "StartCrawler"
    },
    "StartCrawler": {
      "Type": "Task",
      "End": true,
      "Parameters": {
        "Name": crawlerName,
      },
      "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler"
    },
    "Lambda with JSON": {
      "Next": "Fan out batch jobs",
      "Parameters": {
        "FunctionName": LambdaInvokeARN,
        "Payload.$": "$"
      },
      "Resource": "arn:aws:states:::lambda:invoke",
      "ResultPath": "$.body",
      "Retry": [
        {
          "BackoffRate": 2,
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3
        }
      ],
      "Type": "Task"
    }
  },
  "TimeoutSeconds": 3600
}
}
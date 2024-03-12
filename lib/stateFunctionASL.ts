export function generateASLDefinition(LambdaFunctionArn: string): any {
return{
  "Comment": "Scrapy code to run on AWS Batch",
  "StartAt": "Lambda with JSON",
  "TimeoutSeconds": 3600,
  "States": {
    "Lambda with JSON": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "Payload.$": "$",
        "FunctionName": "arn:aws:lambda:eu-central-1:608792983808:function:ScrapyForBatchInvoke"
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
      "Next": "Fan out batch jobs",
      "ResultPath": "$.body"
    },
    "Fan out batch jobs": {
      "Comment": "Start multiple executions of batch job depending on pre-processed data",
      "Type": "Map",
      "MaxConcurrency": 3,
      "Iterator": {
        "StartAt": "Submit Batch Job",
        "States": {
          "Submit Batch Job": {
            "Type": "Task",
            "Resource": "arn:aws:states:::batch:submitJob.sync",
            "Parameters": {
              "JobName.$": "$.JobName",
              "JobDefinition.$": "$.JobDefinition",
              "JobQueue.$": "$.JobQueue"
            },
            "Next": "Sucess Update",
            "Catch": [
              {
                "ErrorEquals": [
                  "States.TaskFailed"
                ],
                "Next": "Failure Update"
              }
            ]
          },
          "Sucess Update": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:putItem",
            "Parameters": {
              "TableName": "ScrapyForBatchDDB",
              "Item": {
                "job_name": {
                  "S.$": "$.JobName"
                },
                "Status": {
                  "S": "SUCCEEDED"
                },
              }
            },
            "End": true
          },
          "Failure Update": {
            "Type": "Task",
            "Resource": "arn:aws:states:::dynamodb:putItem",
            "Parameters": {
              "TableName": "ScrapyForBatchDDB",
              "Item": {
                "job_name": {
                  "S": "$.JobName"
                },
                "Status": {
                  "S": "FAILED"
                },
              }
            },
            "End": true
          }
        }
      },
      "Next": "SNS Publish",
      "ItemsPath": "$.body.Payload.body.BatchJobsList"
    },
    "SNS Publish": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "Message": {
          "status": "Batch Jobs Succeeded"
        },
        "TopicArn": "arn:aws:sns:eu-central-1:608792983808:ScrapybatchexpStack-MySNSTopicF6FB035B-MwiII3oPUUuP"
      },
      "End": true
    }
  }
}
}
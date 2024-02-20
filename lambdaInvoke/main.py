import json
from datetime import datetime
import os

def generate_datetime_string():
    # Get the current date and time down to the minute
    current_datetime = datetime.utcnow().strftime("%Y%m%d-%H%M")
    return current_datetime

def handler(event, context):
    data = {
        "BatchJobsList": [{
            "JobName": f"matas-{generate_datetime_string()}",
            "JobQueue": os.environ.get("job_queue_ARN"),
            "JobDefinition": "arn:aws:batch:eu-central-1:608792983808:job-definition/MatasJobDef:1",
        },
        {
            "JobName": f"notinode-{generate_datetime_string()}",
            "JobQueue": os.environ.get("job_queue_ARN"),
            "JobDefinition": "arn:aws:batch:eu-central-1:608792983808:job-definition/NotinoDEJobDef:1",
        },
        {
            "JobName": f"superdrug-{generate_datetime_string()}",
            "JobQueue": os.environ.get("job_queue_ARN"),
            "JobDefinition": "arn:aws:batch:eu-central-1:608792983808:job-definition/SuperdrugJobDef:1",
        }
    ]}
    
    return {
        "statusCode": 200,
        "body": data
    }

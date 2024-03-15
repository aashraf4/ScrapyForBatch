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
            "JobDefinition": os.environ.get("matas_job_arn"),
        },
        {
            "JobName": f"notinode-{generate_datetime_string()}",
            "JobQueue": os.environ.get("job_queue_ARN"),
            "JobDefinition": os.environ.get("notino_job_arn"),
        },
        {
            "JobName": f"superdrug-{generate_datetime_string()}",
            "JobQueue": os.environ.get("job_queue_ARN"),
            "JobDefinition": os.environ.get("superdrug_job_arn"),
        }
    ]}
    
    return {
        "statusCode": 200,
        "body": data
    }

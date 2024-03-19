## AWS CDK Scrapy For Batch Stack

**Purpose:**

* Automates deployment of a batch processing pipeline for concurrent Scrapy spider jobs.

**Components:**

* Uses AWS services like Batch, DynamoDB, Lambda, SNS, ECR, and Step Functions. 

**Features:**

* Deploy Scrapy spiders using Docker containers with AWS Batch.
* Dynamically scale resources based on workload with AWS Fargate.
* Manage job execution with a customizable AWS Batch job queue.
* Store job data in DynamoDB for tracking.
* Send notifications upon job completion/failure via Amazon SNS.
* Utilize serverless orchestration with AWS Step Functions.

**How it Works:**

1. **DynamoDB Table:** Stores job metadata and status.
2. **Batch Job Definitions:** Define job specifications for Scrapy spiders.
3. **Batch Job Queue:** Manages execution order of spider jobs.
4. **Lambda Functions:** Initiate batch jobs and handle notifications.
5. **SNS Topic:** Sends notifications to subscribed endpoints.
6. **Glue Crawler:** Crawl the S3 bucket to keep data updated and readily available for viewing in Athena.
7. **Step Functions State Machine:** Orchestrates the job workflow.

**Deployment:**

1. Make sure to have an ECR repository exists and is referenced in ./lib
2. Make sure to have an S3 repository exists and is referenced in the images to be deployed to ECR
3. Make sure to have an AWS Glue DB correctly configured
4. Deploy the stack using `cdk deploy`.
**Additional Notes:**
* Ensure proper IAM roles and permissions for secure access to resources.
![Chart](https://i.imgur.com/3ZKAP7d.png)
![State function](https://i.imgur.com/JNM9uD6.jpeg)


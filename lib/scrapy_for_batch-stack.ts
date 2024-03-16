import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as ecr_assets from 'aws-cdk-lib/aws-ecr-assets';
import * as batch from 'aws-cdk-lib/aws-batch';
import * as ecrdeploy from 'cdk-ecr-deployment';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as sfn_tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as snsSubscriptions from 'aws-cdk-lib/aws-sns-subscriptions';
import * as iam from 'aws-cdk-lib/aws-iam';

import { generateASLDefinition } from './stateFunctionASL'; // Import the ASL definition generation function


export class ScrapyForBatchStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // const repo_ = new ecr.Repository(this, 'exprepoID', {
    //   repositoryName: "scrapyforbatchrepo",
    // });
    
    const image = new ecr_assets.DockerImageAsset(this, 'MatasImage', {
        directory: "./image-matas", // Path to Dockerfile
    });
    
    new ecrdeploy.ECRDeployment(this, 'DeployDockerImage', {
      src: new ecrdeploy.DockerImageName(image.imageUri),
      dest: new ecrdeploy.DockerImageName(`608792983808.dkr.ecr.eu-central-1.amazonaws.com/scrapyforbatchrepo:matasscr`),
    });
        
    const image2 = new ecr_assets.DockerImageAsset(this, 'NotinoDEImage', {
        directory: "./image-notinode", // Path to Dockerfile
    });
    
    new ecrdeploy.ECRDeployment(this, 'DeployDockerImage2', {
      src: new ecrdeploy.DockerImageName(image2.imageUri),
      dest: new ecrdeploy.DockerImageName(`608792983808.dkr.ecr.eu-central-1.amazonaws.com/scrapyforbatchrepo:notdescr`),
    });
  
    const image3 = new ecr_assets.DockerImageAsset(this, 'SuperdrugImage', {
        directory: "./image-superdrug", // Path to Dockerfile
    });
    
    new ecrdeploy.ECRDeployment(this, 'DeployDockerImage3', {
      src: new ecrdeploy.DockerImageName(image3.imageUri),
      dest: new ecrdeploy.DockerImageName(`608792983808.dkr.ecr.eu-central-1.amazonaws.com/scrapyforbatchrepo:superdrugscr`),
    }); 
    
    const computeEnvironment = new batch.CfnComputeEnvironment(this, 'ScrapyComputeEnv', {
      type: 'MANAGED', // Specify the type of compute environment (MANAGED or UNMANAGED)
      computeResources: {
        type: 'FARGATE', // Specify the type of compute resources (EC2 or FARGATE)
        maxvCpus: 1,
        subnets: ['subnet-0f05ebcd82dc83a33', 'subnet-0fea57fdbdc09d846', 'subnet-00f3dd1c7627d0db3'], // Specify the subnets for the compute environment
        securityGroupIds: ['sg-0e033cd1b317e3913'], // Specify the security groups for the compute environment
      },
    });

    const jobDefinition = new batch.CfnJobDefinition(this, 'ScrapyForBatchDef1', {
      jobDefinitionName: 'MatasJobDef',
      type: 'container', // Specify the type of job definition (container or multinode)
      containerProperties: {
        image: `608792983808.dkr.ecr.eu-central-1.amazonaws.com/scrapyforbatchrepo:matasscr`,
        resourceRequirements: [
          {
            type: 'MEMORY', // Specify memory requirement
            value: '512', // Memory in MiB
          },
          {
            type: 'VCPU', // Specify vCPU requirement
            value: '.25', // Number of vCPUs
          }
        ],
        networkConfiguration: {
          assignPublicIp: 'ENABLED',
        },
        command: ['python3', 'matas.py'], // Specify the command to run in the container
        executionRoleArn: 'arn:aws:iam::608792983808:role/simple-ecs-role', // Specify the execution role ARN
      },
      platformCapabilities: ['FARGATE'], // Specify Fargate platform capability
    });


    const jobDefinition2 = new batch.CfnJobDefinition(this, 'ScrapyForBatchDef2', {
      jobDefinitionName: 'NotinoDEJobDef',
      type: 'container', // Specify the type of job definition (container or multinode)
      containerProperties: {
        image: `608792983808.dkr.ecr.eu-central-1.amazonaws.com/scrapyforbatchrepo:notdescr`,
        resourceRequirements: [
          {
            type: 'MEMORY', // Specify memory requirement
            value: '512', // Memory in MiB
          },
          {
            type: 'VCPU', // Specify vCPU requirement
            value: '.25', // Number of vCPUs
          }
        ],
        networkConfiguration: {
          assignPublicIp: 'ENABLED',
        },
        command: ['python3', 'notinoDE.py'], // Specify the command to run in the container
        executionRoleArn: 'arn:aws:iam::608792983808:role/simple-ecs-role', // Specify the execution role ARN
      },
      platformCapabilities: ['FARGATE'], // Specify Fargate platform capability
    });
    
    const jobDefinition3 = new batch.CfnJobDefinition(this, 'ScrapyForBatchDef3', {
      jobDefinitionName: 'SuperdrugJobDef',
      type: 'container', // Specify the type of job definition (container or multinode)
      containerProperties: {
        image: `608792983808.dkr.ecr.eu-central-1.amazonaws.com/scrapyforbatchrepo:superdrugscr`,
        resourceRequirements: [
          {
            type: 'MEMORY', // Specify memory requirement
            value: '512', // Memory in MiB
          },
          {
            type: 'VCPU', // Specify vCPU requirement
            value: '.25', // Number of vCPUs
          }
        ],
        networkConfiguration: {
          assignPublicIp: 'ENABLED',
        },
        command: ['python3', 'superdrug.py'], // Specify the command to run in the container
        executionRoleArn: 'arn:aws:iam::608792983808:role/simple-ecs-role', // Specify the execution role ARN
      },
      platformCapabilities: ['FARGATE'], // Specify Fargate platform capability
    });

    // Define a job queue
    const jobQueue = new batch.CfnJobQueue(this, 'ScrapyJobQueue', {
      priority: 1, // Specify the priority of the job queue
      computeEnvironmentOrder: [
        {
          order: 1, // Specify the order of the compute environment
          computeEnvironment: computeEnvironment.ref, // Reference to the compute environment
        },
      ],
    });
    
    // Define a DynamoDB table with ON_DEMAND billing mode
    const table = new dynamodb.Table(this, 'MyDynamoDBTable', {
      tableName: "ScrapyForBatchDDB",
      partitionKey: {
        name: 'job_name',
        type: dynamodb.AttributeType.STRING
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST // ON_DEMAND billing mode
    });

    const lambdaFunction = new lambda.DockerImageFunction(this, 'InvokeFunction', {
      functionName: 'ScrapyForBatchInvoke',
      code: lambda.DockerImageCode.fromImageAsset('./lambdaInvoke'),
      memorySize: 128,
      timeout: cdk.Duration.seconds(30),
      environment: {
        // Specify your environment variables here
        job_queue_ARN: jobQueue.ref,
        matas_job_arn: jobDefinition.ref,
        superdrug_job_arn: jobDefinition3.ref,
        notino_job_arn: jobDefinition2.ref,        
      },
    });
    // Define the SNS topic
    const topic = new sns.Topic(this, 'MySNSTopic', {
      displayName: 'BatchScrapy' // Name the SNS Topic
    });
    topic.addSubscription(new snsSubscriptions.EmailSubscription('aashrafw4@gmail.com'));


    const snsLambdaFunction = new lambda.DockerImageFunction(this, 'SnsFunction', {
      functionName: 'ScrapyForBatchSNS',
      code: lambda.DockerImageCode.fromImageAsset('./lambda-send-sns'),
      memorySize: 128,
      timeout: cdk.Duration.seconds(30),
      environment: {
        // Specify your environment variables here
        sns_ARN: topic.topicArn,
        ddb_ARN: table.tableName,
      },
    });

    // Add permissions to access DynamoDB
    table.grantReadData(snsLambdaFunction);

    // Add permissions to publish to SNS topic
    topic.grantPublish(snsLambdaFunction);

    // Optionally, you can grant additional permissions as needed
    snsLambdaFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: ['sns:ListSubscriptionsByTopic'],
      resources: [topic.topicArn]
    }));
    // Create a new state machine
    // Generate the ASL definition using the provided function ARN
    const aslDefinition = generateASLDefinition(table.tableName, lambdaFunction.functionArn, snsLambdaFunction.functionArn);

    
    const cfnStateMachine = new sfn.CfnStateMachine(this, 'MyCfnStateMachine', {
      roleArn: 'arn:aws:iam::608792983808:role/stepfunctionrole', // Replace with the IAM role ARN

      // Optional properties
      definition: aslDefinition,
      stateMachineName: 'StepFuncBatchScr', // Replace with the desired state machine name
      tracingConfiguration: {
        enabled: true // Enable X-Ray tracing
      }
    });
  }
}

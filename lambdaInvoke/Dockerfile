# Use the official AWS Lambda Python 3.11 base image
FROM public.ecr.aws/lambda/python:3.11

# Set the working directory to /var/task (Lambda task root)
WORKDIR ${LAMBDA_TASK_ROOT}

# Copy requirements.txt to the working directory
COPY requirements.txt .

# Install the specified Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy all files in the current directory to the working directory
COPY . .

# Set the CMD to your Lambda function handler
CMD [ "main.handler" ]
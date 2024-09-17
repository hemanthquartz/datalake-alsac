import json
import boto3
import time
import os
from task1 import run_task1
from task2 import run_task2

# Initialize the CloudWatch Logs client
logs_client = boto3.client('logs')

# Define AWS region for the CloudWatch URL generation
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

def create_log_stream(log_group_name, context):
    """
    Create a new log stream for a task in the provided log group.
    """
    log_stream_name = f"{context.aws_request_id}/{int(time.time())}"
    
    # Ensure log group exists
    try:
        logs_client.create_log_group(logGroupName=log_group_name)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass
    
    # Create a new log stream
    logs_client.create_log_stream(
        logGroupName=log_group_name,
        logStreamName=log_stream_name
    )
    
    return log_stream_name

def log_event(log_group_name, log_stream_name, message):
    """
    Log a message to the specified log stream.
    """
    timestamp = int(time.time() * 1000)
    
    # Fetch the next sequence token (if needed)
    response = logs_client.describe_log_streams(
        logGroupName=log_group_name,
        logStreamNamePrefix=log_stream_name
    )
    
    log_streams = response['logStreams']
    if log_streams and 'uploadSequenceToken' in log_streams[0]:
        sequence_token = log_streams[0]['uploadSequenceToken']
    else:
        sequence_token = None
    
    # Put the log event
    logs_client.put_log_events(
        logGroupName=log_group_name,
        logStreamName=log_stream_name,
        logEvents=[{
            'timestamp': timestamp,
            'message': message
        }],
        sequenceToken=sequence_token
    )

def generate_log_stream_url(region, log_group_name, log_stream_name):
    """
    Generate the CloudWatch URL for the log stream.
    """
    log_stream_url = (
        f"https://{region}.console.aws.amazon.com/cloudwatch/home?"
        f"region={region}#logsV2:log-groups/"
        f"log-group/{log_group_name.replace('/', '$252F')}"
        f"/log-events/{log_stream_name.replace('/', '$252F')}"
    )
    return log_stream_url

def lambda_handler(event, context):
    # Define custom log groups for each task
    task1_log_group = '/custom/task1/logs'
    task2_log_group = '/custom/task2/logs'
    
    # Create log streams for each task
    task1_log_stream = create_log_stream(task1_log_group, context)
    task2_log_stream = create_log_stream(task2_log_group, context)
    
    # Call task1 and task2
    task1_result = run_task1()
    task2_result = run_task2()
    
    # Log the results of each task to their respective log groups
    log_event(task1_log_group, task1_log_stream, f"Task1 Result: {task1_result}")
    log_event(task2_log_group, task2_log_stream, f"Task2 Result: {task2_result}")
    
    # Generate log stream URLs for each task
    task1_log_url = generate_log_stream_url(AWS_REGION, task1_log_group, task1_log_stream)
    task2_log_url = generate_log_stream_url(AWS_REGION, task2_log_group, task2_log_stream)
    
    # Return response with log stream URLs
    return {
        'statusCode': 200,
        'body': json.dumps({
            'task1_log_url': task1_log_url,
            'task2_log_url': task2_log_url,
            'task1_result': task1_result,
            'task2_result': task2_result
        })
    }
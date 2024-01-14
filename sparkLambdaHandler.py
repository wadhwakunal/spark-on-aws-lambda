import boto3
import botocore
import sys
import os
import subprocess
import logging
import json
import time

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
#handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
#handler.setFormatter(formatter)
#logger.addHandler(handler)
            
def get_unprocessed_files(s3_bucket_script: str,unprocessed_file_key: str) -> str:
    try:
        logger.info('Inside function get_unprocessed_files')
        s3 = boto3.resource("s3")
        content = s3.Object(s3_bucket_script, unprocessed_file_key).get()['Body'].read().decode('utf-8')
        logger.info(f'Unprocessed files: {content}')
        logger.info(f'Now deleting file {unprocessed_file_key}')
        s3.Object(s3_bucket_script, unprocessed_file_key).delete()
        return content
    except botocore.exceptions.ClientError as e:
        logger.error(f"Boto Error: {e.response['Error']['Code']}")
    except Exception as e :
        logger.error(f"Error: {e}")
    else:
        logger.info(f'Successfully extracted file names from {unprocessed_file_key}')
    
def s3_script_download(s3_bucket_script: str,input_script: str)-> None:
    """
    """
    try:
        logger.info(f'Now downloading script {input_script} in {s3_bucket_script} to /tmp')
        s3_client = boto3.resource("s3")
        s3_client.Bucket(s3_bucket_script).download_file(input_script, "/tmp/spark_script.py")    
    except Exception as e :
        logger.error(f'Error downloading the script {input_script} in {s3_bucket_script}: {e}')
    else:
        logger.info(f'Script {input_script} successfully downloaded to /tmp')

def write_content_to_s3_file(s3_bucket: str,s3_key: str,content: str) -> None:
    logger.info(f"Writing content to file {s3_key}")
    try:
        s3_client = boto3.resource("s3")
        s3_client.Bucket(s3_bucket).put_object(Key=s3_key, Body=content.encode("utf-8"))
    except botocore.exceptions.ClientError as e:
        logger.error(f"Boto Error: {e.response['Error']['Code']}")
    except Exception as e :
        logger.error(f"Error: {e}")
    else:
        logger.info(f'Successfully written content to file {s3_key}')

def read_content_from_s3_file(s3_bucket: str,s3_key: str) -> str:
    logger.info(f"Reading content from file {s3_key}")
    try:
        s3_client = boto3.resource("s3")
        content = s3_client.Object(s3_bucket, s3_key).get()['Body'].read().decode('utf-8')
        return content
    except botocore.exceptions.ClientError as e:
        logger.error(f"Boto Error: {e.response['Error']['Code']}")
        return ""
    except Exception as e :
        logger.error(f"Error: {e}")
        return ""
    else:
        logger.info(f'Successfully read content from file {s3_key}')

def delete_file_from_s3(s3_bucket: str,s3_key: str) -> None:
    logger.info(f"Deleting file {s3_key}")
    try:
        s3_client = boto3.resource("s3")
        s3_client.Object(s3_bucket, s3_key).delete()
    except botocore.exceptions.ClientError as e:
        logger.error(f"Boto Error: {e.response['Error']['Code']}")
    except Exception as e :
        logger.error(f"Error: {e}")
    else:
        logger.info(f'Successfully deleted file {s3_key}')

def spark_submit(s3_bucket_script: str,input_script: str, event: dict)-> None:
    """
    Submits a local Spark script using spark-submit.
    """
     # Set the environment variables for the Spark application
    # pyspark_submit_args = event.get('PYSPARK_SUBMIT_ARGS', '')
    # # Source input and output if available in event
    # input_path = event.get('INPUT_PATH','')
    # output_path = event.get('OUTPUT_PATH', '')

    #for key,value in event.items():
    #    os.environ[key] = value
    # Run the spark-submit command on the local copy of the script
    try:
        logger.info(f'Spark-Submitting the Spark script {input_script} from {s3_bucket_script}')
        subprocess.run(["spark-submit", "/tmp/spark_script.py", "--event", json.dumps(event)], check=True, env=os.environ)
    except Exception as e :
        logger.error(f'Error Spark-Submit with exception: {e}')
        logger.info(f'Writing unprocessed_file content to the error file: {event["error_file_key"]}')
        write_content_to_s3_file(event["error_file_bucket"],event["error_file_key"],os.environ['INPUT_PATHS'])
        raise e
    else:
        logger.info(f'Script {input_script} successfully submitted')
        logger.info(f'Deleting error file {event["error_file_key"]}')
        delete_file_from_s3(event["error_file_bucket"],event["error_file_key"])
        

def lambda_handler(event, context):

    """
    Lambda_handler is called when the AWS Lambda
    is triggered. The function is downloading file 
    from Amazon S3 location and spark submitting 
    the script in AWS Lambda
    """

    logger.info("******************Start AWS Lambda Handler************")
    s3_bucket_script = os.environ['SCRIPT_BUCKET']
    input_script = os.environ['SPARK_SCRIPT']
    
    #Get the S3 key of file consisting names of unprocessed files from the triggering lambda
    unprocessed_file_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    unprocessed_file_key = event["Records"][0]["s3"]["object"]["key"]
    
    #Get all the unprocessed files as string(each filename in a new line)
    unprocessed_files = get_unprocessed_files(unprocessed_file_bucket,unprocessed_file_key)
    
    #Append error content from error file if it exists
    unprocessed_files = unprocessed_files + "\n" + read_content_from_s3_file(unprocessed_file_bucket, unprocessed_file_key.replace("unprocessed_file","error_file"))
    unprocessed_files = unprocessed_files.rstrip()
    logger.info(f"Final list of unprocessed_files: {unprocessed_files}")
    os.environ['INPUT_PATHS'] = unprocessed_files

    #Download Spark script from S3 to local
    s3_script_download(s3_bucket_script,input_script)
    
    # Set the environment variables for the Spark application
    event['error_file_bucket'] = unprocessed_file_bucket
    event['error_file_key'] = unprocessed_file_key.replace("unprocessed_file","error_file")
    spark_submit(s3_bucket_script,input_script, event)
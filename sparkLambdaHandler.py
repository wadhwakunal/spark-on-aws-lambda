import boto3
import botocore
import sys
import os
import subprocess
import logging
import json
import time
import datetime
import dateutil.tz

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
#handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
#handler.setFormatter(formatter)
#logger.addHandler(handler)
            
def s3_script_download(s3_bucket_script: str,input_script: str)-> None:
    """
    """
    try:
        logger.info(f'Now downloading script {input_script} in {s3_bucket_script} to /tmp')
        s3_client = boto3.resource("s3")
        s3_client.Bucket(s3_bucket_script).download_file(input_script, "/tmp/spark_script.py")    
    except Exception as e :
        logger.error(f'Error downloading the script {input_script} in {s3_bucket_script}: {e}')
        raise e
    else:
        logger.info(f'Script {input_script} successfully downloaded to /tmp')

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
        subprocess.run(["spark-submit", "/tmp/spark_script.py", "--batch_date", json.dumps(event["batch_date"])], check=True, env=os.environ)
    except Exception as e :
        logger.error(f'Error Spark-Submit with exception: {e}')
        raise e
    else:
        logger.info(f'Spark job {input_script} successfully completed')

def raise_alert(job_name,error):
    try:
        logger.info("Inside function raise_alert")
        ses_client = boto3.client("ses")
        CHARSET = "UTF-8"
        ist_tz = dateutil.tz.gettz('Asia/Calcutta')
        current_timestamp = datetime.datetime.now(tz=ist_tz)
    
        response = ses_client.send_email(
            Destination={
                "ToAddresses": [
                    "kunal.wadhwa@sportsbaazi.com",
                    "gautam.dey@sportsbaazi.com",
                    "siddharth.tripathi@sportsbaazi.com"
                ],
            },
            Message={
                "Body": {
                    "Text": {
                        "Charset": CHARSET,
                        "Data": f"Error in Spark Lambda to process {job_name} data \n {error}",
                    }
                },
                "Subject": {
                    "Charset": CHARSET,
                    "Data": f"Error: {job_name} processing {current_timestamp}",
                },
            },
            Source="bb-datalake@sportsbaazi.com"
        )
    except Exception as e :
        logger.error(f'Error in raising alert: {e}')
        

def lambda_handler(event, context):

    """
    Lambda_handler is called when the AWS Lambda
    is triggered. The function is downloading file 
    from Amazon S3 location and spark submitting 
    the script in AWS Lambda
    """
    try:
        logger.info("******************Start AWS Lambda Handler************")
        s3_bucket_script = os.environ['SCRIPT_BUCKET']
        input_script = os.environ['SPARK_SCRIPT']
        
        #Set the S3 key of the batch data that needs to be processed as environment variable
        os.environ['batch_date'] = event["batch_date"]
        
        #Download Spark script from S3 to local
        s3_script_download(s3_bucket_script,input_script)
        
        #Trigger Spark Job
        spark_submit(s3_bucket_script,input_script, event)

        return { "job_status": "Passed" }
    except Exception as e :
        #raise_alert(table_name,e)
        return { "job_status": "Failed"}
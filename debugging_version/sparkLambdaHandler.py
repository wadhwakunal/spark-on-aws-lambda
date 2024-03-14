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
            
def get_unprocessed_files(s3_bucket_script: str,unprocessed_file_key: str) -> str:
    try:
        logger.info('Inside function get_unprocessed_files')
        s3 = boto3.resource("s3")
        content = s3.Object(s3_bucket_script, unprocessed_file_key).get()['Body'].read().decode('utf-8')
        logger.info(f'Unprocessed files: {content}')
        #logger.info(f'Now deleting file {unprocessed_file_key}')
        #s3.Object(s3_bucket_script, unprocessed_file_key).delete()
        return content
    except botocore.exceptions.ClientError as e:
        logger.error(f"Boto Error: {e.response['Error']['Code']}")
        raise e
    except Exception as e :
        logger.error(f"Error: {e}")
        raise e
    else:
        logger.info(f'Successfully extracted file names from {unprocessed_file_key}')

def get_unprocessed_files_size(unprocessed_files: str):
    try:
        logger.info('Inside function get_unprocessed_files_size')
        unprocessed_files_list = unprocessed_files.splitlines()
        s3 = boto3.resource("s3")
        files_size = 0
        for unprocessed_file in unprocessed_files_list:
            bucket_name = unprocessed_file.split('/')[2]
            key_name = '/'.join(unprocessed_file.split('/')[3:])
            size = s3.Object(bucket_name, key_name).content_length
            logger.info(f'Unprocessed file bucket: {bucket_name}, Unprocessed file key: {key_name}, Unprocessed file size: {size}')
            files_size += size
        #logger.info(f'Now deleting file {unprocessed_file_key}')
        #s3.Object(s3_bucket_script, unprocessed_file_key).delete()
        return files_size
    except botocore.exceptions.ClientError as e:
        logger.error(f"Boto Error: {e.response['Error']['Code']}")
        raise e
    except Exception as e :
        logger.error(f"Error: {e}")
        raise e
    
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

def write_content_to_s3_file(s3_bucket: str,s3_key: str,content: str) -> None:
    logger.info(f"Writing content to file {s3_key}")
    try:
        s3_client = boto3.resource("s3")
        s3_client.Bucket(s3_bucket).put_object(Key=s3_key, Body=content.encode("utf-8"))
    except botocore.exceptions.ClientError as e:
        logger.error(f"Boto Error: {e.response['Error']['Code']}")
        raise e
    except Exception as e :
        logger.error(f"Error: {e}")
        raise e
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

def load_partitions(database_name: str,table_name: str,athena_workgroup: str) -> None:
    logger.info(f'Loading partitions of table: {database_name}.{table_name}')
    try:
        athena_client = boto3.client("athena")
        query = f'MSCK REPAIR TABLE {database_name}.{table_name}'
        athena_client.start_query_execution(QueryString=query,QueryExecutionContext={'Database': database_name},WorkGroup=athena_workgroup)
    except botocore.exceptions.ClientError as e:
        logger.error(f"Boto Error: {e.response['Error']['Code']}")
    except Exception as e :
        logger.error(f"Error: {e}")
    else:
        logger.info(f'Successfully loaded partitions in table {database_name}.{table_name}')

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
        load_partitions(event['database_name'],event['table_name'],event['athena_workgroup'])
    except Exception as e :
        logger.error(f'Error Spark-Submit with exception: {e}')
        #logger.info(f'Writing unprocessed_file content to the error file: {event["error_file_key"]}')
        #write_content_to_s3_file(event["error_file_bucket"],event["error_file_key"],os.environ['INPUT_PATHS'])
        raise e
    else:
        logger.info(f'Script {input_script} successfully submitted')
        logger.info(f'Deleting error file {event["error_file_key"]}')
        delete_file_from_s3(event["error_file_bucket"],event["error_file_key"])

def glue_submit(job_name: str, arguments: dict) -> None:
    glue_client = boto3.client("glue")
    try:
        job_run_id = glue_client.start_job_run(JobName=job_name, Arguments=arguments)
        return job_run_id
    except botocore.exceptions.ClientError as e:
        raise Exception( "boto3 client error in run_glue_job: " + e.__str__())
    except Exception as e:
      raise Exception( "Unexpected error in run_glue_job: " + e.__str__())

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
        database_name = os.environ['DATABASE_NAME']
        table_name = os.environ['TABLE_NAME']
        athena_workgroup = os.environ['ATHENA_WORKGROUP']
        glue_job = os.environ['GLUE_JOB']
        threshold = os.environ['DATA_THRESHOLD']
        
        #Get the S3 key of file consisting names of unprocessed files from the triggering lambda
        unprocessed_file_bucket = event["Records"][0]["s3"]["bucket"]["name"]
        unprocessed_file_key = event["Records"][0]["s3"]["object"]["key"]
        
        #Get all the unprocessed files as string(each filename in a new line)
        unprocessed_files = get_unprocessed_files(unprocessed_file_bucket,unprocessed_file_key)
        
        #Append error content from error file if it exists
        unprocessed_files = unprocessed_files.strip() + "\n" + read_content_from_s3_file(unprocessed_file_bucket, unprocessed_file_key.replace("unprocessed_file","error_file"))
        unprocessed_files = unprocessed_files.strip()
        logger.info(f"Final list of unprocessed_files: {unprocessed_files}")

        #Calculate total size of unprocessed files
        total_size_unprocessed_file = get_unprocessed_files_size(unprocessed_files)
        logger.info(f"Size of unprocessed_files: {total_size_unprocessed_file}")

        #Check if total size of uprocessed files is greater than the threshold
        if int(total_size_unprocessed_file) > threshold:
            #Set arguments for glue job
            arguments = {'--INPUT_PATHS': unprocessed_files}
            #Run Glue job
            glue_submit(glue_job,arguments)
        else:
            #Run Spark lambda job
            os.environ['INPUT_PATHS'] = unprocessed_files
    
            #Download Spark script from S3 to local
            s3_script_download(s3_bucket_script,input_script)
        
            # Set the environment variables for the Spark application
            event['error_file_bucket'] = unprocessed_file_bucket
            event['error_file_key'] = unprocessed_file_key.replace("unprocessed_file","error_file")
            event['database_name'] = database_name
            event['table_name'] = table_name
            event['athena_workgroup'] = athena_workgroup
            spark_submit(s3_bucket_script,input_script, event)
    except Exception as e :
        #raise_alert(table_name,e)
        raise e
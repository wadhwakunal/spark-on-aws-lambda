# Use AWS Lambda Python 3.8 image as base
FROM public.ecr.aws/lambda/python:3.8

# Setting the compatible versions of libraries
ARG HADOOP_VERSION=3.2.4
ARG AWS_SDK_VERSION=1.11.901
ARG PYSPARK_VERSION=3.3.0
ARG DELTA_VERSION=2.3.0

# Perform system updates and install dependencies
RUN yum update -y && \
    yum -y update zlib && \
    yum -y install wget && \
    yum -y install yum-plugin-versionlock && \
    yum -y versionlock add java-1.8.0-openjdk-1.8.0.362.b08-0.amzn2.0.1.x86_64 && \
    yum -y install java-1.8.0-openjdk && \
    pip install --upgrade pip && pip install boto3 && \
    pip install pyspark==$PYSPARK_VERSION && \
	pip install delta-spark==$DELTA_VERSION && \
    yum clean all


# Set environment variables for PySpark
ENV SPARK_HOME="/var/lang/lib/python3.8/site-packages/pyspark"
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PATH=$PATH:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH
ENV PATH=$SPARK_HOME/python:$PATH


COPY download_jars.sh /tmp
RUN mkdir -p /var/task/spark-warehouse && mkdir -p /var/task/metastore_db && \
	chmod -R 777 /var/task/spark-warehouse && chmod -R 777 /var/task/metastore_db && \
	chmod +x /tmp/download_jars.sh && \
    /tmp/download_jars.sh $SPARK_HOME $HADOOP_VERSION $AWS_SDK_VERSION $DELTA_VERSION


ENV PATH=${PATH}:${JAVA_HOME}/bin

# Setting  up the ENV vars for local code, in AWS LAmbda you have to set Input_path and Output_path
ENV INPUT_PATH=""
ENV OUTPUT_PATH=""
ENV AWS_ACCESS_KEY_ID=""
ENV AWS_SECRET_ACCESS_KEY=""
ENV AWS_REGION=""
ENV AWS_SESSION_TOKEN=""
ENV CUSTOM_SQL=""

# spark-class file is setting the memory to 1 GB
COPY spark-class $SPARK_HOME/bin/
RUN chmod -R 755 $SPARK_HOME

# Copy the Pyspark script to container
COPY sparkLambdaHandler.py ${LAMBDA_TASK_ROOT}

# calling the Lambda handler
CMD [ "/var/task/sparkLambdaHandler.lambda_handler" ]
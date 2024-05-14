from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerEndpointOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import time
import os
import json
import boto3

'''
This DAG fetches image URLs from a database, sends them to SageMaker for inference, and writes the results back to the database.
Airflow initialization and DAG configuration
'''

inference_bucket = "s3://treetracker-species-id"
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'image_inference_sagemaker',
    default_args=default_args,
    description='Fetch image URLs from DB, send to SageMaker for inference, write results back to DB',
    schedule_interval='@daily',
    catchup=False,
)

'''
Helper functions to generate the input manifest file and trigger the batch transform job in SageMaker
'''
def generate_input_manifest(image_urls, dest_bucket):
    '''
    After fetching the image URLs from the database, generate the input manifest file and upload to S3 for the
    batch transform job to have an input

    :param image_urls:
    :return:
    '''

    # Path where the manifest file will be saved
    output_file_path = 'path/to/your/output-manifest.manifest'

    # Open a file to write
    with open(output_file_path, 'w') as file:
        for uri in image_urls:
            # Create a dictionary with the 'source-ref' key
            entry = {"source-ref": uri}
            # Write the JSON object as a string to the file
            file.write(json.dumps(entry) + '\n')

    print(f"Manifest file created at {output_file_path}")
    # Create an S3 client
    s3 = boto3.client('s3')

    # Upload the manifest file
    s3.upload_file(Filename=output_file_path, Bucket=dest_bucket, Key='path/to/your/output-manifest.manifest')
    print("Manifest file uploaded to S3")

def trigger_batch_transform_job_inference(model_name: str, instance_type: str='ml.m5.large'):
    # TODO: update payload and batch strategy as needed
    sm_boto3 = boto3.client('sagemaker', region_name='eu-central-1')

    batch_job_name = "species-id-job-" + time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime())
    model_name = model_name
    input_data_location = os.path.join(inference_bucket, 'input-manifest.manifest')
    output_location = os.path.join(inference_bucket, 'output')
    # Create the batch transform job
    response = sm_boto3.create_transform_job(
        TransformJobName=batch_job_name,
        ModelName=model_name,
        MaxConcurrentTransforms=4,
        MaxPayloadInMB=6,
        BatchStrategy='MultiRecord',
        TransformInput={
            'DataSource': {
                'S3DataSource': {
                    'S3DataType': 'ManifestFile',
                    'S3Uri': input_data_location
                }
            },
        },
        TransformOutput={
            'S3OutputPath': output_location
        },
        TransformResources={
            'InstanceType': instance_type,
            'InstanceCount': 1
        }
    )
    print("response:", response)


'''
Airflow operators to fetch data from a database, send it to SageMaker for inference, and write the results back to the database.
'''


# TODO: Currently placeholder SQL query to fetch image URLs from the database
sql_filter_by_org_and_date = '''
SELECT image_url from trees WHERE organization = 'your_org' AND date = '{{ ds }}'; 
'''

# Task to fetch data from the database
fetch_data = PostgresOperator(
    task_id='fetch_data_from_db',
    postgres_conn_id='your_db_connection', # need to fill in
    sql=sql_filter_by_org_and_date,  # SQL file or query string
    dag=dag,
)

def call_sagemaker(**context):
    image_urls = context['ti'].xcom_pull(task_ids='fetch_data_from_db')
    # You might need to process image_urls before sending to SageMaker
    generate_input_manifest(image_urls, context["dest_bucket"])
    response = trigger_batch_transform_job_inference()
    return response
    # SageMaker API call goes here
    # Save the output to XComs or directly use another task to save it to DB

call_sagemaker_task = PythonOperator(
    task_id='call_sagemaker_endpoint',
    python_callable=call_sagemaker,
    provide_context=True,
    dag=dag,
)

# Task to write the results back to the database
write_results = PostgresOperator(
    task_id='write_results_to_db',
    postgres_conn_id='your_db_connection',
    sql='sql/query_to_write_data.sql',  # You need to craft this based on how data needs to be written back
    dag=dag,
)

def clean_s3_buckets():
    # Clean up S3 buckets after processing
    s3 = boto3.client('s3')
    s3.delete_object(Bucket=inference_bucket, Key='today/input-manifest.manifest')
    s3.delete_object(Bucket=inference_bucket, Key='today/output/')
    print("S3 buckets cleaned up")

clean_s3_buckets = PythonOperator(
    task_id='clean_s3_buckets',
    python_callable=clean_s3_buckets,
    dag=dag,
)
# Setting up dependencies
fetch_data >> call_sagemaker_task >> write_results



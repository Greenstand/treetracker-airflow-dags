import species_id
import unittest
import psycopg2
import os
import boto3
import json
import time
import urllib
import urllib3
from sshtunnel import SSHTunnelForwarder
from sqlalchemy.orm import sessionmaker #Run pip install sqlalchemy
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
'''
Make sure treetracker-species-id has the following policy added


{
  "Statement":[
    {
      "Sid": "AddPerm",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::examplebucket/*"
    }
  ]
}


so that the uploaded manifest file can be read by SageMakers. Otherwise you have to manually change the 
permissions on the manifest file after uploading and before starting the SageMaker job. 
'''
# assigns environment variables for testing
# this file might be moved to the root directory
import credentials

#  add https://treetracker-production-images.s3.eu-central-1.amazonaws.com/ before img


class Test(unittest.TestCase):
    def test_data_import(self):
        org_id = '8b2628b3-733b-4962-943d-95ebea918c9d'
        start_date = '2024-06-24'
        end_date = '2024-07-24'

        haiti_topleft = (20.02535383561072, -74.47685260343907)
        haiti_bottomright = (17.405263862983954, -71.5108667436321)

        # Get image URLs from the database
        url_list = species_id.get_image_urls(haiti_topleft, haiti_bottomright, start_date, end_date)
        print (url_list)
        return url_list


    def test_get_max_time_created(self):
        # Read environment variable for DB connection string
        conn = psycopg2.connect(os.environ["DB_CONNECTION_STRING"], sslmode='require')

        # SQL query to get the maximum time_created
        sql_max_time_created = '''
            SELECT MAX(time_created) AS max_time_created
            FROM public.trees;
        '''

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(sql_max_time_created)
        result = cursor.fetchone()
        print(result['max_time_created'])

        cursor.close()
        conn.close()

    def test_generate_input_manifest(self):
        '''
        After fetching the image URLs from the database, generate the input manifest file and upload to S3 for the
        batch transform job to have an input

        Verified this works on S3 console online
        :param image_urls:
        :return:
        '''
        org_id = '8b2628b3-733b-4962-943d-95ebea918c9d'
        start_date = '2024-06-24'
        end_date = '2024-07-24'

        haiti_topleft = (20.02535383561072, -74.47685260343907)
        haiti_bottomright = (17.405263862983954, -71.5108667436321)

        # Get image URLs from the database
        image_urls = species_id.get_image_urls(haiti_topleft, haiti_bottomright, start_date, end_date)
        output_file_path = '../local_data/daily-training.manifest'

        # Create an S3 client
        s3_client = boto3.client('s3',
                            aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
                            aws_secret_access_key=os.environ["AWS_SECRET_KEY"]
                            )

        dest_bucket = "treetracker-species-id"
        key = "inference/daily-training.manifest"
        http = urllib3.PoolManager()

        # Open a file to write
        with open(output_file_path, 'w+') as file:
            for uri in image_urls:
                # Create a dictionary with the 'source-ref' key
                entry = {"image-ref": uri}
                # Write the JSON object as a string to the file
                file.write(json.dumps(entry) + '\n')
                urllib.request.urlopen(uri)  # Provide URL
                # stream file to s3 bucket
                s3_client.upload_fileobj(http.request('GET', uri, preload_content=False),
                                         dest_bucket,
                                         "inference" + "/" + uri.split('/')[-1],
                )

        print(f"Manifest file created at {output_file_path}")
        # s3_client.upload_file(Filename=output_file_path, Bucket=dest_bucket, Key=key,
        #                      )
        print("Manifest file uploaded to S3")


    def test_model_inferences(self):
        '''
        To Do: see if the result that results from this can be used in the Python script or in the DAG

        To Do: Automate permission writing for the manifest file so that the SageMaker job can access it- otherwise,
        after creating it you need to go into it and manually allow SageMaker to access it. Maybe already solevd with recent
        change to bucket-level permissions.

        To Do: Test model output from notebook or normal perspective doesn't differ from batch transform API
        (i.e. ensure preprocessing is consistent)


        :return:
        '''
        aws_client = boto3.client('sagemaker',
                                 aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
                                 aws_secret_access_key=os.environ["AWS_SECRET_KEY"],
                                region_name=os.environ["AWS_REGION"]
                                 )
        batch_job_name = "species-id-job-" + time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime())
        model_name = "haitibeta2"
        data_manifest_file = "daily-training.manifest"  # this should be in the same bucket as the inference bucket
        inference_bucket = "treetracker-species-id"

        input_data_location = "s3://" + inference_bucket + "/" + "inference/"
        output_location = "s3://" + inference_bucket + "/" + "predictions/"
        assert input_data_location == "s3://treetracker-species-id/inference/"
        assert output_location == "s3://treetracker-species-id/predictions/"

        # Create the batch transform job
        # see documentation here: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker/client/create_transform_job.html
        response = aws_client.create_transform_job(
            TransformJobName=batch_job_name,
            ModelName=model_name,
            MaxConcurrentTransforms=1,
            MaxPayloadInMB=6,
            BatchStrategy='MultiRecord', # batch strategy and corresponding transform input configs
            TransformInput={
                'DataSource': {
                    'S3DataSource': {
                        'S3DataType': 'S3Prefix',
                        'S3Uri': input_data_location
                    }
                },
            'ContentType': 'image/jpeg',
            'CompressionType': 'None',
            'SplitType': 'None'
            },
            TransformOutput={
                'S3OutputPath': output_location,
                'Accept': 'application/json'
            },
            TransformResources={
                'InstanceType': "ml.g4dn.xlarge",
                'InstanceCount': 1
            } # can add optional experiment tracking configs
        )
        waiter = aws_client.get_waiter('transform_job_completed_or_stopped')

        waiter.wait(
            TransformJobName=batch_job_name,
            WaiterConfig={
                'Delay': 123,  # The amount of time in seconds to wait between attempts. Default: 60
                'MaxAttempts': 123  # The maximum number of attempts to be made. Default: 60
            }
        )
        print("Inference response:", response)

    def test_get_predictions_as_json(self):
        s3_client = boto3.client('s3',
                                 aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
                                 aws_secret_access_key=os.environ["AWS_SECRET_KEY"],
                                region_name=os.environ["AWS_REGION"]
                                 )


        inference_bucket = "treetracker-species-id"

        # List objects in the specified S3 bucket and prefix
        response = s3_client.list_objects_v2(Bucket=inference_bucket, Prefix="predictions")

        # Loop through each object and process .out files
        for obj in response.get('Contents', []):
            key = obj['Key']
            pred_file_name = os.path.splitext(key.split('/')[-1])[0]
            if key.endswith('.out'):
                # Download the .out file
                with open('../local_data/' + key.split('/')[-1], 'wb') as f:
                    s3_client.download_fileobj(inference_bucket, key, f)
                # Read and process the .out file
                with open('../local_data/' + key.split('/')[-1], 'r') as f:
                    prediction = json.loads(f.read())
                    # Do whatever processing you need with the prediction
                    with open('../local_data/' + pred_file_name + '.json', 'w') as json_file:
                        json.dump(prediction, json_file)
                        os.remove('../local_data/' + key.split('/')[-1])

        # TO DO: Write inference results to prod DB

    def test_clean_s3_buckets(self):
        # Verified this works on S3 console online
        # Clean up S3 buckets after processing
        inference_bucket = "treetracker-species-id"
        key = "daily-training.manifest"
        # Create an S3 client
        s3_client = boto3.client('s3',
                            aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
                            aws_secret_access_key=os.environ["AWS_SECRET_KEY"]
                            )
        s3_client.delete_object(Bucket=inference_bucket, Key=key)
        print("S3 buckets cleaned up")

    def test_create_validation(self):
        '''
        To Do: Test if the images are downloaded and saved in the local directory
        :return:
        '''
        local_inference_dir = '../local_data/predictions/'
        acceptance_threshold = 0.5
        species_id.create_validation_set(local_inference_dir, acceptance_threshold)
if __name__ == '__main__':
    # print (os.environ["DB_HOST"])
    unittest.main()

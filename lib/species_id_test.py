import species_id
import unittest
import psycopg2
import os
import boto3
import json
import time


# assigns environment variables for testing
import credentials


class Test(unittest.TestCase):
    def test_data_import(self):
        # read env variables DB_URL
        conn = psycopg2.connect(os.environ["DB_CONNECTION_STRING"], sslmode='require')
        org_id = '8b2628b3-733b-4962-943d-95ebea918c9d'
        start_date = '2019-03-05'
        end_date = '2024-06-08'

        haiti_topleft = (20.02535383561072, -74.47685260343907)
        haiti_bottomright = (17.405263862983954, -71.5108667436321)

        sql_filter_by_org_and_date = '''
        SELECT e.image_url 
        FROM public.trees e 
        WHERE time_created > %s AND time_created < %s 
        AND lat > %s AND lat < %s 
        AND lon > %s AND lon < %s;
        '''

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(sql_filter_by_org_and_date, (start_date,
                                                    end_date,
                                                    haiti_topleft[1],
                                                    haiti_bottomright[1],
                                                    haiti_topleft[0],
                                                    haiti_bottomright[0]
                                                 ))
        result = cursor.fetchall()
        print(result)

        cursor.close()
        conn.close()

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
        image_urls = [
            "https://example.com/images/random1.jpg",
            "https://example.com/images/random2.jpg",
            "https://example.com/images/random3.jpg",
            "https://example.com/images/random4.jpg",
            "https://example.com/images/random5.jpg",
        ]
        output_file_path = '../local_data/daily-training.manifest'

        # Open a file to write
        with open(output_file_path, 'w+') as file:
            for uri in image_urls:
                # Create a dictionary with the 'source-ref' key
                entry = {"source-ref": uri}
                # Write the JSON object as a string to the file
                file.write(json.dumps(entry) + '\n')

        print(f"Manifest file created at {output_file_path}")

        # Create an S3 client
        s3_client = boto3.client('s3',
                            aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
                            aws_secret_access_key=os.environ["AWS_SECRET_KEY"]
                            )

        dest_bucket = "treetracker-species-id"
        key = "daily-training.manifest"
        s3_client.upload_file(Filename=output_file_path, Bucket=dest_bucket, Key=key)
        print("Manifest file uploaded to S3")

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
        s3_client.delete_object(Bucket=inference_bucket, Key=key)
        print("S3 buckets cleaned up")

    def test_model_inferences(self):
        aws_client = boto3.client('sagemaker',
                            aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
                            aws_secret_access_key=os.environ["AWS_SECRET_KEY"]
                            )
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


if __name__ == '__main__':
    # print (os.environ["DB_HOST"])
    unittest.main()

import species_id
import unittest
import psycopg2
import os
import boto3
import json
import time


# assigns environment variables for testing
# this file might be moved to the root directory
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
            "https://herbarium.treetracker.org/taxa/AZADINDI/sl_2020.11.11.21.47.18_8.431009999999999_-13.22481166666667_8f390b40-6ded-45ea-bc66-157608319332_IMG_20201111_130443_4427911057078513137.jpg",
            "https://herbarium.treetracker.org/taxa/CALOCALA/ht_2021.05.26.10.47.45_18.285754728130996_-73.56429898180068_24e57e15-35c0-41f7-919d-543d72620771_IMG_20210524_071644_268577763609314580.jpg",
            "https://herbarium.treetracker.org/taxa/CATALONG/ht_2020.11.15.13.31.15_18.29337283037603_-73.55801749974489_b267cabe-7c8c-4ef5-b3bf-36c09f0053d9_img_20201111_075735_5165114301099204.jpg"
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

    def test_model_inferences(self):
        '''
        To Do: see if the result that results from this can be used in the Python script or in the DAG
        
        To Do: Automate permission writing for the manifest file so that the SageMaker job can access it- otherwise,
        after creating it you need to go into it and manually allow SageMaker to access it. Maybe already solevd with recent
        change to bucket-level permissions.


        :return:
        '''
        aws_client = boto3.client('sagemaker',
                                 aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
                                 aws_secret_access_key=os.environ["AWS_SECRET_KEY"],
                                region_name=os.environ["AWS_REGION"]
                                 )
        batch_job_name = "species-id-job-" + time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime())
        model_name = "haitibeta4"
        data_manifest_file = "daily-training.manifest"  # this should be in the same bucket as the inference bucket
        inference_bucket = "treetracker-species-id"
        input_data_location = "s3://" + os.path.join(inference_bucket, 'daily-training.manifest')
        output_location = "s3://" + os.path.join(inference_bucket)
        assert input_data_location == "s3://treetracker-species-id/daily-training.manifest"
        # Create the batch transform job
        response = aws_client.create_transform_job(
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
                'InstanceType': "ml.g4dn.xlarge",
                'InstanceCount': 1
            }
        )
        print("Inference response:", response)

    # def test_clean_s3_buckets(self):
    #     # Verified this works on S3 console online
    #     # Clean up S3 buckets after processing
    #     inference_bucket = "treetracker-species-id"
    #     key = "daily-training.manifest"
    #     # Create an S3 client
    #     s3_client = boto3.client('s3',
    #                         aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
    #                         aws_secret_access_key=os.environ["AWS_SECRET_KEY"]
    #                         )
    #     s3_client.delete_object(Bucket=inference_bucket, Key=key)
    #     s3_client.delete_object(Bucket=inference_bucket, Key=key)
    #     print("S3 buckets cleaned up")

if __name__ == '__main__':
    # print (os.environ["DB_HOST"])
    unittest.main()

import io
import requests
import psycopg2
import psycopg2.extras
import json
import urllib
import urllib3
import boto3


from sshtunnel import SSHTunnelForwarder
from sqlalchemy.orm import sessionmaker #Run pip install sqlalchemy
from sqlalchemy.engine import URL
from sqlalchemy import create_engine

import os

def load_images_to_s3(image_urls, dest_bucket="treetracker-species-id"):
    '''
    Load image URLs to S3 bucket for inference
    :param image_urls:
    :param dest_bucket:
    :return:
    '''
    s3_client = boto3.client('s3',
                             aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
                             aws_secret_access_key=os.environ["AWS_SECRET_KEY"]
                             )

    http = urllib3.PoolManager()
    # Open a file to write
    for uri in image_urls:
        # Write the JSON object as a string to the file
        urllib.request.urlopen(uri)  # Provide URL
        # stream file to s3 bucket
        s3_client.upload_fileobj(http.request('GET', uri, preload_content=False),
                                 dest_bucket,
                                 "inference" + "/" + uri.split('/')[-1],
                                 )

    return True


def get_image_urls(topleft, bottomright, startdate, enddate):
    '''
    Connect to the remote database and fetch the image URLs for the given bounding box and time range
    :param topleft: top left GPS coordinate of the bounding box
    :param bottomright: bottom right GPS Coordinate of the bounding box
    :param startdate: starting period for inference
    :param enddate: ending period for inference
    :return:
    '''
    results_list = None
    try:
        with SSHTunnelForwarder((os.environ["REMOTE_HOST"], int(os.environ["REMOTE_PORT"])),
                                ssh_username=os.environ["REMOTE_USER"],
                                ssh_password=os.environ["REMOTE_PASSWORD"],
                                ssh_pkey=os.environ["REMOTE_KEY"],
                                remote_bind_address=(os.environ["DB_HOST"], int(os.environ["DB_PORT"]))
                                ) as server:
            server.start()
            print("Server connected via SSH")

            # connect to PostgreSQL
            local_port = str(server.local_bind_port)
            print(local_port)
            create_engine_url = URL.create("postgresql",
                                           username=os.environ["DB_USER"],
                                           password=os.environ["DB_PASSWORD"],
                                           host="localhost",
                                           port=local_port,
                                           database=os.environ["DB_NAME"])
            engine = create_engine(create_engine_url)
            Session = sessionmaker(bind=engine)
            session = Session()

            sql_filter_by_org_and_date = '''
                SELECT e.image_url
                FROM public.trees e
                WHERE time_created > '%s' AND time_created < '%s'
                AND lat > %s AND lat < %s
                AND lon > %s AND lon < %s;
                ''' % (startdate,
                       enddate,
                       bottomright[0],
                       topleft[0],
                       topleft[1],
                       bottomright[1]
                       )

            result = session.execute(sql_filter_by_org_and_date)
            result_list = result.fetchall()
            if len(result_list) > 0:
                results_list = ([url[0] for url in result_list])
        session.close_all()
        server.close()
        return results_list
    except:
        session.rollback()
        return results_list
        raise EnvironmentError("Connection failed")
    finally:
        return results_list
    return results_list

def generate_input_manifest(image_urls, dest_bucket):
    '''
    After fetching the image URLs from the database, generate the input manifest file and upload to S3 for the
    batch transform job to have an input

    :param image_urls:
    :return:
    '''

    # Path where the manifest file will be saved
    output_file_path = 'daily-training.manifest'

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
    s3.upload_file(Filename=output_file_path, Bucket=dest_bucket, Key=output_file_path)
    print("Manifest file uploaded to S3")

def create_validation_set(local_inference_dir, acceptance_threshold=0.7):
    '''

    Create a set of images based on the inference results for a human annotator to review.

    @param save_dir: Local directory to save the images and inferences to.
    @param acceptance_threshold: Probability above which we will consider the model "confidence" high enough.
    Needs to be >= 0.5.

    :return:
    '''
    num_imgs_passing_threshold = 0
    inference_files = 0
    for json_response in os.listdir(local_inference_dir):
        if not json_response.endswith(".json"):
            continue
        else:
            inference_files += 1
        image_url = "https://treetracker-production-images.s3.eu-central-1.amazonaws.com/" + os.path.splitext(json_response)[0]
        inference_result = json.load(open(os.path.join(local_inference_dir, json_response)))
        for species, probs in inference_result.items():
            if float(probs) > acceptance_threshold:
                # Download the image from the URL
                try:
                    image = requests.get(image_url)
                    image = io.BytesIO(image.content)
                    # Save the image to the local directory
                    basename = os.path.splitext(os.path.splitext(json_response)[0])[0]
                    ext = os.path.splitext(os.path.splitext(json_response)[0])[1]
                    imname =  basename + "_" + species + ext
                    with open(os.path.join(local_inference_dir, "validation", imname), 'wb') as f:
                        f.write(image.read())
                    num_imgs_passing_threshold += 1
                except:
                    print(f"Failed to download {image_url}")
                    continue
    print(f"Verification set created with {num_imgs_passing_threshold} images out of {inference_files} inference files")

def clear_s3_after_inference(inference_bucket="treetracker-species-id"):
    '''
    Clean up S3 buckets after processing
    :param inference_bucket:
    :return:
    '''
    # Create an S3 client
    s3_client = boto3.client('s3',
                             aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
                             aws_secret_access_key=os.environ["AWS_SECRET_KEY"]
                             )
    # List objects in the specified S3 bucket and prefix
    response = s3_client.list_objects_v2(Bucket=inference_bucket, Prefix="predictions")

    # Loop through each object and process .out files
    for obj in response.get('Contents', []):
        key = obj['Key']
        # TO DO: Write inference results to prod DB
        s3_client.delete_object(bucket=inference_bucket, key=key)

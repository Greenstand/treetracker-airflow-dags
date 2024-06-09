import io
import requests
import psycopg2
import psycopg2.extras

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


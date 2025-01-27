import boto3
import os
from datetime import datetime

# Initialize the S3 client
s3 = boto3.client('s3')

# Set the destination bucket name
RAW_BUCKET = 'your-raw-bucket-name'

def lambda_handler(event, context):
    # Extract the information about the uploaded file from the event
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']
    parts = source_key.split('/')
    subfolder = parts[-2]
    # Get the current date in YYYY-MM-DD format
    current_date = datetime.now().strftime('%Y-%m-%d')

    # Define the new key (file name) with the current date
    filename = os.path.basename(source_key)
    destination_key = f"raw/{subfolder}/{current_date}/{filename}"  # Add current date as the folder
    archive_key = f"archive/{subfolder}/{current_date}/{filename}"
    try:
        # Copy the file to the raw bucket with the new key
        s3.copy_object(
            Bucket=source_bucket,
            Key=destination_key,
            CopySource={'Bucket': source_bucket, 'Key': source_key}
        )

        s3.copy_object(
            Bucket=source_bucket,
            Key=archive_key,
            CopySource={'Bucket': source_bucket, 'Key': source_key}
        )
        
        # Optionally, delete the original file from the landing bucket
        s3.delete_object(Bucket=source_bucket, Key=source_key)
        
        return {
            'statusCode': 200,
            'body': f"File moved to {source_bucket}/{destination_key}"
        }

    except Exception as e:
        print(f"Error moving file: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }

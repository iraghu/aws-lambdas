import json
import boto3
from datetime import datetime
def lambda_handler(event, context):
    # TODO implement
    # get s3 client from boto to connect with aws s3
    s3_client = boto3.client('s3')

    #get bucket and path attributes from the event
    landing_bucket = event['landingbucket']
    landingpath=event['landingpath']

    # Check if there are files in the landing path
    response = s3_client.list_objects_v2(Bucket=landing_bucket,Prefix=landingpath)

    # Get the current date in YYYY-MM-DD format
    current_date = datetime.now().strftime('%Y-%m-%d')

    #Read the file from s3
    if 'Contents' in response and len(response['Contents']) > 0:
        for obj in response['Contents']:
            keyname=obj['Key']
            parts = keyname.split('/')
            subfolder = parts[-2]
            filename=parts[-1]
            destination_key = f"raw/{subfolder}/{current_date}/{filename}"
            archive_key = f"archive/{subfolder}/{current_date}/{filename}"
            # moving  files from landing to raw path
            s3_client.copy_object(Bucket=landing_bucket,Key=destination_key,
            CopySource={'Bucket': landing_bucket, 'Key': keyname})

            # archive the  files from landing to archive
            s3_client.copy_object(Bucket=landing_bucket,Key=archive_key,
            CopySource={'Bucket': landing_bucket, 'Key': keyname})

            # Optionally, delete the original file from the landing bucket
            s3_client.delete_object(Bucket=landing_bucket, Key=keyname)

        return {'filesExist': True}
    else:
        return {'filesExist': False}

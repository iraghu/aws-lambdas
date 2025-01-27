import json
import boto3
from datetime import datetime
def lambda_handler(event, context):
    # TODO implement
    s3_client = boto3.client('s3')
    landing_bucket = event['landingbucket']
    landingpath=event['landingpath']
    # Check if there are files in the landing path
    response = s3_client.list_objects_v2(Bucket=landing_bucket,Prefix=landingpath)
    current_date = datetime.now().strftime('%Y-%m-%d')
    if 'Contents' in response and len(response['Contents']) > 0:
        for obj in response['Contents']:
            keyname=obj['Key']
            parts = keyname.split('/')
            subfolder = parts[-2]
            filename=parts[-1]
            destination_key = f"archive/{subfolder}/{current_date}/{filename}"
            s3_client.copy_object(Bucket=landing_bucket,Key=destination_key,
            CopySource={'Bucket': landing_bucket, 'Key': keyname})
            s3_client.delete_object(Bucket=landing_bucket, Key=keyname)
     

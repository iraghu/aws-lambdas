import json
import boto3
import pandas as pd
from io import BytesIO
from io import StringIO
import io
import random
from datetime import datetime

def lambda_handler(event, context):
    # TODO implement
    columns=event.get('columnstring')
    column_list = columns.split(',')
    print(column_list)
    tablename=event.get('tablename')
    refinedzone=event.get('refinedzone')
    processedzone=event.get('processedzone')
    outputpath=f'{processedzone}/{tablename}'
    inputpath=f'{refinedzone}/{tablename}/'
    datepath =datetime.now().strftime('%Y-%m-%d')
    s3_client=boto3.client('s3',region_name='ap-south-1')
    response = s3_client.list_objects_v2(Bucket='gluerawbucket',Prefix=inputpath)
    dfs=[]
    for obj in response['Contents']:
        if "parquet" in obj['Key']:
         print(f"Object: {obj['Key']}")
         filecontent = s3_client.get_object(Bucket='gluerawbucket', Key=obj['Key'])
         parquet_bytes = filecontent['Body'].read()
         bytes_io = io.BytesIO(parquet_bytes)
         df = pd.read_parquet(bytes_io)
         df['current_date'] = datepath
         dfs.append(df)
    randomnum=random.randint(1000,9999)
    final_df = pd.concat(dfs, ignore_index=True)
    selectedDF=final_df[column_list]
    parquet_buffer = BytesIO()
    selectedDF.to_parquet(parquet_buffer,engine='pyarrow', index=False)
    parquet_buffer.seek(0)
    s3_client.put_object(Bucket='gluerawbucket', Key=f'{outputpath}/{datepath}/{randomnum}.parquet', Body=parquet_buffer)
    return event
    

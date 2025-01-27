import json
import boto3
import pandas as pd
from io import BytesIO
from io import StringIO
import random
from datetime import datetime
def lambda_handler(event, context):
    # TODO implement
    tablename=event.get('tablename')
    outputpath=event.get('outputpath')
    inputpath=event.get('inputpath')
    print("check table name"+tablename)
    print("check input"+inputpath)
    datepath =datetime.now().strftime('%Y-%m-%d')
    s3_client=boto3.client('s3',region_name='ap-south-1')
    response = s3_client.list_objects_v2(Bucket='gluerawbucket',Prefix=inputpath)
    dfs=[]
    for obj in response['Contents']:
        if "json" in obj['Key']:
            print(f"Object: {obj['Key']}")
            filecontent = s3_client.get_object(Bucket='gluerawbucket', Key=obj['Key'])
            json_data = filecontent['Body'].read().decode('utf-8')
            parsed_json = json.loads(json_data)
            df = pd.json_normalize(parsed_json)
            df['current_date'] = datetime.now().strftime('%Y-%m-%d')
            dfs.append(df)
    randomnum=random.randint(1000,9999)
    final_df = pd.concat(dfs, ignore_index=True)
    parquet_buffer = BytesIO()
    final_df.to_parquet(parquet_buffer,engine='pyarrow', index=False)
    parquet_buffer.seek(0)
    s3_client.put_object(Bucket='gluerawbucket', Key=f'{outputpath}/{tablename}/{datepath}/{randomnum}.parquet', Body=parquet_buffer) 
    return event     
    

    

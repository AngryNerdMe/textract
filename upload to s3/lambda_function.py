import json
import base64
import boto3
import time

BUCKET_NAME = 'textract-mumbai'

def lambda_handler(event, context):
    body = json.loads(event['body'])   
    content = body['Content']
    file_name = body['FileName']
    final_file_name = file_name +"_"+str(int(round(time.time() * 1000)))
    file_type = body['FileType']
    file_content = base64.b64decode(content)
    file_path = 'Raw_Data/Avlight/out_'+final_file_name+"."+file_type
    s3 = boto3.client('s3')
    try:
        s3_response = s3.put_object(Bucket=BUCKET_NAME, Key=file_path, Body=file_content)
    except Exception as e:
        raise IOError(e)
    return {
        'statusCode': 200,
        'headers': {
                    'Access-Control-Allow-Header': '*',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps({
                "FileName": final_file_name,
                "FilePath": file_path
                })
    }
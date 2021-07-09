import json
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Key
from botocore.errorfactory import ClientError

class fakefloat(float):
    def __init__(self, value):
        self._value = value
    def __repr__(self):
        return str(self._value)

def defaultencode(o):
    if isinstance(o, Decimal):
        # Subclass float with custom repr?
        return fakefloat(o)
    raise TypeError(repr(o) + " is not JSON serializable")
    
def key_exists(mykey, mybucket):
    s3 = boto3.resource('s3').Bucket(mybucket)
    try:
        s3.Object(mykey).get()
        return True
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            print('NoSuchKey')
        return False
    pass
  
def lambda_handler(event, context):
    # TODO implement
    body = json.loads(event['body'])   
    file = body['filename']
    filetype = body['filetype']
    
    if file == '':
        return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Header': '*',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                'body': json.dumps({
                    'Status' : 'Error',
                    'Message' : 'No File Name'
                })
        }
    
    TABLE_NAME = "TextRactProduction" 

    # Creating the DynamoDB Table Resource
    dynamodb = boto3.resource('dynamodb', region_name="ap-south-1")
    table = dynamodb.Table(TABLE_NAME)
    
    response = table.get_item(
    Key={
        'FileName': file
    }
    )
    
    if 'Item' in response:
        final_json = response['Item']
        final_json['InvoiceTotal'] = final_json['InvoiceTotal[INR]']
        final_json['SupplierName'] = final_json['Supplier Name']
        final_json['GSTIN'] = final_json['GSTIN/UIN']
        
        msg= ''
        for key in final_json:
            if(final_json[key] == "N/A"):
                msg = 'Some value is invalid, need manual processing'
                break
            else:
                msg= 'Successfully uploaded to database'
        final_json['Message']= msg
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': json.dumps(final_json, default=defaultencode)
        }

    else:
        if key_exists('Raw_Data/Avlight/out_'+file+'.'+filetype,'textract-mumbai-production'):
            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Header': '*',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                'body': json.dumps({
                    'Status' : 'In Progress',
                    'Message' : 'File is being processed. Please wait for a while and try again'
                })
            } 
        else:
            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Header': '*',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                'body': json.dumps({
                    'Status' : 'Error',
                    'Message' : 'Could not be uploaded, need manual processing'
                })
            } 

    












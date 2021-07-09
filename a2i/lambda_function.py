import json
import boto3
import re
from pprint import pprint

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')

s3BucketName = 'textract-mumbai-production'

SUCCESS_PREFIX = "Output/Avlight/Successful/"


def lambda_handler(event, context):
 bucket = event['Records'][0]['s3']['bucket']['name']
    # print(bucket)
 json_file = event['Records'][0]['s3']['object']['key']
    # print(str(json_file))
 json_object = s3_client.get_object(Bucket=bucket,Key=json_file)
    # print(json_object)
 jsonFile = json_object['Body'].read()
    # print(jsonFile)
 human_output = json.loads(jsonFile)
 #for i in human_output: # Iterating through the json 
  #print(i) 
 
 output_shown = human_output['inputContent']['initialValue']
 #for i in output_shown: # Iterating through the json 
	 #print(i) 
     
 for i in range(len(output_shown['lines'])):
 #print("before edit")
  #print(output_shown['lines'][i]['5_Quantity ']) 
  #print("After edit")
  j = 'quantity'+str(i)
  output_shown['lines'][i]['5_Quantity '] = human_output['humanAnswers'][0]['answerContent'][j]
  #print(output_shown['lines'][i]['5_Quantity '])
  k = 'amount'+str(i)
  output_shown['lines'][i]['8_Basic Value '] = human_output['humanAnswers'][0]['answerContent'][k]
  l = 'item'+str(i)
  output_shown['lines'][i]['3_Item Code '] = human_output['humanAnswers'][0]['answerContent'][l]  
  m = 'csgst'+str(i)
  output_shown['lines'][i]['13_CGST [INR] Amount '] = human_output['humanAnswers'][0]['answerContent'][m]
  n = 'sgst'+str(i)
  output_shown['lines'][i]['14_SGST Rate '] = human_output['humanAnswers'][0]['answerContent'][n]
  
 output_shown['InvoiceDate'] = human_output['humanAnswers'][0]['answerContent']['date']
 output_shown['GSTIN/UIN'] = human_output['humanAnswers'][0]['answerContent']['gst']
 output_shown['InvoiceNo'] = human_output['humanAnswers'][0]['answerContent']['id']
 output_shown['Supplier Name'] = human_output['humanAnswers'][0]['answerContent']['invsupp']
 output_shown['Partcount'] = human_output['humanAnswers'][0]['answerContent']['partcount']
 output_shown['Amounts']['BasicValue'] = human_output['humanAnswers'][0]['answerContent']['total']
 output_shown['Totalcount'] = human_output['humanAnswers'][0]['answerContent']['totalcount']
 output_shown['totalgst'] = human_output['humanAnswers'][0]['answerContent']['totalgst']
 print(output_shown)    
  
 fileName = output_shown['filename']
 s3object = s3.Object(s3BucketName, SUCCESS_PREFIX + fileName +'.json')
 print('Processed :{}/{}{}{} '.format(s3BucketName ,SUCCESS_PREFIX,fileName,'.json'))
 print('File Flagged {}'.format(fileName))
        
 response  = s3object.put(Body=(bytes(json.dumps(output_shown).encode('UTF-8'))))
 print(response)
import boto3
import os
import sys
import uuid
from urllib.parse import unquote_plus
from PIL import Image
from PIL.Image import core as _imaging
#old import after this
from pdf2image import convert_from_path,convert_from_bytes
from PIL import ImageFilter,ImageFile
import img2pdf
from img2pdf import convert
import image_cleaner as imgc
#import img2pdf
#import parse
import argparse
import io
import os
import boto3
import math
import json
#end of imports

 
OUTPUT_BUCKET = "mate-mumbai-prod"
PREFIX = "Raw_Data/Avlight/"


s3BucketName = ''

 

 

mySession = boto3.session.Session()
awsRegion = mySession.region_name
s3 = boto3.resource('s3')

def img2bytes(image):
    buf = io.BytesIO()
    image.save(buf, format='JPEG')
    byte_im = buf.getvalue()
    return byte_im
    

 

    
    # filename = "CKM_EB_20200825_00003.pdf"
    

 

 


def lambda_handler(event, context):
    # TODO implement
    
    
    file = event['Records'][0]['s3']['object']['key']
    s3BucketName  = event['Records'][0]['s3']['bucket']['name']
    fileName = file.split('/')[-1][:-4]
    print(fileName)
    #print(subprocess.check_output(['pip','install','Pillow']) )
    print('Processing File : {}'.format(file))
    s3_client = boto3.client('s3')
    file = s3_client.get_object(Bucket=s3BucketName,Key=file)
   
    images = convert_from_bytes(file['Body'].read())        
    print("Processing Started")
    print("PDF has {} pages".format(len(images)))
    
    processed_images=[]
    print("Cleaning Images with filters")
    for imgnum,image in enumerate(images):
        cleaned_image = imgc.cleanImage(image)   
        processed_images.append(cleaned_image)
    
    print("Converting images to PDF Bytes")
    pdfout = img2pdf.convert(*list(map(img2bytes, processed_images)))
    
    boto3.resource('s3',region_name='ap-south-1').Object(s3BucketName,'preprocessed/Avlight/'+fileName+'.pdf').put(Body=pdfout)

    ''' show jupyter code try again
    s3object = s3.Object(s3BucketName, 'preprocessed/Avlight/' + fileName+'.pdf'))
    print('Processed : ',file.replace('Raw Data','preprocessed'))
    s3object.put(
        Body=(bytes(json.dumps(pdfout).encode('UTF-8')))
    )
    '''
    print("Processing Completed")
    #print(formArr)
        
    
    
        
   
    
    
        
    #print(awsRegion)
    
    return {
        'statusCode': 200,
        #'body': json.dumps(formArr)
    }
 
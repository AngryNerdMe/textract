import boto3
from trp import Document 
import time
import re
import logging
import json
import time
import pprint
import uuid

OUTPUT_BUCKET = "textract-mumbai-production"
FLAGGED_PREFIX = "Output/Avlight/Flagged/"
SUCCESS_PREFIX = "Output/Avlight/Successful/"
DOCUMENT_THRES = 92.5 
INDV_THRES = 90
s3BucketName = '' 
confidence1 = ''
#fileName = ''
logging.basicConfig(level=print,format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

mySession = boto3.session.Session()
awsRegion = mySession.region_name

# Amazon S3 client
s3 = boto3.resource('s3')


# Amazon Textract client
textract = boto3.client('textract')
#Function to remove non numeric characters from string

a2i_runtime_client = boto3.client('sagemaker-a2i-runtime')

### import pprint

# Pretty print setup
pp = pprint.PrettyPrinter(indent=2)

# Function to pretty-print AWS SDK responses
def print_response(response):
    if 'ResponseMetadata' in response:
        del response['ResponseMetadata'] 
    pp.pprint(response)

    


def convert2num(string):
    return re.sub("[^\d\.]", "", string)

#To get Index by String in Tuple of Confidence
def get_index(string,li):
    #temp = list(li.items())  
    return  [idx for idx, key in enumerate(li) if key[0] == string][0] 
    
# Function Start and Monitor running Jobs
def startJob(s3BucketName, objectName):
    response = None
    response = textract.start_document_analysis(
    DocumentLocation={
        'S3Object': {
            'Bucket': s3BucketName,
            'Name': objectName
        }
    },  FeatureTypes=[
        'TABLES'
    ],)
    # print('start job res'+str(response))
    

    
    return response["JobId"]

def isJobComplete(jobId):
    response = textract.get_document_analysis(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while(status == "IN_PROGRESS"):
        time.sleep(5)
        response = textract.get_document_analysis(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status

def getJobResults(jobId):

    pages = []
    response = textract.get_document_analysis(JobId=jobId)
    print('get job res '+str(response))
    print(str(response['Blocks']))
    
    le = []
    for i in response['Blocks'] :
        if i['BlockType'] == "TABLE":
            le.append(i)
    
    print('from le' + str(le))
    
    for i in le:
        if(i['Geometry']['BoundingBox']['Height']>0.9):
            confidence1 = (i['Confidence'])
            print(confidence1)    
            
    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']

    while(nextToken):
        response = textract.get_document_analysis(JobId=jobId, NextToken=nextToken)

        pages.append(response)
        print("Resultset page recieved: {}".format(len(pages)))
        nextToken = None
        if('NextToken' in response):
            nextToken = response['NextToken']
    
    pages.append(confidence1)
    return pages

#Checks and validate following:
#   ->Total Amount , Basic Amount, CGSTOutput ,SGSTOutput are present
#   ->Total = Basic Amount + CGSTOutput + SGSTOutput

#### function for issue path



def a2i_caller(document,file,s3BucketName,flag):   
    if flag == False:
        print("Calling A2I.....") 
        
        humanLoopName = str(uuid.uuid4())                          
        
        #str(int(round(time.time() * 1000)))          

    
        #for k,v in invoices.items():
            #print(invoices)
        #file['filename']=fileName
        file['invid']=file['InvoiceNo']
        file['invdt']=file['InvoiceDate']
        file['gst'] = file['GSTIN/UIN'] 
        file['amount'] = file['Amounts']['BasicValue']
        file['invsupp'] = file['Supplier Name']
        file['totalgst'] = (file['Amounts']['CGSTOutput'] + file['Amounts']['SGSTOutput'])
        
        totalcount = 0
        icount = 0
        j = 0
        for i in file["lines"]:
            i['name']= i['3_Item Code ']
            i['quantity'] = i['5_Quantity '] 
            totalcount = totalcount + i['5_Quantity ']
            icount = icount + 1
            i['amount'] = i['8_Basic Value ']  
            try:
                i['cgst'] = i['13_CGST [INR] Amount '] 
            except:
                i['cgst'] = ''
            try:
               i['sgst'] = i['15_[INR] Amount ']
            except KeyError:
               i['sgst'] = ''
                
            
            
        file['Totalcount'] = totalcount
        file['Partcount'] = icount
        #for i in Items:
        print("After edit - "+ str(file)) 
        
        Items=[]
        for i in file["lines"]:
            Items.append(i)
        #for i in Items:
        print(Items) 
        
        
            
        response = a2i_runtime_client.start_human_loop(
                HumanLoopName=humanLoopName,
                FlowDefinitionArn="arn:aws:sagemaker:ap-south-1:122080215404:flow-definition/fd-textract-bef59902-cbd8-473d-b625-ec42c0bdb929",
                HumanLoopInput={
                    "InputContent": json.dumps({
                                "initialValue": file,
                                "items": Items,
                                "bucket": s3BucketName, #change-to-bucket-of-preprocessed-doc
                                "document":document #change-to-path-of-preprocessed-doc
                                })
                }
            )
        print(response)    
        print_response(response)
    
        
          
def check_integrity(s3BucketName,document,file,check_conf,table_conf):                   
    flag = True
                          

    ''' if len(file['Amounts']) == 6:
        print('All amounts Present')
    else:
        logging.warning('All amounts not Present')
        flag = False'''
      
    # try:
    #     print("First Try Block")
    #     if(file['Amounts']['BasicValue']+file['Amounts']['CGSTOutput']+file['Amounts']['SGSTOutput']) == file['Amounts']['InvoiceTotal[INR]']:
    #         print('Total Amount is correct')
    #     else:
    #         logging.error('Total Amount is incorrect\n Flagging the File for manual processing')
    #         flag= False
    #         #a2i_caller(document,file,s3BucketName,flag)                                        #CHANGES.........
            
    # except Exception as e:
    #     print("First try exception")
    #     logging.warning('Amount is Missing\n Total Amount cannot be calculated \n Flagging the File for manual processing',exc_info=True)
    #     flag= False
    #     #a2i_caller(document,file,s3BucketName,flag)                                          #CHANGES.........
        
    try:
 
        def get_conf(value):
            try:
                if(value == 'InvoiceNo'):
                    return check_conf[get_index(value,li=check_conf)+2][1]
                else:
                    return check_conf[get_index(value,li=check_conf)+1][1]
            except:
                return '0'

        keys = ['GSTIN/UIN','Date','InvoiceNo','CGSTOutput','SGSTOutput','BasicValue','InvoiceTotal[INR]','TotalAmount']
    
        conf = {}
        for i in keys:
            conf[i] = get_conf(i)
        
        conf['Supplier Name'] = check_conf[11][1]
        conf['TotalGST'] = (int(conf['CGSTOutput'])+int(conf['SGSTOutput']))//2
        print(conf)
        file['confidence'] = conf
        
        
        gst_conf = check_conf[get_index('GSTIN/UIN',li=check_conf)+1][1]
        date = check_conf[get_index('Date',li=check_conf)+1][1]
        supplier_name = check_conf[11][1]
        inv_no = check_conf[get_index('InvoiceNo',li=check_conf)+2][1]
        
        document_conf = (gst_conf+date+supplier_name+inv_no+table_conf)/5
        print('Document Confidence :'+str(document_conf))
        
        if document_conf < DOCUMENT_THRES:
            print('Flagging for manual processing ... \n Document Confidence less than threshold')
            flag= False
            #a2i_caller(document,file,s3BucketName,flag)                    #CHANGES.........
            
            
        # # RJ
        # cgst = check_conf[get_index('CGSTOutput',li=check_conf)+1][1]
        # sgst = check_conf[get_index('SGSTOutput',li=check_conf)+1][1]
        # basicValue = check_conf[get_index('BasicValue',li=check_conf)+1][1]
        # invoiceTotal = check_conf[get_index('InvoiceTotal[INR]',li=check_conf)+1][1]
        # # gstin/uin = check_conf[get_index('GSTIN/UIN',li=check_conf)+1][1]
        # # invoiceNo = check_conf[get_index('InvoiceNo',li=check_conf)+1][1]
        # total = (cgst+sgst)//2


        # print('GSTIN Confidence :'+str(gst_conf))
        # print('Date Confidence :'+str(date))
        # print('Supplier Name Confidence :'+str(supplier_name))
        # print('Invoice No Confidence :'+str(inv_no))
        # print('Item Level Confidence :'+str(table_conf))
        # # RJ
        # print('CGST Confidence :'+str(cgst))
        # print('SGST Confidence :'+str(sgst))
        # print('basicValue Confidence :'+str(basicValue))
        # print('invoiceTotal Confidence :'+str(invoiceTotal))
        # print('total Confidence :'+str(total))

        # conf = {}
        # conf['GSTIN'] = str(gst_conf)
        # conf['Date'] = str(date)
        # conf['SupplierName'] = str(supplier_name)
        # conf['InvoiceNo'] = str(inv_no)
        # conf['cgst'] = str(cgst)
        # conf['sgst'] = str(sgst)
        # conf['basicValue'] = str(basicValue)
        # conf['invoiceTotal'] = str(invoiceTotal)
        # conf['total'] = str(total)
        

        # file['conf'] = {}
        # for i,j in enumerate(conf):
        #     file['conf'][j] = str(conf[j])

        



       
        
        if gst_conf < INDV_THRES:
            #print('Flagging for manual processing ...')
            flag= False
            #a2i_caller(document,file,s3BucketName,flag)
        if date < INDV_THRES:
            #print('Flagging for manual processing ...')
            flag= False
            #a2i_caller(document,file,s3BucketName,flag)
        if supplier_name < INDV_THRES:
            #print('Flagging for manual processing ...')
            flag= False
           # a2i_caller(document,file,s3BucketName,flag)
        if inv_no < INDV_THRES:
            #print('Flagging for manual processing ...')
            flag= False
           # a2i_caller(document,file,s3BucketName,flag)
        if table_conf < INDV_THRES:
            #print('Flagging for manual processing ...')
            flag= False
            
        if flag == False :
            a2i_caller(document,file,s3BucketName,flag)
        
        return True
        
            
        
    except Exception as e:
        logging.warning('Some Index is missing \n Flagging for manual Process',exc_info=True)
        flag= False
        a2i_caller(document,file,s3BucketName,flag)                            #CHANGES.........



def lambda_handler(event, context):
    # TODO implement
    
    print(event)
    
    file = event['Records'][0]['s3']['object']['key']                   #location same as that of document
    s3BucketName  = event['Records'][0]['s3']['bucket']['name']
    fileName = file.split('/')[-1][4:-4]
    document = event['Records'][0]['s3']['object']['key']                #CHANGES...
    print('Processing File : {}'.format(file))

    
    # start the job and poll for completion.
    #print('File\t',file)
    
    jobId = startJob(s3BucketName, file)
    print("Started job with id: {}".format(jobId))
    if(isJobComplete(jobId)):
        response = getJobResults(jobId)
        confidence1 = response[-1]
        print("from lambda_handler "+str(confidence1))
        response = response[ :-1]
   
    # Format the response with helper class from trp.py
    # https://github.com/aws-samples/amazon-textract-code-samples/blob/master/python/trp.py
    resp_trp = Document(response)
    
    for page in resp_trp.pages:
        tArr = [] #Main table List (2 tables for this template , only the first table of intrest)
        for table in page.tables:
            stArr =[] #Sub Table List
            for r, row in enumerate(table.rows):
                rArr = [] #Row List
                for c, cell in enumerate(row.cells):
                    rArr.append(cell.text)
                stArr.append(rArr)
            tArr.append(stArr)
    
    try:
        
        if len(tArr[0][0]) < 18:
            raise Exception("Some Columns are missing, Cannot Parse Item Level Information")
            
        for i in range(1,len(tArr[0])):
            if(re.match('\d',tArr[0][i][0])):
                print('Ok')
            else:
                print('Problem at Sr. No :',i,'\nValue : ',tArr[0][i][0])
                tArr[0][i][0] = str(i)
                print('Corrected it :',i)
    except Exception as e:
        logging.error('Column Missing.\nCannot Process Further\nSkipping to Next',exc_info=True)
        print('Column Missing.\nCannot Process Further\nSkipping to Next')
        
        #manual_files.append(file)
        
    # Process Lines from Raw Text
# Add each line to line list lArr. If word starts with : then append to previous line

    check_conf = []
    lArr = []
    for page in resp_trp.pages:
        for lines in page.lines:
            line=''
            l_conf = []
            for w, word in enumerate(lines.words):
                 #print(word.text + '<====>'+str(word.confidence))
                 check_conf.append((word.text,word.confidence))
                 line += word.text
            if (line.strip()[0] == ':'):
                lArr[-1] += line
            else:
                lArr.append(line)
            #print(line + '<====>'+str(lines.confidence))
            check_conf.append((line,lines.confidence))
    print('larr-')
    print(lArr)           
    formArr={'Supplier Name':'AVLIGHT AUTOMOTIVES PVT. LTD.'}
    for line in lArr:
        if ":" in line:
            keyval = line.split(':', 1)
            formArr[keyval[0]]=keyval[1]
        #    print(lArr[i],':',float(lArr[i+1].replace(',' , '')))
    
    
    # Get key Value Pairs (: Delimited)
# Process each item in line list to delimit by : and store as dictonary formArr

    lkpVals = ['BasicValue', 'ToolAmort', 'CGSTOutput','SGSTOutput','LessToolAmort','InvoiceTotal[INR]']
    formArr['Amounts']={}
    for i,line in enumerate(lArr):
        if line in lkpVals:
            '''if line == 'InvoiceTotal[INR]':
                formArr['Amounts'][lArr[i]] =float(convert2num(lArr[i+3]))
            else :'''
            formArr['Amounts'][lArr[i]] =float(convert2num(lArr[i+1]))
    #        print(lArr[i],':',float(lArr[i+1].replace(',' , '')))
    
     
    # Table Process
# Process Amount fields from first table. The column index is hardcoded
# Store each row as dictonary with key as the text from first row.
    AmountFields = [ 'Quantity ',   'RATE [INR] ',   'Basic Value ',   'Tool Amort Rate ',   'Tool Amort Val ',   'Taxable Value ',   'Rate ',   'Amount ']
    AmountFieldsIdx = [ 5,7,8,9,10,11,12,13,14,15,16,17]
    header = tArr[0][0]
    table = []
    for i in range(1,len(tArr[0])):
        try:
            row={}
            for j in range(len(tArr[0][i])):
                if j==0:
                    row[tArr[0][0][j]] = i
                else:
                    if j in AmountFieldsIdx:
                        row[str(j)+'_'+str(tArr[0][0][j])] = float(convert2num(tArr[0][i][j]))
                    else:
                        row[str(j)+'_'+str(tArr[0][0][j])] = tArr[0][i][j]
            table.append(row)
        except Exception as e:
            logging.warning('Unecessary rows detected.')
            break

    # Create Json Formatted string
    #import json 
    formArr['confidence1'] = confidence1
    formArr['lines'] = table
    formArr['filename'] = fileName
    invoice_str = json.dumps(formArr)  
    
    # print(invoice_str)
    
    #print(formArr['Amounts']['BasicValue'])
    print("File :"+invoice_str)
    print(check_conf)
    
    if not check_integrity(s3BucketName,file,formArr,check_conf,resp_trp.pages[0].tables[0].confidence):
        s3object = s3.Object(s3BucketName, FLAGGED_PREFIX + fileName +'.json')
        print('Processed :{}/{}{}{} '.format(s3BucketName ,FLAGGED_PREFIX,fileName,'.json'))
        print('File Flagged {}'.format(fileName))
        
        s3object.put(
            Body=(bytes(json.dumps(formArr).encode('UTF-8')))
        )
    else:
        s3object = s3.Object(s3BucketName, SUCCESS_PREFIX  + fileName +'.json')
        print('Processed :{}/{}{}{} '.format(s3BucketName ,SUCCESS_PREFIX,fileName,'.json'))
        print('File Successfully processed {}'.format(fileName))
        
        s3object.put(
            Body=(bytes(json.dumps(formArr).encode('UTF-8')))
        )
    
        #print('Integrity:',file)
        #manual_files.append(file)
        #RJ
    '''
    # Output File to S3
    s3object = s3.Object(s3BucketName, file.replace('preprocessed','Final')+'.json')
    print('Processed : ',file.replace('preprocessed','Final')+'.json')
    s3object.put(
        Body=(bytes(json.dumps(formArr).encode('UTF-8')))
    )
    '''
    #print(formArr)
    
    
        
    #print(awsRegion)
    return {
        'statusCode': 200,
        'body': json.dumps(formArr)
    }


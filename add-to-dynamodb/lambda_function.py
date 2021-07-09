import json
import boto3
import datetime
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    # print(event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    # print(bucket)
    json_file = event['Records'][0]['s3']['object']['key']
    # print(str(json_file))
    json_object = s3_client.get_object(Bucket=bucket,Key=json_file)
    # print(json_object)
    jsonFile = json_object['Body'].read()
    # print(jsonFile)
    jsonDict = json.loads(jsonFile)
    
    print(jsonDict['confidence'])
    confidence1 = jsonDict['confidence1']
    print(jsonDict['confidence1'])
    
    #logic to take out the name of the folder
    folders = json_file.split("/")
    FileName = folders[3].split(".")[0]
    status = folders[2]
    #end
    
    # logic starts-  for finding the number of parts
    numberOfParts=0
    lines = []
    bigdata = []
    
    lines = (jsonDict['lines'])
    for line in (lines):
        data = {}
        numberOfParts+=1
        data['itemCode'] = line["3_Item Code "]
        data['quantity'] = line["5_Quantity "]
        data['basicValue'] = line["8_Basic Value "]
        data['cgst'] = line["13_CGST [INR] Amount "]
        data['sgst'] = line["15_[INR] Amount "]
        bigdata.append(data)
        
    # logic ends
    
    # logic for calulating the totalGST, InvoiceTotal[INR], BasicValue, InvoiceTotal- starts
    amounts = []
    amounts = jsonDict['Amounts']
    SGSTOutput = amounts['SGSTOutput']
    CGSTOutput = amounts['CGSTOutput']
    TotalGST = CGSTOutput+SGSTOutput
    BasicValue = amounts['BasicValue']
    if('InvoiceTotal[INR]' in amounts): 
        InvoiceTotal = amounts['InvoiceTotal[INR]']
    else:
        InvoiceTotal = "N/A"
    # logic ends
    
    # calulating totalQuantity
    lines = (jsonDict['lines'])
    totalQuantity=0
    for line in lines:
        totalQuantity += int(line["5_Quantity "])
   
    # ends

    
    #creating the json to be uploaded in DynamoDB - start
    res = {}
    x = datetime.datetime.now()
    res['time'] = str(x)
    if('GSTIN/UIN' in jsonDict):
        res['GSTIN/UIN'] = str(jsonDict['GSTIN/UIN'])
    else:
        res['GSTIN/UIN'] = str("N/A")
 
    if('InvoiceNo' in jsonDict):
        res['InvoiceNo'] = str(jsonDict['InvoiceNo'])
    else:
        res['InvoiceNo'] = str("N/A")
        
    '''res['Supplier Name'] = str(jsonDict["Supplier Name"])
    # res['InvoiceNo'] = str(jsonDict["InvoiceNo"])
    res['InvoiceDate'] = str(jsonDict['InvoiceDate'])
    # res['GSTIN/UINN'] = str(jsonDict['GSTIN/UINN'])
    res['NumberOfParts'] = numberOfParts
    res['TotalGST'] = str(TotalGST)
    res['InvoiceTotal[INR]'] = str(InvoiceTotal)
    res['BasicValue'] = str(BasicValue)
    res['TotalQuantity'] = str(totalQuantity)
    '''
    
    def check(num):
        num = float(num)
        if(num<80):
            return -1
        else:
            return 1
    
    
    # temp = {}
    # temp['1'] = str((jsonDict["Supplier Name"]))
    # temp['2'] =  (jsonDict['confidence']['SupplierName'])
    # temp['3'] = check(jsonDict['confidence']['SupplierName'])
    
  
    
    # temp2 = {}
    # temp2['1'] = str(jsonDict['InvoiceDate'])
    # temp2['2'] = jsonDict['confidence']['Date']
    # temp['3'] = check(jsonDict['confidence']['Date'])
    
    
    # temp3 = {}

    # temp4={}
    # temp4['1'] = str(jsonDict['TotalGST'])
    # temp4['2'] = jsonDict['confidence']['total']
    # temp4['3'] = check(jsonDict['confidence']['total'])
    
    # res['Supplier Name'] = temp
    # res['InvoiceNo'] = temp1
    # res['InvoiceDate'] = temp2
    # res['GSTIN/UINN'] = temp3 
    # res['TotalGST'] = temp4
    
      # temp1= {}
    # temp1['1'] = str(jsonDict["InvoiceNo"])
    # temp1['2'] = jsonDict['confidence']['InvoiceNo']
    # temp1['3'] = check(jsonDict['confidence']['InvoiceNo'])
    
    # temp2['1'] = str(jsonDict['InvoiceDate'])
    # temp2['2'] = jsonDict['confidence']['Date']
    # temp['3'] = check(jsonDict['confidence']['Date'])
    
    # temp4['1'] = str(jsonDict['TotalGST'])
    # temp4['2'] = jsonDict['confidence']['total']
    # temp4['3'] = check(jsonDict['confidence']['total'])

    # temp3['1'] = str(jsonDict['GSTIN/UINN'])
    # temp3['2'] = jsonDict['confidence']['GSTIN']
    # temp3['3'] = check(jsonDict['confidence']['GSTIN'])
    
    # res['InvoiceTotal[INR]'] = {}
    # res['InvoiceTotal[INR]'] = json.dumps(({'0':str(InvoiceTotal) ,'1':jsonDict['confidence']['invoiceTotal'] ,'2':check(jsonDict['confidence']['invoiceTotal']) }))
    
    res['BasicValue'] = {}
    res['BasicValue'] = json.dumps({'0':str(BasicValue) ,'1':jsonDict['confidence']['BasicValue'] ,'2':check(jsonDict['confidence']['BasicValue']) })
    
    res['CGSTOutput'] = {}
    res['CGSTOutput'] = json.dumps({"0": str(CGSTOutput),'1':jsonDict['confidence']['CGSTOutput'] ,'2':check(jsonDict['confidence']['CGSTOutput']) })
    
    res['SGSTOutput'] = {}
    res['SGSTOutput'] = json.dumps({"0": str(SGSTOutput),'1':jsonDict['confidence']['SGSTOutput'] ,'2':check(jsonDict['confidence']['SGSTOutput']) })
        
    res['GSTIN/UIN'] = {}
    res['GSTIN/UIN'] = json.dumps({'0':str(jsonDict['GSTIN/UIN']) ,'1':jsonDict['confidence']['GSTIN/UIN'] ,'2':check(jsonDict['confidence']['GSTIN/UIN']) })
    
    res['TotalGST'] = {}
    res['TotalGST'] = json.dumps({'0':str(TotalGST) ,'1':jsonDict['confidence']['TotalGST'] ,'2':check(jsonDict['confidence']['TotalGST']) })
    
    res['InvoiceDate'] = {}
    res['InvoiceDate'] = json.dumps({'0':str(jsonDict['InvoiceDate']) ,'1':jsonDict['confidence']['Date'] , '2':check(jsonDict['confidence']['Date']) })
    
    res['InvoiceNo'] = {}
    res['InvoiceNo'] = json.dumps({'0':str(jsonDict["InvoiceNo"]) , '1': jsonDict['confidence']['InvoiceNo'], '2': check(jsonDict['confidence']['InvoiceNo'])})
    
    res['Supplier Name'] = {}
    res['Supplier Name'] = json.dumps({'0':str((jsonDict["Supplier Name"])),'1':(jsonDict['confidence']['Supplier Name']),'2':check(jsonDict['confidence']['Supplier Name'])})
    
    res['lineItem'] = {}
    res['lineItem'] = json.dumps({'0':bigdata, '1': str(confidence1), '2': check(confidence1)})
    
    res['NumberOfParts'] = numberOfParts
    res['InvoiceTotal[INR]'] = str(InvoiceTotal)
    res['TotalQuantity'] = str(totalQuantity)
    
    res["Location"] = "s3/"+str(bucket)
    res["FileName"] = str(FileName)
    res["Status"] = str(status)
    res["Location"] = "https://s3.console.aws.amazon.com/s3/buckets/textract-mumbai-production/"+str(json_file)
    
    #json certion - end
    
    # toSave = json.dumps(res)
    # print(toSave)
    
    print(res)
    
    table = dynamodb.Table('TextRactProductionData')
    response = table.put_item(Item=res)
    print(datetime.datetime.now())
    # print(str(jsonDict))
    return {
        'statusCode': 200,
        # 'body': json.dumps('Hello from Lambda!')
    }

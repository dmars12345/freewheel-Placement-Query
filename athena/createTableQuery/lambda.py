import boto3
import json
from datetime import date
import datetime as datetime

def lambda_handler(event,context):

    lambda_client = boto3.client('lambda', region_name ='us-east-1')
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    athena_client = boto3.client('athena')

    queryData = json.loads(s3_resource.Object('placement-query-bucket','tableQuery.json').get()['Body'].read().decode('utf-8'))
    QueryStart = athena_client.start_query_execution(QueryString = queryData['query'] ,QueryExecutionContext = {'Database': queryData['database']},WorkGroup = queryData['workgroup'])
    functionPayload =  json.dumps({'tableQueryId' : QueryStart['QueryExecutionId'],'bucketKey' : 'athena/'})
    invokeFunction = lambda_client.invoke(FunctionName= queryData['dataArn'] , InvocationType='RequestResponse',Payload= functionPayload)

    return {'time' : json.dumps(datetime.now()), 'queryId': QueryStart['QueryExecutionId']}

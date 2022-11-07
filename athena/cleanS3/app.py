def lambda_handler(event,context):
    import boto3
    import json
    from datetime import date

    s3_resource = boto3.resource('s3')
    glue_client = boto3.client('glue')

    queryData = json.loads(s3_resource.Object('placement-query-bucket','tableQuery.json').get()['Body'].read().decode('utf-8'))
    copy_source = {
    'Bucket':queryData['bucketName'],  'Key': event['bucketKey'] + event['dataQueryId'] +'.csv' }
    outPut = queryData['cleanOutput'].replace('/date/', f"/{date.today()}/")
    
    bucket = s3_resource.Bucket(queryData['bucketName'])
    bucket.copy(copy_source , outPut )


    response = glue_client.delete_table(
    CatalogId= queryData['catalogId'],
    DatabaseName=queryData['database'],
    Name=queryData['table'],

    )

    s3_resource.Object(queryData['bucketName'],event['bucketKey'] + event['dataQueryId'] +'.csv'  ).delete()
    s3_resource.Object(queryData['bucketName'],event['bucketKey'] + event['dataQueryId'] +'.csv.metadata'  ).delete()
    s3_resource.Object(queryData['bucketName'],event['bucketKey'] + event['tableQueryId'] +'.txt'  ).delete()

    return json.dumps('done')

import json
import boto3
def lambda_handler(event,context):
    
        ecs_client= boto3.client('ecs',region_name = 'us-east-1')
        s3_resource = boto3.resource('s3','us-east-1')
        taskData = json.loads(s3_resource.Object(bucketName,'lambda.json').get()['Body'].read().decode('utf-8'))
        taskName = ecs_client.describe_task_definition(taskDefinition =taskData['familyName'] )['taskDefinition']['taskDefinitionArn'].split('/')[-1]
        run_task = ecs_client.run_task( taskDefinition= taskName,
        cluster= taskData['cluster'], overrides = {'taskRoleArn' : taskData['role']},count=1,launchType='FARGATE',
        networkConfiguration= {'awsvpcConfiguration': {'subnets': taskData['subnet'] , 'securityGroups':taskData['sg'] , 'assignPublicIp': 'ENABLED'}})
        time = str(run_task['tasks'][0]['createdAt'])
        return json.dumps({'time' :time, 'id': str(run_task ['tasks'][0]['attachments'][0]['id'])})

import boto3
import json
from pathlib import Path
desc = 'desc'


s3_resource = boto3.resource('s3', aws_access_key_id = creds['access'],
  aws_secret_access_key =  creds['secret'],
                 region_name ='us-east-1')



s3_client= boto3.client('s3', aws_access_key_id = creds['access'],
  aws_secret_access_key =  creds['secret'],
                 region_name ='us-east-1') 

bucket_name =  desc + '-bucket'

que_bucket_object = s3_resource.create_bucket(Bucket = bucket_name)
put = s3_client.put_public_access_block(Bucket =bucket_name,
PublicAccessBlockConfiguration={"BlockPublicAcls":True ,"IgnorePublicAcls": True,"BlockPublicPolicy":True,"RestrictPublicBuckets":True})
#create bucket and block public access

ecr_client = boto3.client('ecr', aws_access_key_id = creds['access'],
aws_secret_access_key =  creds['secret'],
                  region_name ='us-east-1')

ecr_name = desc 
ecr_response = ecr_client.create_repository(
repositoryName=ecr_name,imageTagMutability='IMMUTABLE',imageScanningConfiguration={'scanOnPush': True},encryptionConfiguration={ 'encryptionType': 'AES256'})
push = ecr_client.set_repository_policy(repositoryName=ecr_name,policyText=json.dumps( {"Version": "2008-10-17", "Statement":[{"Sid": "Statement1",  "Effect": "Allow", "Principal": "*",
"Action": ["ecr:BatchCheckLayerAvailability", "ecr:BatchGetImage",  "ecr:CompleteLayerUpload", "ecr:GetDownloadUrlForLayer", "ecr:InitiateLayerUpload",  "ecr:PutImage", "ecr:UploadLayerPart"]}]}
))
#set repo policy to enable uploads from ec2

EcrArn = ecr_response['repository']['repositoryArn']
EcrName = ecr_response['repository']['repositoryName']
EcrUri = ecr_response['repository']['repositoryUri']

ecs_client = boto3.client('ecs', aws_access_key_id = creds['access'],aws_secret_access_key =  creds['secret'],region_name ='us-east-1')



cluster_name = desc + '-cluster'

create_cluster= ecs_client.create_cluster(clusterName= cluster_name)

update_cluster = ecs_client.update_cluster_settings(cluster=  cluster_name, settings = [{'name' :'containerInsights' ,
                                                                               'value' :'enabled'}])
#create and save cluster to allow logs to go to cloud watch

bucketName =bucket_name
trust = 'ecs-tasks'
iam_client = boto3.client('iam', aws_access_key_id = creds['access'],
aws_secret_access_key =  creds['secret'],
                  region_name ='us-east-1')

role_dict = {}

trust_policy = {
"Version": "2012-10-17",
"Statement": [
    {
        "Effect": "Allow",
        "Principal": {
            "Service": f"{trust}.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
    }
]
}

roleName = desc.replace('-','_') + '_'  + 'role'
create_role  = iam_client.create_role(
RoleName =  roleName,
AssumeRolePolicyDocument = json.dumps(trust_policy))

#create role to for the ecs task which runs the freewheel placement query

role_dict[roleName] = {'arn' : create_role['Role']['Arn'] ,'trust' : trust, 
                                             'policies' : []}

policy = {
"Version": "2012-10-17",
"Statement": [
    {
        "Sid": "VisualEditor0",
        "Effect": "Allow",
        "Action": "ecr:GetAuthorizationToken",
        "Resource": "*"
    }
]
}

policy_name = desc + '_token_access_policy' 
create_policy = iam_client.create_policy(PolicyName= policy_name  , PolicyDocument = json.dumps(policy))
role_dict[roleName]['policies'].append({'name': create_policy['Policy']['PolicyName'] , 'arn' : create_policy['Policy']['Arn']})
iam_client.attach_role_policy(RoleName = create_role['Role']['RoleName'], PolicyArn= create_policy['Policy']['Arn'])
# allow the task get authorization from ecs

policy = {
"Version": "2012-10-17",
"Statement": [
    {
        "Effect": "Allow",
        "Action":
        [   "logs:CreateLogStream",
            "logs:PutLogEvents"],
        "Resource": [

                f"*"
            ]
    }]}

#allow the task to create logs and put log events
policy_name = desc + '_logs_access_policy' 
create_policy = iam_client.create_policy(PolicyName= policy_name  ,PolicyDocument = json.dumps(policy))
role_dict[roleName]['policies'].append({'name': create_policy['Policy']['PolicyName'] , 'arn' : create_policy['Policy']['Arn']})
iam_client.attach_role_policy(RoleName = create_role['Role']['RoleName'],PolicyArn= create_policy['Policy']['Arn'])


policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "VisualEditor0",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucketName}",
                   f"arn:aws:s3:::{bucketName}/*"
                ]
            }
        ]
    }  

policy_name = desc + '_bucket_access_policy' 
create_policy = iam_client.create_policy(PolicyName= policy_name  , PolicyDocument = json.dumps(policy))
role_dict[roleName]['policies'].append({'name': create_policy['Policy']['PolicyName'] , 'arn' : create_policy['Policy']['Arn']})
iam_client.attach_role_policy(RoleName = create_role['Role']['RoleName'], PolicyArn= create_policy['Policy']['Arn'])

#grant the task to read from the bucket where the queries are stored and write output too

policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": "secretsmanager:GetSecretValue",
            "Resource": "arn:aws:secretsmanager:us-east-1:AWSID:secret:freewheel-J2n8Cy"
        }
    ]
}

policy_name = desc + 'freewheel_secret_get_value_policy'
create_policy = iam_client.create_policy(PolicyName= policy_name  ,PolicyDocument = json.dumps(policy))
role_dict[roleName]['policies'].append({'name': create_policy['Policy']['PolicyName'] , 'arn' : create_policy['Policy']['Arn']})
iam_client.attach_role_policy(RoleName = create_role['Role']['RoleName'],PolicyArn= create_policy['Policy']['Arn'])


policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "kms:Decrypt",
                "ssm:GetParameters"
            ],
            "Resource": [
                "arn:aws:ssm:us-east-1:AWSID:parameter/*",
                "arn:aws:kms:us-east-1:AWSID:key/freewheel"
            ]
        }
    ]
}

#enable the ecs task to grab user name and password for freewheel authentication from secrets mannager 
policy_name = desc + '_decrypt_monday_key_and_get_param_access' 
create_policy = iam_client.create_policy(PolicyName= policy_name  ,PolicyDocument = json.dumps(policy))
role_dict[roleName]['policies'].append({'name': create_policy['Policy']['PolicyName'] , 'arn' : create_policy['Policy']['Arn']})
iam_client.attach_role_policy(RoleName = create_role['Role']['RoleName'],PolicyArn= create_policy['Policy']['Arn'])
push_docker_string_file = f'''docker build -t api:statesjobid .
docker run -d api:statesjobid
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin AWSID.dkr.ecr.us-east-1.amazonaws.com
docker tag api:statesjobid {EcrUri}:{desc}
docker push {EcrUri}:{desc}
'''
print(push_docker_string_file)
image_name = f"{EcrUri}:{desc}"
    
#create bash script which creates the container and sends it to the repository

family_name = desc +'_task'
log_group = f'/ecs/{family_name}'

ecs_client = boto3.client('ecs', aws_access_key_id = creds['access'],aws_secret_access_key =  creds['secret'],region_name ='us-east-1')

register_task =ecs_client.register_task_definition(family =family_name, containerDefinitions= [{'name': family_name,
      'image': image_name ,
      'cpu': 0,
      'essential': True,
      'environment': [],
      'mountPoints': [],
      'volumesFrom': [],
      'logConfiguration': {'logDriver': 'awslogs',
       'options': {'awslogs-group': log_group,
        'awslogs-region': 'us-east-1',
                   
                   
        'awslogs-stream-prefix': 'ecs'}}}],
        executionRoleArn = role_dict[roleName]['arn'],
                                          requiresCompatibilities = ['FARGATE'],
                                        networkMode = 'awsvpc', cpu = '256',

                                        memory = '1024')



task_arn = register_task['taskDefinition']['taskDefinitionArn']
log_client = boto3.client('logs', aws_access_key_id =  creds['access'], aws_secret_access_key = creds['secret'],
region_name ='us-east-1')

create_cloud_logs_response = log_client.create_log_group(logGroupName= log_group )


lambda_image = f"{EcrUri}:lambda-function"


trust = 'lambda'
iam_client = boto3.client('iam', aws_access_key_id = creds['access'],
aws_secret_access_key =  creds['secret'],
                  region_name ='us-east-1')


lambda_role_dict = {}







trust_policy = {
"Version": "2012-10-17",
"Statement": [
    {
        "Effect": "Allow",
        "Principal": {
            "Service": f"{trust}.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
    }
]
}

lambdaRoleName = desc.replace('-','_') + '_start_task_'  + 'role'
create_role  = iam_client.create_role(
RoleName =  lambdaRoleName,
AssumeRolePolicyDocument = json.dumps(trust_policy))

lambda_role_dict[lambdaRoleName] = {'arn' : create_role['Role']['Arn'] ,'trust' : trust, 
                                             'policies' : []}

policy = {
"Version": "2012-10-17",
"Statement": [
    {
        "Sid": "VisualEditor0",
        "Effect": "Allow",
        "Action": "iam:PassRole",
        "Resource": role_dict[roleName]['arn']
    }
]
}

policy_name = desc + '_' + 'pass_role_policy'
create_policy = iam_client.create_policy(PolicyName= policy_name  , PolicyDocument = json.dumps(policy))
lambda_role_dict[lambdaRoleName]['policies'].append({'name': create_policy['Policy']['PolicyName'] , 'arn' : create_policy['Policy']['Arn']})
iam_client.attach_role_policy(RoleName = create_role['Role']['RoleName'], PolicyArn= create_policy['Policy']['Arn'])

policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "VisualEditor0",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucketName}",
                   f"arn:aws:s3:::{bucketName}/*"
                ]
            }
        ]
    }  

policy_name = desc + 'lambda_bucket_access_policy' 
create_policy = iam_client.create_policy(PolicyName= policy_name  , PolicyDocument = json.dumps(policy))
lambda_role_dict[lambdaRoleName]['policies'].append({'name': create_policy['Policy']['PolicyName'] , 'arn' : create_policy['Policy']['Arn']})
iam_client.attach_role_policy(RoleName = create_role['Role']['RoleName'], PolicyArn= create_policy['Policy']['Arn'])


policy_name = desc + '_' + 'task_access_policy'
policy = {
"Version": "2012-10-17",
"Statement": [
    {
        "Sid": "VisualEditor0",
        "Effect": "Allow",
        "Action": "ecs:RunTask",
        "Resource":task_arn.replace(':3',':*')
    }
]
}

create_policy = iam_client.create_policy(PolicyName= policy_name  , PolicyDocument = json.dumps(policy))

lambda_role_dict[lambdaRoleName]['policies'].append({'name': create_policy['Policy']['PolicyName'] , 'arn' : create_policy['Policy']['Arn']})

iam_client.attach_role_policy(RoleName = create_role['Role']['RoleName'], PolicyArn= create_policy['Policy']['Arn'])


policy_name = desc + '_' + 'describe_task_access_policy'

policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": "ecs:DescribeTaskDefinition",
            "Resource": "*"
        }
    ]
}

create_policy = iam_client.create_policy(PolicyName= policy_name  , PolicyDocument = json.dumps(policy))

lambda_role_dict[lambdaRoleName]['policies'].append({'name': create_policy['Policy']['PolicyName'] , 'arn' : create_policy['Policy']['Arn']})

iam_client.attach_role_policy(RoleName = create_role['Role']['RoleName'], PolicyArn= create_policy['Policy']['Arn'])





push_lambda_docker_string_file = f'''docker build -t api:lambda{desc}query .
docker run -d api:lambda{desc}query
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin AWSID.dkr.ecr.us-east-1.amazonaws.com
docker tag api:lambda{desc}query {EcrUri}:lambda-function
docker push {EcrUri}:lambda-function
'''
lambda_image = f"{EcrUri}:lambda-function"
print(push_lambda_docker_string_file )

lambdaName = desc + '_start_task_function'

lambda_client = boto3.client('lambda', aws_access_key_id =  creds['access'], aws_secret_access_key = creds['secret'],
region_name ='us-east-1')

try:
    create_lambda_response = lambda_client.create_function(FunctionName = lambdaName, Timeout = 10,
    Code = {'ImageUri': lambda_image },PackageType = 'Image' ,Role = lambda_role_dict[lambdaRoleName]['arn'])
except:
    time.sleep(10)
    create_lambda_response = lambda_client.create_function(FunctionName = lambdaName, Timeout = 10,
    Code = {'ImageUri': lambda_image },PackageType = 'Image' ,Role =  lambda_role_dict[lambdaRoleName]['arn'])

event_client = boto3.client('events', aws_access_key_id =  creds['access'], aws_secret_access_key = creds['secret'],
region_name ='us-east-1')

rule_name = desc + '_start_task_lambda_rule'
create_rule = event_client.put_rule(Name =rule_name, ScheduleExpression = 'cron(0 6 * * ? *)')
lambda_client.add_permission(FunctionName=  create_lambda_response['FunctionName'],Action= 'lambda:InvokeFunction',
Principal='events.amazonaws.com',SourceArn= create_rule['RuleArn'],StatementId =desc + '_invoke_start_task_jobid_function') 
targets = [{'Id': '1' , 'Arn': create_lambda_response['FunctionArn'] }]
event_client.put_targets(Rule = rule_name ,Targets = targets)

glue_client = boto3.client('glue', aws_access_key_id = creds['access'],aws_secret_access_key =  creds['secret'],region_name ='us-east-1')
athena_client = boto3.client('athena', aws_access_key_id = creds['access'],aws_secret_access_key =  creds['secret'],region_name ='us-east-1')
catalogName = desc.replace('-','')
catalogResponse = athena_client.create_data_catalog(Name=catalogName,Type='GLUE' ,
Parameters={'catalog-id': 'AWSID'})
workGroupName = desc.replace('-','')
workGroupResponse = athena_client.create_work_group(Name= workGroupName,
Configuration={'ResultConfiguration': {'OutputLocation': 's3://' +bucketName + '/athena/' }})
catalog = 'AWSID'
database_name = desc.replace('-',"")
db_dict = {'Name': database_name,'CreateTableDefaultPermissions':
[{'Principal': {'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'},'Permissions': ['ALL']}]}
create_database = glue_client.create_database(CatalogId = catalog,DatabaseInput = db_dict)

workGroupArn = f"arn:aws:athena:us-east-1:AWSID:workgroup/{workGroupName}"
athenaCatalogArn =      f"arn:aws:athena:us-east-1:AWSID:datacatalog/{catalogName}"
catalogArn = f"arn:aws:glue:us-east-1:AWSID:catalog"
databaseArn = f"arn:aws:glue:us-east-1:AWSID:database/{database_name}"
tableArn = f"arn:aws:glue:us-east-1:AWSID:table/{database_name}/*"

trust = 'lambda'
athena_role_dict = {}
trust_policy = {"Version": "2012-10-17","Statement": [ {"Effect": "Allow","Principal": {"Service": f"{trust}.amazonaws.com" },"Action": "sts:AssumeRole"}]}

athenaRoleName = desc.replace('-','_') + '_athena_task_'+ 'role'
create_role  = iam_client.create_role(
RoleName =  athenaRoleName,
AssumeRolePolicyDocument = json.dumps(trust_policy))

athena_role_dict[athenaRoleName] = {'arn' : create_role['Role']['Arn'] ,'trust' : trust, 'policies' : []}

policy = {"Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "VisualEditor0",
                "Effect": "Allow",
                "Action": [
                    "athena:UpdateDataCatalog",
                    "athena:StartQueryExecution",
                    "athena:StopQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults"

                ],
                "Resource": [
                    workGroupArn,
                   athenaCatalogArn

                ]
            }]}
    
policy_name = desc + '-athena-query-access-policy'

create_policy = iam_client.create_policy(PolicyName= policy_name  , PolicyDocument = json.dumps(policy))
athena_role_dict[athenaRoleName]['policies'].append({'name': create_policy['Policy']['PolicyName'] , 'arn' : create_policy['Policy']['Arn']})
attach = iam_client.attach_role_policy(RoleName = create_role['Role']['RoleName'], PolicyArn= create_policy['Policy']['Arn'])

policy = {"Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:CreateTable",
                "glue:GetTable"
            ],
            "Resource": [
                tableArn,
                databaseArn,
                catalogArn
            ]


            }]}
policy_name = desc + '-glue-excecution-access-policy'

create_policy = iam_client.create_policy(PolicyName= policy_name  , PolicyDocument = json.dumps(policy))
athena_role_dict[athenaRoleName]['policies'].append({'name': create_policy['Policy']['PolicyName'] , 'arn' : create_policy['Policy']['Arn']})
attach = iam_client.attach_role_policy(RoleName = create_role['Role']['RoleName'], PolicyArn= create_policy['Policy']['Arn'])

policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "VisualEditor0",
                "Effect": "Allow",
                "Action": [
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                    "s3:ListMultipartUploadParts",
                    "s3:AbortMultipartUpload",
                    "s3:CreateBucket",
                    "s3:PutObject",
                    "s3:PutBucketPublicAccessBlock"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucketName}",
                   f"arn:aws:s3:::{bucketName}/*"
                ]
            }
        ]
    }

policy_name = desc + '-s3-bucket-access-policy'
create_policy = iam_client.create_policy(PolicyName= policy_name  , PolicyDocument = json.dumps(policy))
athena_role_dict[athenaRoleName]['policies'].append({'name': create_policy['Policy']['PolicyName'] , 'arn' : create_policy['Policy']['Arn']})
attach = iam_client.attach_role_policy(RoleName = create_role['Role']['RoleName'], PolicyArn= create_policy['Policy']['Arn'])

policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "lambda:InvokeFunction",
                "lambda:GetFunction",
                "lambda:InvokeAsync"
            ],
            "Resource": [dataLambdaArn,cleanLambdaArn]
        }
    ]
}
    
policy_name = desc + '-invoke-all-lambda-policy'
create_policy = iam_client.create_policy(PolicyName= policy_name  , PolicyDocument = json.dumps(policy))
athena_role_dict[athenaRoleName]['policies'].append({'name': create_policy['Policy']['PolicyName'] , 'arn' : create_policy['Policy']['Arn']})
attach = iam_client.attach_role_policy(RoleName = create_role['Role']['RoleName'], PolicyArn= create_policy['Policy']['Arn'])

tableLambdaArn = 'arn:aws:lambda:us-east-1:AWSID:function:placement-athena-create-table'
dataLambdaArn = 'arn:aws:lambda:us-east-1:AWSID:function:placement-athena-data'
cleanLambdaArn = 'arn:aws:lambda:us-east-1:AWSID:function:placement-clean-s3'


push_lambda_docker_string_file = f'''docker build -t api:lambda{desc}query .
docker run -d api:lambda{desc}query
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin AWSID.dkr.ecr.us-east-1.amazonaws.com
docker tag api:lambda{desc}query {EcrUri}:table-query
docker push {EcrUri}:table-query
'''
lambda_image = f"{EcrUri}:table-query"
print(push_lambda_docker_string_file )

lambdaName = 'placement-athena-create-table'
try:
    create_lambda_response = lambda_client.create_function(FunctionName = lambdaName, Timeout = 10, MemorySize = 1024,
 Code = {'ImageUri': lambda_image },PackageType = 'Image' ,Role = athena_role_dict[athenaRoleName]['arn'])

except:
    
    create_lambda_response = lambda_client.create_function(FunctionName = lambdaName, Timeout = 10, MemorySize = 1024,
 Code = {'ImageUri': lambda_image },PackageType = 'Image' ,Role = athena_role_dict[athenaRoleName]['arn'])
    

lambda_client.add_permission(FunctionName=  create_lambda_response['FunctionName'],Action= 'lambda:InvokeFunction',
Principal='s3.amazonaws.com',SourceAccount = 'AWSID',SourceArn= f"arn:aws:s3:::{bucketName}",StatementId =desc + '_invoke_table_query_function') 

trig_list = [{"Id": lambdaName,
            "LambdaFunctionArn":tableLambdaArn  , "Events": ["s3:ObjectCreated:Put"],
        'Filter': {'Key':{'FilterRules': [{'Name':'Prefix', 'Value' : ''},
        {'Name': 'Suffix', 'Value':'placements.csv' }]}}}]

trig_doc = {'LambdaFunctionConfigurations' : trig_list}

try:
    create_put_notification_con = s3_client.put_bucket_notification_configuration(Bucket = bucketName,NotificationConfiguration =  trig_doc)

except:
    time.sleep(10)
    create_put_notification_con = s3_client.put_bucket_notification_configuration(Bucket = bucketName,NotificationConfiguration =  trig_doc)

    
push_lambda_docker_string_file = f'''docker build -t api:lambda{desc}data .
docker run -d api:lambda{desc}data
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin AWSID.dkr.ecr.us-east-1.amazonaws.com
docker tag api:lambda{desc}data {EcrUri}:data
docker push {EcrUri}:data
'''
lambda_image = f"{EcrUri}:data"
print(push_lambda_docker_string_file )

lambdaName = 'placement-athena-data'
try:
    create_lambda_response = lambda_client.create_function(FunctionName = lambdaName, Timeout = 10, MemorySize = 1024,
 Code = {'ImageUri': lambda_image },PackageType = 'Image' ,Role = athena_role_dict[athenaRoleName]['arn'])

except:
    
    create_lambda_response = lambda_client.create_function(FunctionName = lambdaName, Timeout = 10, MemorySize = 1024,
 Code = {'ImageUri': lambda_image },PackageType = 'Image' ,Role = athena_role_dict[athenaRoleName]['arn'])
    

lambda_client.add_permission(FunctionName=  create_lambda_response['FunctionName'],Action= 'lambda:InvokeFunction',
    Principal='lambda.amazonaws.com',SourceAccount = 'AWSID', SourceArn= tableLambdaArn  ,StatementId = f'{lambdaName}_invoke') 

push_lambda_docker_string_file = f'''docker build -t api:lambda{desc}s3 .
docker run -d api:lambda{desc}s3
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin AWSID.dkr.ecr.us-east-1.amazonaws.com
docker tag api:lambda{desc}s3 {EcrUri}:clean
docker push {EcrUri}:clean
'''
lambda_image = f"{EcrUri}:data"
print(push_lambda_docker_string_file )

lambdaName = 'placement-clean-s3'
try:
    create_lambda_response = lambda_client.create_function(FunctionName = lambdaName, Timeout = 10, MemorySize = 1024,
 Code = {'ImageUri': lambda_image },PackageType = 'Image' ,Role = athena_role_dict[athenaRoleName]['arn'])

except:
    
    create_lambda_response = lambda_client.create_function(FunctionName = lambdaName, Timeout = 10, MemorySize = 1024,
 Code = {'ImageUri': lambda_image },PackageType = 'Image' ,Role = athena_role_dict[athenaRoleName]['arn'])
    

lambda_client.add_permission(FunctionName=  create_lambda_response['FunctionName'],Action= 'lambda:InvokeFunction',
    Principal='lambda.amazonaws.com',SourceAccount = 'AWSID', SourceArn= dataLambdaArn  ,StatementId = f'{lambdaName}_invoke')

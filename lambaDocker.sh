docker build -t api:lambdaplacement-queryquery .
docker run -d api:lambdaplacement-queryquery
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin AWSID.dkr.ecr.us-east-1.amazonaws.com
docker tag api:lambdaplacement-queryquery AWSID.dkr.ecr.us-east-1.amazonaws.com/placement-query:lambda-function
docker push AWSID.dkr.ecr.us-east-1.amazonaws.com/placement-query:lambda-function

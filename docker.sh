docker build -t api:query .
docker run -d api:query
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin AWSID.dkr.ecr.us-east-1.amazonaws.com
docker tag api:query AWSID.dkr.ecr.us-east-1.amazonaws.com/placement-query:placement-query
docker push AWSID.dkr.ecr.us-east-1.amazonaws.com/placement-query:placement-query

FROM public.ecr.aws/lambda/python:3.9
COPY app.py "/var/task"
COPY requirements.txt  .
RUN pip3 install -r requirements.txt --target "/var/task"
CMD [ "app.lambda_handler" ]

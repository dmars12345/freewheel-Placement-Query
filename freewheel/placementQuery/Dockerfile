FROM python:3.9

RUN pip3 install boto3

RUN pip3 install datetime

RUN pip3 install requests

RUN pip3 install freewheel4py==1.14

RUN pip3 install pandas

RUN pip3 install xmltodict

RUN pip3 install dict2xml

ADD app.py .

CMD ["python",  "./app.py"]

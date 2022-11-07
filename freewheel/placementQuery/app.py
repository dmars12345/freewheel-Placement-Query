import requests as rs
import json
from json import JSONDecodeError
import xmltodict
from dict2xml import dict2xml
from freewheel4py import freewheelPlacements as fwp
from datetime import date
import pandas as pd
from io import StringIO
import boto3
import datetime

session = boto3.session.Session().client( service_name='secretsmanager',region_name='us-east-1')
user = json.loads(session.get_secret_value(SecretId='arn:aws:secretsmanager:us-east-1:870811929817:secret:freewheel-J2n8Cy')['SecretString'])['fwUser']
password = json.loads(session.get_secret_value(SecretId='arn:aws:secretsmanager:us-east-1:870811929817:secret:freewheel-J2n8Cy')['SecretString'])['fwPass']
FW = fwp.fw(username = user  , password = password)
s3_resource = boto3.resource('s3',  region_name ='us-east-1')


def mapPlacement(self,fw):
    get= xmltodict.parse(rs.get(f"https://api.freewheel.tv/services/v3/insertion_orders/{self.data['placement']['insertion_order_id']}",headers = fw.xml).text,dict_constructor=dict)
    self.insertionOrderId= get['insertion_order']['id']
    self.campaignId = get['insertion_order']['campaign_id']
    get = xmltodict.parse(rs.get(f"https://api.freewheel.tv/services/v3/campaign/{self.campaignId}",headers = fw.xml).text,dict_constructor=dict)
    self.advertiserId = get['campaign']['advertiser_id']
    self.agencyId = get['campaign']['agency_id']

bucketName = 'placement-query-bucket'    
placementQueries= json.loads(s3_resource.Object(bucketName, 'query.json').get()['Body'].read().decode('utf-8'))
outputList = []
for query in placementQueries['query']:
    unfPlacements= fwp.placementNameQuery(FW,query = query['query'])
    get = [item.Get(FW) for item in unfPlacements]
    pacing = [item.getPacing(FW) for item in unfPlacements]
    io =  [mapPlacement(self =item ,fw = FW ) for item in unfPlacements]
    placements = [item if list(item.pacing.keys())[0] == 'placement_id' else None for item in unfPlacements]
    

    for item in placements:
        if item is not None:
            try:
                if item.data['placement']['budget']['over_delivery_value'] is None:
                    item.data['placement']['budget']['over_delivery_value'] = 0
                else:
                    item.data['placement']['budget']['over_delivery_value'] = int(item.data['placement']['budget']['over_delivery_value']) /100
            except KeyError:
                item.data['placement']['budget']['over_delivery_value'] = 0            
            if item.pacing['budget'] == 'All Impressions Sponsorship' or '%' in item.pacing['budget']:
                item.pacing['budget'] = '0'

    placementList = [{'date': str(date.today()),'placementName': item.data['placement']['name'], 'placementId': item.id,
    'insertionOrderId': item.insertionOrderId, 'advertiserId': item.advertiserId, 'campaignId': item.campaignId, 'agencyId': item.agencyId, 
    'startDate' : item.data['placement']['schedule']['start_time'].split('T')[0], 'endDate' : item.data['placement']['schedule']['end_time'].split('T')[0],
    'uga' : item.pacing['unconstrained_gross_available'], 'ga': item.pacing['gross_available'],'netAvails' : item.pacing['net_available'],
    'osi' : item.pacing['on_schedule_indicator']/100,'ffdr': item.pacing['forecast_final_delivery_rate']/100,
    'forecastedDelivery' : item.pacing['forecast_to_be_delivered_impressions'], 'budget': int(item.pacing['budget'].replace(',', '').replace('imps', '')),
    'deliveredImpressions': item.pacing['delivered_impressions'],'overDelivery': item.data['placement']['budget']['over_delivery_value'],
    'leftToDeliver': (int(item.pacing['budget'].replace(',', '').replace('imps', '')) - item.pacing['delivered_impressions']) * (1 +item.data['placement']['budget']['over_delivery_value']),
    'budgetModel' : item.data['placement']['budget']['budget_model']}
    if item is not None
    else {'date': None,'placementName': None,'placementId': None,'insertionOrderId': None,'advertiserId': None,'campaignId': None,'agencyId': None,
    'startDate': None,'endDate': None,'uga': None,'ga': None,'netAvails': None,'osi': None,'ffdr': None,'forecastedDelivery': None,
    'budget': None,'deliveredImpressions': None,'overDelivery': None,'leftToDeliver': None,'budgetModel': None}
    for item in placements ]
          
    outputDf = pd.DataFrame(placementList)
    outputDf['lower'] =outputDf['placementName'].str.lower()
    if query['exclude'] is not None:
        outputDf = outputDf[~outputDf['lower'].str.contains(query['exclude'].lower())].reset_index()
    del outputDf['lower']
    csvBuffer = StringIO()
    outputDf.to_csv(csvBuffer,index = False)
    s3_resource.Object('placement-query-bucket' , f"queries/{date.today()}/{query['query']}/{query['query']}.csv").put(Body = csvBuffer.getvalue())
    outputList.append(outputDf)
    
    
placementDf = pd.concat(outputList)
placementDf = placementDf[~placementDf['date'].isin([None])].reset_index()
placementDf['lower'] = placementDf['placementName'].str.lower()
for item in placementQueries['exclude']:
    placementDf = placementDf[~placementDf['lower'].str.contains(item.lower())]
    
placementDf = placementDf.drop_duplicates(subset = ['placementName'])
del placementDf['lower']
del placementDf['index']
csv= StringIO()
placementDf.to_csv(csv,index = False)
post = s3_resource.Object('placement-query-bucket' , f"queries/{date.today()}/aggregate/placements.csv").put(Body =csv.getvalue())
print(str(datetime.datetime.now()))

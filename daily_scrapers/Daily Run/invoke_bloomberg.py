import csv
import json
import boto3
import datetime
import time
from datetime import timedelta, date
import mysql.connector
import pandas as pd
import subprocess
import mysql.connector
import json
import pandas as pd
from bloomberg_lambda import lambda_handler
subprocess.call("sudo rm -rf /tmp/*",shell = True)
import logging as log
#server paths
# log_file_path = '/home/ubuntu/agrud-scrapers/daily_run/invoke_bloomberg/scraper_run_log.txt'

#local paths
log_file_path = r'D:\\sriram\\agrud\\daily_scrapers\\Daily Run\\scraper_run_log.txt'
log.basicConfig(filename = log_file_path,filemode='a',level=log.INFO)
my_log = log.getLogger()

def get_data():
    mydb = mysql.connector.connect(host='34.67.106.166',database='rentech_db',user='testuser',password='SAdf!AdsWER!@Ew',auth_plugin='mysql_native_password')
    df = pd.read_sql("""
            SELECT *
                FROM web_scraping_masters where status =  'Y' and source_name = 'Bloomberg' and json_structure_type = 0
                """, con = mydb)
    risk = df[df['url']== 'https://www.bloomberg.com/quote']
    src_list = risk['source_identifier']
    src_list = list(src_list)
    indicator = risk[["master_id","indicator_id","source_indicator_name"]]
    df = indicator.loc[:, ~indicator.columns.duplicated()]
    source_map = {}
    for index, row in df.iterrows():
        if row['master_id'] not in source_map:
            source_map[row['master_id']] = []
        source_map[row['master_id']].append({row['indicator_id']:row['source_indicator_name']})

    master = []
    for i in source_map:
        master.append(i)

    src_list = list([i for j, i in enumerate(src_list) if i not in src_list[:j]]) 
    return src_list,master

def invoke(src_list,master_ids):
    my_log.info(f'----------------started at:{datetime.datetime.now()}--------------------')
    client = boto3.client('lambda',
                        aws_access_key_id = 'AKIA3UZVVDCEDYBT7AVT',
                        aws_secret_access_key='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',
                        region_name='us-east-1')
    for src,master_id in zip(src_list,master_ids):
        payload = json.dumps({"source_id":[src],"master_id":[master_id]})
        my_log.info(payload)
        lambda_handler(json.loads(payload))
        # response = client.invoke(FunctionName='arn:aws:lambda:us-east-1:800587061384:function:bloomberg', 
        #     InvocationType='RequestResponse', LogType='None', Payload=payload)
        # data = response['Payload'].read()
        # value = json.loads(data.decode('utf-8'))['message']
        # my_log.info(f'{value}')
    my_log.info(f'----------------ended at:{datetime.datetime.now()}--------------------')
        
def scrape():
    src_list,master_ids = get_data()
    invoke(src_list,master_ids)

scrape()
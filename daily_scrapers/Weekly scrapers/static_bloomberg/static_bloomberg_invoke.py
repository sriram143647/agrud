import csv
import json
import boto3
import datetime
import time
from datetime import timedelta, date
import mysql.connector
import logging as log
# server paths
log_file_path = '/home/ubuntu/agrud-scrapers/weekly_run/static_bloomberg/scraper_run_log.txt'
log.basicConfig(filename = log_file_path,filemode='a',level=log.INFO)
my_log = log.getLogger()

lst1 =    [
    "COINETH:SS",
    "COINXBT:SS",
    "BTCE:GR",
    "COINXBE:SS"
  ]
lst2 =  [
    "84949",
    "84950",
    "86831",
    "86833"
  ]

def invoke():
  my_log.info(f'----------------started at:{datetime.datetime.now()}--------------------')
  for src_id,mst_id in zip(lst1,lst2):
      client = boto3.client('lambda',
                      aws_access_key_id='AKIA3UZVVDCEDYBT7AVT',
                      aws_secret_access_key='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',
                          region_name='us-east-1')

      payload = json.dumps({
      "source_id": [src_id],
      "master_id": [mst_id]
      })

      my_log.info(payload)
      response = client.invoke(
          FunctionName='arn:aws:lambda:us-east-1:800587061384:function:static_bloomberg',
          InvocationType='RequestResponse',
          LogType='None',
          Payload=payload
      )
      my_log.info(response['Payload'].read().decode("utf-8"))
  my_log.info(f'----------------ended at:{datetime.datetime.now()}--------------------')

invoke()

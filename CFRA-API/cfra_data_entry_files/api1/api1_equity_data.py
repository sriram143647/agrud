import pandas as pd
import requests
import datetime
import concurrent.futures
import os
session = requests.Session()
to_dt = today = datetime.datetime.today().strftime('%Y-%m-%d')
from_dt = (datetime.datetime.strptime(to_dt, '%Y-%m-%d').date() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
map_file = os.getcwd()+'\\ticker_to_masterid_map.csv'

def get_access_token():
  token_URL = "https://auth.cfraresearch.com/oauth2/token"
  client_ID = "3l7nhm06sin88ru1gjs06nvnhv"
  clientSecret = "1s2uvqtvp8fo9scimkarviohdb35fgr5iqmugsh8e6hvpejdj199"
  response = requests.post(
      token_URL,
      data={"grant_type": "client_credentials"},
      auth=(client_ID, clientSecret),
  )
  token = 'Bearer ' + response.json()["access_token"] 
  print('access token accquired')
  return token

def get_tickers():
  dataset = []
  df = pd.read_csv(map_file)
  for index,row in df.iterrows():
    data = {}
    data['ticker'] = row[0]
    data['exchange_code'] = row[1]
    dataset.append(data)
  return dataset
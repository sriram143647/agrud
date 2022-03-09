import pandas as pd
import json
import requests
session = requests.Session()

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

headers = {
  'accept': '*/*',
  'Authorization': get_access_token(),
  'x-api-key': 'z18plRKX0s6BEZuOB34WQZHEDUlgBI1aXXrwP6Ha'
}

def get_data():
  api_url = f"https://api.cfraresearch.com/equities"
  print(api_url)
  result = session.get(api_url, headers=headers).json()
  df = pd.json_normalize(result['result'])
  # df.to_csv("api4_temp_data.csv", index = False)
  # filter data
  df1 = pd.read_csv('input.csv')
  # df2 = pd.read_csv('api4_temp_data.csv')
  df2 = df
  df3 = df1.merge(df2, how = "left", left_on = ["source_symbol", "source_exchange"], right_on=["ticker", "exchange_code"])
  df3 = df3.dropna()
  df3.to_csv('api4.csv',index=False)

get_data()

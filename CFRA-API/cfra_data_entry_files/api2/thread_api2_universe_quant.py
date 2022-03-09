import pandas as pd
import requests
import os
session = requests.Session()
input_tickers_file = os.getcwd()+'\\input_tickers.csv'

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


def scrape():
  # headers = {
  # 'accept': '*/*',
  # 'Authorization': get_access_token(),
  # 'x-api-key': 'z18plRKX0s6BEZuOB34WQZHEDUlgBI1aXXrwP6Ha'
  # }
  # mainDf = pd.DataFrame()
  # opts = ['strong_sell', 'sell', 'hold', 'buy', 'strong_buy']
  # for p in opts:
  #     api_url = f"https://api.cfraresearch.com/equity/quant/universe?recommendation={p}"
  #     print(api_url)
  #     result = session.get(api_url, headers=headers).json()
  #     df = pd.DataFrame(result['result'])
  #     df['recommendation'] = p
  #     mainDf = mainDf.append(df)
  # mainDf.to_csv("api2_temp_data.csv", index = False)
  #data filter
  df1 = pd.read_csv(input_tickers_file)
  # df2 = 
  df2 = pd.read_csv('api2_temp_data.csv')
  df3 = df1.merge(df2, how = "left", left_on = ["source_symbol", "source_exchange"], right_on=["ticker", "exchange_code"])
  df3 = df3.dropna()
  df3.to_csv('api2.csv', index=False)

if __name__ == "__main__":
  scrape()
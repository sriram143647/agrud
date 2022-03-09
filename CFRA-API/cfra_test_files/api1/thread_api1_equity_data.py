import pandas as pd
import requests
import datetime
import concurrent.futures
session = requests.Session()
to_dt = today = datetime.datetime.today().strftime('%Y-%m-%d')
from_dt = (datetime.datetime.strptime(to_dt, '%Y-%m-%d').date() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')

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

header = {
  'accept': '*/*',
  'Authorization':get_access_token(),
  'x-api-key': 'z18plRKX0s6BEZuOB34WQZHEDUlgBI1aXXrwP6Ha'
}

def get_tickers():
  dataset = []
  df = pd.read_csv(r'D:\\sriram\\agrud\\cfra\\CFRA-API\\cfra_test_files\\api1\\input_tickers.csv')
  for index,row in df.iterrows():
    data = {}
    data['ticker'] = row[0]
    data['exchange_code'] = row[1]
    dataset.append(data)
  return dataset

def get_api_urls():
  api_urls = []
  dataset = get_tickers()
  for input in dataset:
    ticker = input['ticker']
    exchange_code = input['exchange_code']
    api_url = f"https://api.cfraresearch.com/equity/data/stars/ticker/{ticker}/exchange/{exchange_code}?date_from={from_dt}&date_to={to_dt}"
    api_urls.append(api_url)
  return api_urls


def fetch_data(api_url):
  ticker = api_url.split('/')[-3]
  exchange_code = api_url.split('/')[-1].split('?')[0]
  result = session.get(api_url, headers=header).json()
  try:
      if 'CFRA_ENTITY_UNKNOWN' in result['error_code']:
        print(f'ticker:{ticker} not found')
        return 0
  except KeyError:
      pass
  ticker = result['result']['ticker']
  print(f'ticker:{ticker} found data fetched')
  star_data = result['result']['stars_data']
  df = pd.DataFrame(star_data)
  df.insert(loc = 0,column = 'ticker',value = ticker)
  df.insert(loc = 1,column = 'exchange_code',value = exchange_code)
  with open('api1_data.csv', mode='a', newline='') as f:
    df.to_csv(f,header=f.tell()==0,index=False)


def scrape():
    urls = get_api_urls()
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as link_executor:
        [link_executor.submit(fetch_data,link) for link in urls]

scrape()
import pandas as pd
import requests
import csv
import json
import os
import concurrent.futures
from bs4 import BeautifulSoup
session = requests.Session()
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


def write_header(): 
    with open('api3_data.csv', mode='a', encoding='utf-8',newline="") as output_file:
        writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow([
          "3_yr_proj_eps_cagr_prcntg","analyst","business_summary","fair_value","highlights","highlights_date","income_estimates",
          "insider_activity","investment_risk_rationale","investment_risk_rationale_date","oper_eps_2022E","oper_eps_2023E",
          "pdf_url","price_at_publication","publication_date","price_to_oper_eps_2022E","quality_risk_assessment","reporting_frequency",
          "revenue_estimates","sub_industry_outlook","summary","trading price","technical_evaluation","oper_eps_2021E",
          "price_to_oper_eps_2021E","ticker","exchange_code"
        ])

def write_data(data):
    with open('api3_data.csv', mode='a', encoding='utf-8', newline="") as output_file:
        writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(data)

def get_data():
  dataset = []
  df = pd.read_csv(map_file)
  for index,row in df.iterrows():
    data = {}
    data['ticker'] = row[0]
    data['exchange_code'] = row[1]
    dataset.append(data)
  return dataset

def get_api_urls():
  api_urls = []
  dataset = get_data()
  for input in dataset[:]:
    ticker = input['ticker']
    exchange_code = input['exchange_code']
    api_url = f"https://api.cfraresearch.com/equity/research/stars-full/ticker/{ticker}/exchange/{exchange_code}"
    api_urls.append(api_url)
  return api_urls

def write_error(ticker,exchange_code):
    text = f'{ticker}, {exchange_code}'
    file1 = open("ticker_errors.csv",mode="a",encoding='utf-8')
    file1.write(str(text)+'\n')
    file1.close()

def ticker_data(ticker,exchange_code):
    text = f'{ticker}, {exchange_code}'
    file1 = open("ticker_data.csv",mode="a",encoding='utf-8')
    file1.write(str(text)+'\n')
    file1.close()

def fetch_data(api_url,header):
  ticker = api_url.split('/')[-3]
  exchange_code = api_url.split('/')[-1]
  result = session.get(api_url, headers=header).json()
  try:
      if 'CFRA_ENTITY_UNKNOWN' in result['error_code']:
        print(f'ticker:{ticker} not found')
        write_error(ticker,exchange_code)
        return 0
  except KeyError:
      pass
  result = result['result']
  print(f'ticker:{ticker} found data fetched')
  ticker_data(ticker,exchange_code)
  try:
    ticker = result['ticker']
  except:
    write_error(ticker,exchange_code)
  try:
    exchange_code = result['exchange']
  except:
    write_error(ticker,exchange_code)
  try:
    three_yrs_eps_per = result['3_yr_proj_eps_cagr_prcntg']
  except:
    three_yrs_eps_per = ''
  try:   
    analyst = result['analyst']
  except:
    analyst = ''
  try:
    busi_summ = BeautifulSoup(result['business_summary'],'html5lib').text
  except:
      busi_summ = ''
  try:
    f_value = json.dumps(result['fair_value'])
  except:
    f_value = ''
  try:
    high_lights = BeautifulSoup(result['highlights'],'html5lib').text
  except:
    high_lights = ''
  try:
    high_lights_date = result['highlights_date']
  except:
    high_lights_date = ''
  try:
    income_est = json.dumps(result['income_estimates'])
  except:
    income_est = ''
  try:
    isn_act = result['insider_activity']
  except:
    isn_act = ''
  try:
    inves_risk_rational = BeautifulSoup(result['investment_risk_rationale'],'html5lib').text
  except:
    inves_risk_rational = ''
  try:
    inves_risk_rational_dt = result['investment_risk_rationale_date']
  except:
    inves_risk_rational_dt = ''
  try:
    oper_2022 = result['oper_eps_2022E']
  except:
    oper_2022 = ''
  try:
    oper_2023 = result['oper_eps_2023E']
  except:
    oper_2023 = ''
  try:
    pdf_url = result['pdf_url']
  except:
    pdf_url = ''
  try:
    price_pub = result['price_at_publication']
  except:
    price_pub = ''
  try:
    pub_date = result['publication_date']
  except:
    pub_date = ''
  try:
    price_oper_2022 = result['price_to_oper_eps_2022E']
  except:
    price_oper_2022 = ''
  try:
    qual_risk_asses = BeautifulSoup(result['quality_risk_assessment']['text'],'html5lib').text
  except:
    qual_risk_asses = ''
  try:
    report_freq = result['reporting_frequency']
  except:
    report_freq = ''
  try:
    rev_est =  json.dumps(result['revenue_estimates'])
  except:
    rev_est = ''
  try:
    sub_ind_outlook = BeautifulSoup(result['sub_industry_outlook'],'html5lib').text
  except:
    sub_ind_outlook = ''
  try:
    summ = BeautifulSoup(result['summary'],'html5lib').text
  except:
    summ = ''
  try:
    trading_price = result['trading_price']
  except:
    trading_price = ''
  tech_eval = result['technical_evaluation']
  try:
    oper_2021 = result['oper_eps_2021E']
  except:
    oper_2021 = ''
  try:
    price_oper_2021 = result['price_to_oper_eps_2021E']
  except:
    price_oper_2021 = ''
  data = [
    three_yrs_eps_per,analyst,busi_summ,f_value,high_lights,high_lights_date,income_est,isn_act,inves_risk_rational,inves_risk_rational_dt,
    oper_2022,oper_2023,pdf_url,price_pub,pub_date,price_oper_2022,qual_risk_asses,report_freq,rev_est,sub_ind_outlook,summ,
    trading_price,tech_eval,oper_2021,price_oper_2021,ticker,exchange_code
  ]
  write_data(data)

def scrape():
  header = {
  'accept': '*/*',
  'Authorization': get_access_token(),
  'x-api-key': 'z18plRKX0s6BEZuOB34WQZHEDUlgBI1aXXrwP6Ha'
  }
  write_header()
  urls = get_api_urls()
  with concurrent.futures.ThreadPoolExecutor(max_workers=15) as link_executor:
      [link_executor.submit(fetch_data,link,header) for link in urls]

if __name__ == "__main__":
  scrape()
from bs4 import BeautifulSoup
from datetime import datetime,timedelta
import csv
import json
import pandas as pd
import os
import requests
import concurrent.futures
import common_utility as cu
session = requests.session()
domain = os.getcwd().split('\\')[-1].replace(' ','_')
output_file = f"{domain}_data.csv"
for file in os.listdir():
    if 'MF List - Final' in file and '.csv' in file:
        data_file = os.getcwd()+'\\'+file
        break
non_scraped_isin_file = f"{domain}_non_scraped_data.csv"  

def get_auth_token(header,pi_token):
    url = f'https://sg.morningstar.com/sg/report/fund/performance.aspx?t={pi_token}'
    res = requests.get(url, headers=header)
    soup = BeautifulSoup(res.text,'html5lib')
    bearer_token = 'Bearer '+soup.find('sal-components').get('maas_token')
    return bearer_token

def get_realtime_api_key(header):
    url = "https://euim.mstar.com/modules/sal-report/fund/dist/js/sal-components-wrapper.js?v=3.59.0"
    res = requests.get(url, headers=header)
    soup = BeautifulSoup(res.text,'html5lib')
    api_realtime_key = res.text.split('realtime')[1].split('};')[0].replace(':','').replace('\n','').replace('\r','').replace("'","").strip()
    return api_realtime_key

def get_headers():
    isin = 'SGXZ83598466' 
    header = {
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'cookie': cu.getCookie('https://sg.morningstar.com/sg/'),
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
    }
    now = datetime.now()
    tm_stamp = int(datetime.timestamp(now))
    url = f'https://sg.morningstar.com/sg/util/SecuritySearch.ashx?source=nav&moduleId=6&ifIncludeAds=True&usrtType=v&q={isin}&limit=100&timestamp={tm_stamp}'
    res = session.get(url,headers=header)
    soup = BeautifulSoup(res.text,'html5lib')
    try:
        data = soup.text.split('|||')[1].split('|')[1]
    except:
        return 0
    j_data = json.loads(data)
    pi_token = j_data['pi']
    auth_token = get_auth_token(header,pi_token)
    api_realtime_key = get_realtime_api_key(header)
    api_header = {
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'authorization':auth_token,
        'x-api-realtime-e':api_realtime_key,
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
    }
    return header,api_header


def morningstar_gen_case(api_header,lst):
    nav_price = ''
    nav_date = ''
    isin = lst[0]
    master_id = lst[1]
    api_url = lst[2]
    res2 = session.get(api_url,headers = api_header)
    j_data2 = json.loads(res2.text)
    try:
        try:
            nav_price = j_data2['nav']
        except:
            nav_price = ''
        
        try:
            date = j_data2['navEndDate']
            if datetime.strptime(date,'%Y-%m-%d').date() > datetime.now().date() - timedelta(days=10):
                nav_date = datetime.strftime(datetime.strptime(date,'%Y-%m-%d'),'%Y-%m-%d')
        except:
            nav_date = ''
    except:
        pass
    if nav_price != '' and nav_date != '':
        row = [master_id,isin,round(nav_price,2),nav_date]
        with open(output_file,"a",newline="") as file:
            writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writerow(row)
        return 0
    else:
        return 0

def start_sg_morningstar_scraper():
    data_lst = []
    cu.csv_filter(output_file)
    downloaded_isin = pd.read_csv(output_file)['isin name'].values.tolist()
    header,api_header = get_headers()
    df = pd.read_csv(data_file,encoding="utf-8")
    df = df.drop_duplicates(subset=['Master ID'])
    df = df[df['Symbol'].str.contains('SG')]
    df = df[~df['Symbol'].isin(downloaded_isin)]
    for i,row in df.iterrows():
        isin = row[0]
        master_id = row[2]
        now = datetime.now()
        tm_stamp = int(datetime.timestamp(now))
        url = f'https://sg.morningstar.com/sg/util/SecuritySearch.ashx?source=nav&moduleId=6&ifIncludeAds=True&usrtType=v&q={isin}&limit=100&timestamp={tm_stamp}'
        res = session.get(url,headers=header)
        soup = BeautifulSoup(res.text,'html5lib')
        try:
            data = soup.text.split('|||')[1].split('|')[1]
        except:
            continue
        j_data = json.loads(data)
        i_token =  j_data['i']
        api_url = f"https://www.us-api.morningstar.com/sal/sal-service/fund/quote/realTime/{i_token}/data?secExchangeList=null&random=0.42294477494679383&languageId=en&locale=en&clientId=MDC_intl&benchmarkId=mstarorcat&component=sal-components-mip-quote&version=3.60.0"
        lst = [isin,master_id,api_url]
        data_lst.append(lst)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as link_executor:
        [link_executor.submit(morningstar_gen_case,api_header,lst) for lst in data_lst]
    df = cu.csv_filter(output_file)
    # cu.db_insert(df)


if __name__ == '__main__':
    start_sg_morningstar_scraper()
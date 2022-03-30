from doctest import master
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from bs4 import BeautifulSoup
from datetime import datetime,timedelta
import csv
import json
import pandas as pd
import numpy as np
import os
import requests
import pymysql
session = requests.session()
domain = os.getcwd().split('\\')[-1].replace(' ','_')
output_file = f"{domain}_data.csv"
for file in os.listdir():
    if 'MF List - Final' in file and '.csv' in file:
        data_file = os.getcwd()+'\\'+file
        break
non_scraped_isin_file = f"{domain}_non_scraped_data.csv"  

def get_driver():
    s=Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument("--disable-logging")
    # options.add_argument("--incognito")
    options.add_argument('--headless')
    driver = webdriver.Chrome(service=s,options=options)
    return driver

def getCookie(url):
    driver = get_driver()
    driver.get(url)
    cookies_list = driver.get_cookies()
    cookies_json = {}
    for cookie in cookies_list:
        cookies_json[cookie['name']] = cookie['value']
    cookies_string = str(cookies_json).replace("{", "").replace("}", "").replace("'", "").replace(": ", "=").replace(",", ";")
    driver.quit()
    return cookies_string

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
        'cookie': getCookie('https://sg.morningstar.com/sg/'),
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

def db_insert(df):
    result = df[['master id','price','date']].values.tolist()
    try:
        db_conn = pymysql.connect(host='54.237.79.6',user='rentech_user',database = 'rentech_db',password='N)baegbgqeiheqfi3e9314jnEkekjb')
        cursor = db_conn.cursor()
        sql = "INSERT IGNORE INTO `raw_data_test` VALUES (NULL, %s, 371, %s, NULL, 2, %s, '0:0:0', 12, NULL, CURRENT_TIMESTAMP());"
        cursor.executemany(sql, result)
        rows = cursor.rowcount
        print(f'{rows} rows inserted')
        db_conn.commit()
    except Exception as e:
        print(f'Exception: {e}')
    finally:
        if db_conn.open:
            cursor.close()
            db_conn.close()
            print('Connection closed')

def write_header():
    with open(output_file,"a",newline="") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(['master id','isin name','price','date'])

def write_output(data):
    with open(output_file,"a",newline="") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(data)

def csv_filter():
    filtered_df = pd.DataFrame()
    unique_isin = []
    cols = ['master id','isin name','price','date']
    try:
        df = pd.read_csv(output_file,encoding='utf-8')
    except FileNotFoundError:
        write_header()
        return 0
    for isin,grouped_df in df.groupby(by=['isin name']):
        for i,row in grouped_df.iterrows():
            if row[2] is not np.nan and row[3] is not np.nan:
                if isin not in unique_isin:
                    unique_isin.append(isin)
                    filtered_df = filtered_df.append(pd.DataFrame([row],columns=cols),ignore_index=True)
    try:
        filtered_df.to_csv(output_file,encoding='utf-8',columns=cols,index=False)
        return filtered_df
    except:
        pass

def morningstar_gen_case(api_header,header,isin, master_id):
    nav_price = ''
    nav_date = ''
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
    i_token =  j_data['i']
    api_url = f"https://www.us-api.morningstar.com/sal/sal-service/fund/quote/realTime/{i_token}/data?secExchangeList=null&random=0.42294477494679383&languageId=en&locale=en&clientId=MDC_intl&benchmarkId=mstarorcat&component=sal-components-mip-quote&version=3.60.0"
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
        write_output(row)
        return 0
    else:
        f = open(non_scraped_isin_file, 'a')
        f.write(f'{isin}\n')
        f.close()
        return 0

def isin_downloaded():
    isin_downloaded = []
    with open(output_file,"r") as file:
        csvreader = csv.reader(file)
        header = next(csvreader)
        for row in csvreader:
            isin_downloaded.append(row[1])
    return isin_downloaded

def start_sg_morningstar_scraper():
    csv_filter()
    downloaded_isin = isin_downloaded()
    header,api_header = get_headers()
    df = pd.read_csv(data_file,encoding="utf-8")
    df = df.drop_duplicates(subset=['Master ID'])
    df = df[df['Symbol'].str.contains('SG')]
    df = df[~df['Symbol'].isin(downloaded_isin)]
    for i,row in df.iterrows():
        isin = row[0]
        master_id = row[2]
        morningstar_gen_case(api_header,header,isin,master_id)
    df = csv_filter()
    # db_insert(df)


if __name__ == '__main__':
    start_sg_morningstar_scraper()
    # master_id = '140871'
    # isin = 'GB0000796242'
    # header,api_header = get_headers()
    # morningstar_gen_case(api_header,header,isin, master_id)


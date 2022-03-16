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
import logging as log
log_file_path = r'D:\\sriram\\agrud\\NAV_scraping\\scraper_run_log.txt'
log.basicConfig(filename = log_file_path,filemode='a',level=log.INFO)
my_log = log.getLogger()  

def get_driver():
    s=Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    # options.add_argument("--incognito")
    # options.add_argument('--headless')
    driver = webdriver.Chrome(service=s,options=options)
    return driver
    
header = {
    'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
}

api_header = {
    'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'cookie':'RT_sg_LANG=en-SG; __utma=254057994.487918150.1647251651.1647251651.1647251651.1; __utmc=254057994; __utmz=254057994.1647251651.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmt=1; ad-profile={"NeedRefresh":false,"UserType":0,"AudienceType":-1,"PortofolioCreated":0,"IsForObsr":false,"NeedPopupAudienceBackfill":false,"EnableInvestmentInUK":-1}; _ga=GA1.2.487918150.1647251651; _gid=GA1.2.1825311572.1647251651; qs_wsid=6A444965ECFD2630B888F9F28389E0AA; Instid=EURTL; mid=5810194844484897674; _gat_gtmIntl=1; __utmb=254057994.11.9.1647252129314',
    'x-api-realtime-e':'eyJlbmMiOiJBMTI4R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.jidbnhWUJvWMvD2omPxv9L_-iiW424c89SMUexsXDrY3VE6CfIU1g6cRPZ-DToeSbtFEvMPV4DoNlOtzUJ63Ryja23rtjlMsrpV-nLpUUpjcZp7ZL0YjGQNbsq1a-vAwf7GBOk6lnsOWycXB0mKaHMXfHpgwAsRcfGK1QpIb27U.da0qHJBnRmc0_EOq.1fXioE66EIItzsggPK3b4HNypNp1Ltva84HWRNmxwBPsUo5kvUXaYDuFjLHT2K39RsoMqZzERuQMfrP8fYoqTfkBmg-xD5sQbqHYeKgvuqMdOlkRVx4y16ft1RiliknyWNGxTd_5KXZrkDadGR7gHQyC775iNEBi0bI9F_JXu3t7_8uBMHNOYlIeRHm2Dqmz17ukL_zoGOX74_KiaYkBL5RamA.7wz-dbBZR15KCDRanQrHPg',
    'authorization':'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ik1EY3hOemRHTnpGRFJrSTRPRGswTmtaRU1FSkdOekl5TXpORFJrUTROemd6TWtOR016bEdOdyJ9.eyJodHRwczovL21vcm5pbmdzdGFyLmNvbS9tc3Rhcl9pZCI6Ijc2NjU2NkFELTkxMjEtNDJDMS05RjM2LTkwREM1RkNENUUxQyIsImh0dHBzOi8vbW9ybmluZ3N0YXIuY29tL2VtYWlsIjoiajYzazg5NW5qcGE4bW5tcTRwMmptc3R0Y2g5MHNhaGJAbWFhcy1tc3Rhci5jb20iLCJodHRwczovL21vcm5pbmdzdGFyLmNvbS9yb2xlIjpbIkVDLlNlcnZpY2UuQ29uZmlndXJhdGlvbiIsIkVDLlNlcnZpY2UuSG9zdGluZyIsIkVDVVMuQVBJLkF1dG9jb21wbGV0ZSIsIkVDVVMuQVBJLlNjcmVlbmVyIiwiRUNVUy5BUEkuU2VjdXJpdGllcyIsIlBBQVBJVjEuWHJheSIsIlZlbG9VSS5BbGxvd0FjY2VzcyJdLCJodHRwczovL21vcm5pbmdzdGFyLmNvbS9jb21wYW55X2lkIjoiMjgyY2M4NjUtMzUwNS00ZTIwLThkMjQtODRhNDM4YjIxOTI0IiwiaHR0cHM6Ly9tb3JuaW5nc3Rhci5jb20vaW50ZXJuYWxfY29tcGFueV9pZCI6IkNsaWVudDAiLCJodHRwczovL21vcm5pbmdzdGFyLmNvbS9kYXRhX3JvbGUiOlsiRUNVUy5EYXRhLlVTLk9wZW5FbmRGdW5kcyIsIlFTLk1hcmtldHMiLCJRUy5QdWxscXMiLCJTQUwuU2VydmljZSJdLCJodHRwczovL21vcm5pbmdzdGFyLmNvbS9sZWdhY3lfY29tcGFueV9pZCI6IjI0YmYwYTg1LTMyNzEtNGIxYi1hYjFlLTBlOWZkMTg4MThiZCIsImh0dHBzOi8vbW9ybmluZ3N0YXIuY29tL3VpbV9yb2xlcyI6Ik1VX01FTUJFUl8xXzEiLCJpc3MiOiJodHRwczovL2xvZ2luLXByb2QubW9ybmluZ3N0YXIuY29tLyIsInN1YiI6ImF1dGgwfDc2NjU2NkFELTkxMjEtNDJDMS05RjM2LTkwREM1RkNENUUxQyIsImF1ZCI6WyJodHRwczovL2F1dGgwLWF3c3Byb2QubW9ybmluZ3N0YXIuY29tL21hYXMiLCJodHRwczovL3VpbS1wcm9kLm1vcm5pbmdzdGFyLmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE2NDcyNTI3NTIsImV4cCI6MTY0NzI1NjM1MiwiYXpwIjoiaVFrV3hvYXBKOVB4bDhjR1pMeWFYWnNiWFY3OWc2NG0iLCJzY29wZSI6Im9wZW5pZCIsImd0eSI6InBhc3N3b3JkIn0.sDLNa2msJpDYibJ7taujY_WkiV2YY-GwB_2wKfrrZxnJTAOZ2oeicXsaHCRvMnZ1d4qfhDSGv8KKgqMSZ7-gf3u7xGYUhWOw2OF88AT-_fREbJgjf8Kgi-zdY-Xj1XmKv3zjK1RdIpoqJU74NLURG0bZe40l8eaZgay4aqBPQB3XLzZc8HX-x8fMyOKdTVzov08pi1tqkqas1w2ZcIthI3JVM25hq2mGFtUuDwiRk1wuHDVh6Yqghwuk2-j3CIPScn5iawyGna2402WFBRKz4MEgkyY_iWYpdndhcSVtsg6IgOwJfJ8z2Skep5PkWWhjvDfV_0We7oofaByuGRmscQ',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
}

def db_insert(df):
    result = df[['master id','price','date']].values.tolist()
    try:
        db_conn = pymysql.connect(host='54.237.79.6',user='rentech_user',database = 'rentech_db',password='N)baegbgqeiheqfi3e9314jnEkekjb')
        cursor = db_conn.cursor()
        sql = "INSERT IGNORE INTO `raw_data_test` VALUES (NULL, %s, 371, %s, NULL, 2, %s, '0:0:0', 12, NULL, CURRENT_TIMESTAMP());"
        cursor.executemany(sql, result)
        rows = cursor.rowcount
        my_log.info(f'{rows} rows inserted')
        db_conn.commit()
    except Exception as e:
        my_log.info(f'Exception: {e}')
    finally:
        if db_conn.open:
            cursor.close()
            db_conn.close()
            my_log.info('Connection closed')

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

def morningstar_gen_case(isin, master_id):
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
    api_url = f"https://www.us-api.morningstar.com/sal/sal-service/fund/quote/realTime/{j_data['i']}/data?secExchangeList=null&random=0.42294477494679383&languageId=en&locale=en&clientId=MDC_intl&benchmarkId=mstarorcat&component=sal-components-mip-quote&version=3.60.0"
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
        my_log.info(f'isin {isin} scraped sg_morningstar')
        row = [master_id,isin,nav_price,nav_date]
        write_output(row)
        return 0
    else:
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
    df = pd.read_csv(data_file,encoding="utf-8")
    df = df.drop_duplicates(subset=['Master ID'])
    for i,row in df.iterrows():
        isin = row[0]
        master_id = row[2]
        if isin not in downloaded_isin and 'SG' in isin:
            morningstar_gen_case(isin,master_id)
    df = csv_filter()
    # db_insert(df)


if __name__ == '__main__':
    start_sg_morningstar_scraper()
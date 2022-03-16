from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from datetime import datetime,timedelta
import csv
import json
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import os
import pymysql.cursors
session = requests.session()
domain = os.getcwd().split('\\')[-1].replace(' ','_')
output_file = f"{domain}_data.csv"
non_scraped_isin_file = f"{domain}_non_scraped_data.csv" 
for file in os.listdir():
    if 'MF List - Final' in file and '.csv' in file:
        data_file = os.getcwd()+'\\'+file
        break  


def get_driver():
    s=Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    # options.add_argument("--incognito")
    options.add_argument('--headless')
    driver = webdriver.Chrome(service=s,options=options)
    # driver.minimize_window()
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

def get_header():
    url = 'https://fundinfo.com/en'
    header = {
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'cookie': getCookie(url),
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
    }
    return header

def db_insert(df):
    import mysql.connector
    result = df[['master id','price','date']].values.tolist()
    try:
        db_conn = mysql.connector.connect(host='54.237.79.6',user='rentech_user',database = 'rentech_db',password='N)baegbgqeiheqfi3e9314jnEkekjb',auth_plugin='mysql_native_password')
        cursor = db_conn.cursor()
        sql = """INSERT INTO `raw_data_test` (`id`, `master_id`, `indicator_id`, `value_data`, `json_data`, `data_type`, `ts_date`, `ts_hour`, `job_id`, `timestamp`) VALUES 
        (NULL, %s, 371, %s, NULL, 2, %s, '0:0:0', 12, NULL, NOW()) ON DUPLICATE KEY UPDATE 
        master_id = VALUES(master_id), indicator_id = VALUES(indicator_id), value_data = VALUES(value_data), json_data = VALUES(json_data),
        data_type = VALUES(data_type), ts_date = VALUES(ts_date) ,ts_hour = VALUES(ts_hour), job_id = VALUES(job_id), batch_id = VALUES(batch_id);"""
        cursor.executemany(sql, result)
        rows = cursor.rowcount
        print(f'{rows} rows inserted')
        db_conn.commit()
    except Exception as e:
        print(f'Exception: {e}')
    finally:
        if (db_conn.is_connected()):
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
        df = pd.read_csv(f"{domain}_data.csv")
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
        filtered_df.to_csv(f"{domain}_data.csv",columns=cols,index=False)
        return filtered_df
    except:
        pass

def prof_investor_scraper(header,isin,master_id):
    nav_price = ''
    nav_date = ''
    for country in ['LU','SG','HK','CH','GB','IE','DE','SE']:
        if 'SG' in isin:
            country = 'SG'
        url = f'https://fundinfo.com/en/{country}-prof/LandingPage/Data?skip=0&query={isin}&orderdirection='
        res = session.get(url,headers=header)
        soup = BeautifulSoup(res.text,'html5lib')
        data = json.loads(soup.text)
        try:
            nav_price = data['Data'][0]['S']['OFDY901035'].split('|')[0]
        except:
            pass
        try:
            date = data['Data'][0]['S']['OFDY901035'].split('|')[1]
            if datetime.strptime(date,'%Y-%m-%d').date() > datetime.now().date() - timedelta(days=10):
                nav_date = datetime.strftime(datetime.strptime(date,'%Y-%m-%d'),'%Y-%m-%d')
        except:
            pass
        
        if nav_price != '' and nav_date != '':
            row = [master_id,isin,nav_price,nav_date]
            write_output(row)
            return 0
        else:
            continue
    f = open(non_scraped_isin_file, 'a')
    f.write(f'{isin}\n')
    f.close()
    return 0

def isin_downloaded():
    isin_downloaded = []
    with open(f"{domain}_data.csv","r") as file:
        csvreader = csv.reader(file)
        header = next(csvreader)
        for row in csvreader:
            isin_downloaded.append(row[1])
    return isin_downloaded

def start_fundinfo_prof_scraper(case):
    csv_filter()
    downloaded_isin = isin_downloaded()
    header = get_header()
    df = pd.read_csv(data_file,encoding="utf-8")
    df = df.drop_duplicates(subset=['Master ID'])
    if case == 1:
        for i,row in df.iterrows():
            isin = row[0]
            master_id = row[2]
            if isin not in downloaded_isin and 'SG' not in isin:
                prof_investor_scraper(header,isin,master_id)
    if case == 2:
        for i,row in df.iterrows():
            isin = row[0]
            master_id = row[2]
            if isin not in downloaded_isin and 'SG' in isin:
                prof_investor_scraper(header,isin,master_id)
    df = csv_filter()
    # db_insert(df)
            
if __name__ == '__main__':
    start_fundinfo_prof_scraper()
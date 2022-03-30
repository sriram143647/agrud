from email import header
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from bs4 import BeautifulSoup
from datetime import datetime,timedelta
import csv
import numpy as np
import pandas as pd
import requests
import os
session = requests.session()
domain = os.getcwd().split('\\')[-1].replace(' ','_')
output_file = f"{domain}_data.csv"
non_scraped_isin_file = f"{domain}_non_scraped_data.csv"
for file in os.listdir():
    if 'MF List - Final' in file and '.csv' in file:
        data_file = os.getcwd()+'\\'+file
        break  

def get_driver():
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches',['enable-logging'])
    # options.add_argument("--incognito")
    options.add_argument('--headless')
    driver = webdriver.Chrome(ChromeDriverManager().install(),options=options)
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
    url = 'https://markets.ft.com/data'
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
    
def markets_ft_scraper(header,isin,master_id,curr):
    nav_price = ''
    nav_date = ''
    url = f'https://markets.ft.com/data/funds/tearsheet/summary?s={isin}:{curr}'
    res = session.get(url,headers=header)
    soup = BeautifulSoup(res.text,'html5lib')
    try:
        nav_price =  soup.find('span',{'class':'mod-ui-data-list__value'}).text.replace(',','').strip()
        if '%' in nav_price:
            nav_price = ''
        date_tag = soup.find('ul',{'class':'mod-tearsheet-overview__quote__bar'})
        try:
            date = date_tag.find_next_sibling('div').text.split(',')[1].strip().replace('as of ','').replace('.','').strip()
        except:
            try:
                date = ' '.join(date_tag.find_next_sibling('div').text.split(',')[1].strip().replace('as of ','').replace('.','').split(':')[0].split(' ')[0:-1]).strip()
            except:
                pass
        try:
            if datetime.strptime(date,'%b %d %Y').date() > datetime.now().date() - timedelta(days=10):
                nav_date = datetime.strftime(datetime.strptime(date,'%b %d %Y'),'%Y-%m-%d')
        except:
            pass
    except:
        pass    
    if nav_price != '' and nav_date != '':
        row = [master_id,isin,round(eval(nav_price),2),nav_date]
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

def start_markets_ft_scraper():
    csv_filter()
    header = get_header()
    downloaded_isin = isin_downloaded()
    df = pd.read_csv(data_file,encoding="utf-8")
    df = df.drop_duplicates(subset=['Master ID'])
    df = df[df['Currency'].notna()]
    df = df[~df['Symbol'].isin(downloaded_isin)]
    for i,row in df.iterrows():
        isin = row[0]
        curr = row[1]
        master_id = row[2]
        markets_ft_scraper(header,isin,master_id,curr)
    df = csv_filter()
    # db_insert(df)

if __name__ == '__main__':
    start_markets_ft_scraper()
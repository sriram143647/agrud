from datetime import datetime,timedelta
import csv
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import common_utility as cu
import concurrent.futures
session = requests.session()
domain = os.getcwd().split('\\')[-1].replace(' ','_')
output_file = f"{domain}_data.csv"
non_scraped_isin_file = f"{domain}_non_scraped_data.csv" 
for file in os.listdir():
    if 'MF List - Final' in file and '.csv' in file:
        data_file = os.getcwd()+'\\'+file
        break

def priv_investor_scraper(header,lst):
    nav_price = ''
    nav_date = ''
    isin = lst[0]
    master_id = lst[1]
    url = lst[2]
    res = session.get(url,headers=header)
    soup = BeautifulSoup(res.text,'html5lib')
    data = json.loads(soup.text)
    if data['Data'] == []:
        return 0
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
        row = [master_id,isin,round(eval(nav_price),2),nav_date]
        with open(output_file,"a",newline="") as file:
            writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writerow(row)
        return 0
    else:
        return 0

def start_fundinfo_priv_scraper(case):
    data_lst = []
    cu.csv_filter(output_file)
    downloaded_isin = pd.read_csv(output_file)['isin name'].values.tolist()
    header = cu.get_header('https://fundinfo.com/en')
    df = pd.read_csv(data_file,encoding="utf-8")
    df = df.drop_duplicates(subset=['Master ID'])
    df = df[~df['Symbol'].isin(downloaded_isin)]
    if case == 1:
        for i,row in df.iterrows():
            isin = row[0]
            master_id = row[2]
            for country in ['LU','SG','HK','CH','GB','IE','DE','SE']:
                url = f'https://fundinfo.com/en/{country}-priv/LandingPage/Data?skip=0&query={isin}&orderdirection='
                lst = [isin,master_id,url]
                data_lst.append(lst)
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as link_executor:
                [link_executor.submit(priv_investor_scraper,header,lst) for lst in data_lst]
    if case == 2:
        df = df[df['Symbol'].str.contains('SG')]
        for i,row in df.iterrows():
            isin = row[0]
            master_id = row[2]
            for country in ['LU','SG','HK','CH','GB','IE','DE','SE']:
                url = f'https://fundinfo.com/en/{country}-priv/LandingPage/Data?skip=0&query={isin}&orderdirection='
                lst = [isin,master_id,url]
                data_lst.append(lst)
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as link_executor:
            [link_executor.submit(priv_investor_scraper,header,lst) for lst in data_lst]
    df = cu.csv_filter(output_file)
    # db_insert(df)
            
if __name__ == '__main__':
    start_fundinfo_priv_scraper()
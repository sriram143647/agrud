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
import concurrent.futures
import common_utility as cu
session = requests.session()
domain = os.getcwd().split('\\')[-1].replace(' ','_')
output_file = f"{domain}_data.csv"
non_scraped_isin_file = f"{domain}_non_scraped_data.csv"
for file in os.listdir():
    if 'MF List - Final' in file and '.csv' in file:
        data_file = os.getcwd()+'\\'+file
        break  
  
def markets_ft_scraper(header,lst):
    nav_price = ''
    nav_date = ''
    isin = lst[0]
    master_id = lst[1]
    url = lst[2]
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
        with open(output_file,"a",newline="") as file:
            writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writerow(row)
        return 0
    else:
        return 0

def start_markets_ft_scraper():
    data_lst= []
    cu.csv_filter(output_file)
    downloaded_isin = pd.read_csv(output_file)['isin name'].values.tolist()
    header = cu.get_header('https://markets.ft.com/data')
    df = pd.read_csv(data_file,encoding="utf-8")
    df = df.drop_duplicates(subset=['Master ID'])
    df = df[df['Currency'].notna()]
    df = df[~df['Symbol'].isin(downloaded_isin)]
    for i,row in df.iterrows():
        isin = row[0]
        curr = row[1]
        master_id = row[2]
        url = f'https://markets.ft.com/data/funds/tearsheet/summary?s={isin}:{curr}'
        lst = [isin,master_id,url]
        data_lst.append(lst)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as link_executor:
        [link_executor.submit(markets_ft_scraper,header,lst) for lst in data_lst]
    df = cu.csv_filter(output_file)
    # cu.db_insert(df)

if __name__ == '__main__':
    start_markets_ft_scraper()
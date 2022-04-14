from bs4 import BeautifulSoup
from datetime import datetime,timedelta
import pandas as pd
import requests
import os
import csv
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

def fundsquare_scraper(header,lst):
    nav_price = ''
    nav_date = ''
    isin = lst[0]
    master_id = lst[1]
    url = lst[2]
    res = session.get(url,headers=header)
    soup = BeautifulSoup(res.text,'html5lib')
    try:
        pts = soup.find('table',{'width':'85%'}).find_all('td')
    except:
        pass

    try:
        for pt in pts:
            try:
                nav_price = pt.text.replace('\xa0',' ').split(' ')[0]
                if nav_price.replace('.', '',1).isdigit():
                    nav_price = pt.text.replace('\xa0',' ').split(' ')[0]
                else:
                    nav_price = ''
            except:
                pass

            try:
                nav_date = datetime.strftime(datetime.strptime(pt.text,'%d/%m/%Y'),'%Y-%m-%d')
                if datetime.strptime(pt.text,'%m/%d/%Y').date() > datetime.now().date() - timedelta(days=10):
                    nav_date = datetime.strftime(datetime.strptime(pt.text,'%d/%m/%Y'),'%Y-%m-%d')
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

def start_fundsquare_scraper():
    data_lst= []
    cu.csv_filter(output_file)
    downloaded_isin = pd.read_csv(output_file)['isin name'].values.tolist()
    header = cu.get_header('https://www.fundsquare.net/homepage')
    df = pd.read_csv(data_file,encoding="utf-8")
    df = df.drop_duplicates(subset=['Master ID'])
    df = df[~df['Symbol'].isin(downloaded_isin)]
    for i,row in df.iterrows():
        isin = row[0]
        master_id = row[2]
        url = f'https://www.fundsquare.net/search-results?ajaxContentView=renderContent&=undefined&search={isin}&isISIN=O&lang=EN&fastSearch=O'
        lst = [isin,master_id,url]
        data_lst.append(lst)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as link_executor:
        [link_executor.submit(fundsquare_scraper,header,lst) for lst in data_lst]
    df = cu.csv_filter(output_file)
    # cu.db_insert(df)

if __name__ == '__main__':
    start_fundsquare_scraper()
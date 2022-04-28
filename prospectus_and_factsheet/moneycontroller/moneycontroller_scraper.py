from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import csv
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import os
import requests
import concurrent.futures
session = requests.session()
domain = os.getcwd().split('\\')[-1].replace(' ','_')
output_file = f'{domain}_data_links.csv'

def write_header():
    with open(output_file,"a",newline="") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(['master id','isin name','factsheet link','prospectus link'])

def write_output(data):
    with open(output_file,"a",newline="") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(data)

def get_driver():
    s=Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
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
    link = 'https://www.moneycontroller.co.uk/'
    header = {
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'cookie': getCookie(link),
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
    }
    return header

def moneycontroller_gen_case(header,lst):
    factsheet_link = ''
    prospectus_link = ''
    isin = lst[0]
    master_id = lst[1]
    url = lst[2]
    res = session.get(url,headers=header)
    soup = BeautifulSoup(res.text,'html5lib')
    try:
        link = soup.find('td',{'class':'nome_fondo'}).find('a').get('href')
    except:
        row = [master_id,isin,factsheet_link,prospectus_link]
        write_output(row)
        return 0
    res2 = session.get(link,headers=header)
    soup2 = BeautifulSoup(res2.text,'html5lib')
    links = soup2.find('ul',{'class':'doc_list'}).find_all('li')
    for link in links:
        if 'fact sheet' in link.find('a').text.lower():
            if factsheet_link == '':
               factsheet_link = link.find('a').get('href')
        if 'prospectus' == link.find('a').text.lower():
                if prospectus_link == '':
                    prospectus_link = link.find('a').get('href')
        if factsheet_link != '' and prospectus_link != '':
            row = [master_id,isin,factsheet_link,prospectus_link]
            write_output(row)
            return 0

def csv_filter():
    filtered_df = pd.DataFrame()
    unique_isin = []
    cols = ["master id","isin name","factsheet link","prospectus link"]
    try:
        df = pd.read_csv(output_file)
    except FileNotFoundError:
        write_header()
        return 0
    for isin,grouped_df in df.groupby(by=['isin name']):
        for i,row in grouped_df.iterrows():
            if row[2] is not np.nan and row[3] is not np.nan:
                if isin not in unique_isin:
                    unique_isin.append(isin)
                    temp_df = pd.DataFrame([row],columns=cols)
                    filtered_df = pd.concat([filtered_df,temp_df])
    try:
        filtered_df.to_csv(output_file,columns=cols,index=False)
    except:
        pass

def start_moneycontroller_scraper():
    data_lst = []
    csv_filter()
    downloaded_isin = pd.read_csv(output_file)['isin name'].values.tolist()
    header = get_header()
    df = pd.read_csv(data_file,encoding="utf-8")
    df = df.drop_duplicates(subset=['master_id'])
    df = df[~df['symbol'].isin(downloaded_isin)]
    for i,row in df.iterrows():
        isin = row[3]
        master_id = row[0]
        url = f'https://www.moneycontroller.co.uk/return-performance-funds-etfs?search={isin}'
        lst = [isin,master_id,url]
        data_lst.append(lst)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as link_executor:
        [link_executor.submit(moneycontroller_gen_case,header,lst) for lst in data_lst]
    csv_filter()

if __name__ == '__main__':
    for file in os.listdir():
        if 'Factsheet_Prospectus' in file and '.csv' in file:
            data_file = os.getcwd()+'\\'+file
            break  
    start_moneycontroller_scraper()
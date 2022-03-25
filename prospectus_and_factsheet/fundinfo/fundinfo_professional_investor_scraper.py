from email import header
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
session = requests.session()
domain = os.getcwd().split('\\')[-1].replace(' ','_')
output_file = f"{domain}_data.csv"
non_scraped_isin_file = f"{domain}_non_scraped_data.csv" 
for file in os.listdir():
    if 'Factsheet_Prospectus' in file and '.csv' in file:
        data_file = os.getcwd()+'\\'+file
        break


def get_driver():
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
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
    url = 'https://fundinfo.com/en'
    header = {
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'cookie': getCookie(url),
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
    }
    return header

def write_header():
    with open(output_file,"a",newline="") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(["master id","isin name","factsheet link","prospectus link"])

def write_output(data):
    with open(output_file,"a",newline="") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(data)

def csv_filter():
    filtered_df = pd.DataFrame()
    unique_isin = []
    cols = ["master id","isin name","factsheet link","prospectus link"]
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

def prof_investor_scraper(header,isin,master_id):
    factsheet_link = ''
    prospectus_link = ''
    for country in ['LU','SG','HK','CH','GB','IE','DE','SE']:
        url = f'https://fundinfo.com/en/{country}-prof/LandingPage/Data?skip=0&query={isin}&orderdirection='
        res = session.get(url,headers=header)
        soup = BeautifulSoup(res.text,'html5lib')
        data = json.loads(soup.text)
        try:
            for d in data['Data'][0]['D']['MR']:
                if d['Language'] == 'EN':
                    if factsheet_link == '':
                        factsheet_link = d['Url']
                        break
                else:
                    continue
        except:
            pass
        
        try:
            for d in data['Data'][0]['D']['PR']:
                if d['Language'] == 'EN':
                    if prospectus_link == '':
                        prospectus_link = d['Url']
                        break
                else:
                    continue
        except:
            pass
        if factsheet_link != '' and prospectus_link != '':
            row = [master_id,isin,factsheet_link,prospectus_link]
            write_output(row)
            return 0
    row = [master_id,isin,factsheet_link,prospectus_link]
    write_output(row)
    return 0

def isin_downloaded():
    isin_downloaded = []
    with open(output_file,"r") as file:
        csvreader = csv.reader(file)
        header = next(csvreader)
        for row in csvreader:
            isin_downloaded.append(row[1])
    return isin_downloaded

def start_fundinfo_prof_scraper():
    csv_filter()
    downloaded_isin = isin_downloaded()
    header = get_header()
    df = pd.read_csv(data_file,encoding="utf-8")
    df = df.drop_duplicates(subset=['master_id'])
    df = df[~df['symbol'].isin(downloaded_isin)]
    for i,row in df.iterrows():
        isin = row[3]
        master_id = row[0]
        prof_investor_scraper(header,isin,master_id)
    csv_filter()
            
if __name__ == '__main__':
    start_fundinfo_prof_scraper()
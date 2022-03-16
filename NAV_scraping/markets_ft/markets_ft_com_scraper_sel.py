from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
import csv
import time
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import os
domain = os.getcwd().split('\\')[-1].replace(' ','_')
output_file = f"{domain}_data.csv"
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

def get_driver():
    s=Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument("--incognito")
    # options.add_argument('--headless')
    driver = webdriver.Chrome(service=s,options=options)
    # driver.minimize_window()
    return driver

def markets_ft_scraper(driver,isin,master_id):
    start_tm = datetime.now()
    nav_price = ''
    nav_date = ''

    try:
        search_in = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//*[@placeholder="Search securities"]')))
        search_in.clear()
    except:
        pass

    try:
        search_in = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//*[@placeholder="Search securities"]')))
        search_in.send_keys(isin)
    except:
        pass

    try:
        ele_lst = WebDriverWait(driver,3).until(EC.visibility_of_all_elements_located((By.CLASS_NAME,'mod-ui-autocomplete__suggestion')))
        for ele in ele_lst:
            if (isin in ele.text or 'fund' in ele.text.lower()) and  (isin in ele.text or 'funds' in ele.text.lower()):
                ele.click()
                break
            else:
                row = [master_id,isin,nav_price,nav_date]
                write_output(row)
                end_tm = datetime.now()
                print(f'Time taken to run isin {isin} is {end_tm-start_tm}')
                return 0
    except:
        row = [master_id,isin,nav_price,nav_date]
        write_output(row)
        end_tm = datetime.now()
        print(f'Time taken to run isin {isin} is {end_tm-start_tm}')
        return 0

    time.sleep(5)
    soup = BeautifulSoup(driver.page_source,'html5lib')
    nav_price = soup.find('span',{'class':'mod-ui-data-list__value'}).text
    if '%' in nav_price:
        nav_price = ''
    date_tag = soup.find('ul',{'class':'mod-tearsheet-overview__quote__bar'})
    try:
        date = date_tag.find_next_sibling('div').text.split(',')[1].strip().replace('as of ','').replace('.','')
        nav_date = datetime.strftime(datetime.strptime(date,'%b %d %Y'),'%Y-%m-%d')
    except:
        try:
            date = ' '.join(date_tag.find_next_sibling('div').text.split(',')[1].strip().replace('as of ','').replace('.','').split(':')[0].split(' ')[0:-1])
            nav_date = datetime.strftime(datetime.strptime(date,'%b %d %Y'),'%Y-%m-%d')
        except:
            pass
    if nav_price != '' and nav_date != '':
        row = [master_id,isin,nav_price,nav_date]
        write_output(row)
        end_tm = datetime.now()
        print(f'Time taken to run isin {isin} is {end_tm-start_tm}')
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
    downloaded_isin = isin_downloaded()
    driver = get_driver()
    link = 'https://markets.ft.com/data/'
    driver.get(link)
    df = pd.read_csv(data_file,encoding="utf-8")
    df = df.drop_duplicates(subset=['Security ID'])
    for i,row in df.iterrows():
        isin = row[0]
        master_id = row[1]
        if isin not in downloaded_isin and 'SG' not in isin:
            markets_ft_scraper(driver,isin,master_id)
    driver.quit()
    csv_filter()

if __name__ == '__main__':
    for file in os.listdir():
        if 'MF List - Final' in file and '.csv' in file:
            data_file = os.getcwd()+'\\'+file
            break  
    start_markets_ft_scraper()

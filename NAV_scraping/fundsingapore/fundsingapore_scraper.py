from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException
from datetime import datetime
import csv
import time
import pandas as pd
import numpy as np
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
        df = pd.read_csv(output_file)
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
        filtered_df.to_csv(output_file,columns=cols,index=False)
    except:
        pass

def get_driver():
    s=Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    driver = webdriver.Chrome(service=s,options=options)
    # driver.minimize_window()
    return driver

def fundsingapore_scraper(driver,isin,master_id):
    print(isin)
    start_tm = datetime.now()
    nav_price = ''
    nav_date = ''

    try:
        search_in = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//*[@class="SearchInput_searchInput__y_s8m"]/section/input')))
        search_in.clear()
    except:
        pass

    try:
        search_in = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//*[@class="SearchInput_searchInput__y_s8m"]/section/input')))
        search_in.send_keys(isin)
    except:
        pass

    try:
        search_btn = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//*[@class="SearchInput_searchInput__y_s8m"]/button')))
        search_btn.click()
    except:
        pass

    try:
        fund_ele = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//*[@class="tr tr0"]/div[2]/header/h6')))
        driver.execute_script("arguments[0].click();",fund_ele)
    except TimeoutException:
        row = [master_id,isin,nav_price,nav_date]
        write_output(row)
        return 0
    except:
        pass
    
    for _ in range(10):
        try:
            WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//*[@class="FundDetailHeader_fdHeader__head__2_9Cn"]/hgroup/h1')))
            break
        except:
            continue
        
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        # Scroll down to bottom
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        
        # Calculate new scroll height and compare with last scroll height
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    time.sleep(5)
    soup = BeautifulSoup(driver.page_source,'html5lib')
    try:
        links = soup.find('section',{'class':'FundDocuments_fundDocuments__3tpgn'}).find_all('a')
        for link in links:
            if 'prospectus' in  link.get('title').lower(): 
                if prospectus_link == '':
                    try:
                        prospectus_link = soup.find('section',{'class':'FundDocuments_fundDocuments__3tpgn'}).find('a',{'title':'Prospectus'}).get('href')
                    except:
                        prospectus_link = ''

            if 'factsheet' in link.get('title').lower():
                if factsheet_link == '':
                    try:
                        factsheet_link = soup.find('section',{'class':'FundDocuments_fundDocuments__3tpgn'}).find('a',{'title':'Factsheet'}).get('href')
                    except:
                        factsheet_link = ''

            if factsheet_link != '' and prospectus_link != '':
                row = [master_id,isin,factsheet_link,prospectus_link]
                write_output(row)
                driver.quit()
                return 0
    except:
        pass
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

def start_fundsingapore_scraper():
    csv_filter()
    downloaded_isin = isin_downloaded()
    driver = get_driver()
    link = 'https://fundsingapore.com/fund-library'
    driver.get(link)
    df = pd.read_csv(data_file,encoding="utf-8")
    df = df.drop_duplicates(subset=['Security ID'])
    for i,row in df.iterrows():
        isin = row[0]
        master_id = row[1]
        if isin not in downloaded_isin and 'SG' in isin:
            fundsingapore_scraper(driver,isin,master_id)
    driver.quit()
    csv_filter()

if __name__ == '__main__':
    for file in os.listdir():
        if 'MF List' in file and '.csv' in file:
            data_file = os.getcwd()+'\\'+file
            break  
    start_fundsingapore_scraper()
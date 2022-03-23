from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import csv
import time
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import os
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
    # options.add_argument('--headless')
    driver = webdriver.Chrome(service=s,options=options)
    # driver.minimize_window()
    return driver

def moneycontroller_gen_case(driver,isin,master_id):
    print(isin)
    factsheet_link = ''
    prospectus_link = ''

    try:
        accept_ele = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//button[@class="iubenda-cs-accept-btn iubenda-cs-btn-primary"]')))
        accept_ele.click()
    except:
        pass
    
    try:
        search_in = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//*[@id="titolo_isin"]')))
        search_in.clear()
    except:
        try:
            search_in = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//div[@class="searchform"]/input')))
            search_in.clear()
        except:
            pass

    try:
        search_in = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//*[@id="titolo_isin"]')))
        search_in.send_keys(isin)
    except:
        try:
            search_in = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//div[@class="searchform"]/input')))
            search_in.send_keys(isin)
        except:
            pass

    try:
        search_btn = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//*[@type="submit"]')))
        search_btn.click()
    except:
        try:
            search_btn = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//div[@class="searchform"]/button')))
            search_btn.click()
        except:
            pass
    
    try:
        fund_link = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//td[@class="nome_fondo"]/a')))
        fund_link.click()
    except TimeoutException:
        row = [master_id,isin,factsheet_link,prospectus_link]
        write_output(row)
        return 0
    except Exception as e:
        pass    

    time.sleep(10)
    soup = BeautifulSoup(driver.page_source,'html5lib')
    links = soup.find('ul',{'class':'doc_list'}).find_all('li')
    for link in links:
        if 'fact sheet' in link.find('a').text.lower():
            if factsheet_link == '':
               factsheet_link = link.find('a').get('href')
               print('factsheet link is found')
        if 'prospectus' == link.find('a').text.lower():
                if prospectus_link == '':
                    prospectus_link = link.find('a').get('href')
                    print('prospectus link is found')
        if factsheet_link != '' and prospectus_link != '':
            row = [master_id,isin,factsheet_link,prospectus_link]
            write_output(row)
            return 0
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
                    filtered_df = filtered_df.append(pd.DataFrame([row],columns=cols),ignore_index=True)
    try:
        filtered_df.to_csv(output_file,columns=cols,index=False)
    except:
        pass

def isin_downloaded():
    isin_downloaded = []
    with open(output_file,"r") as file:
        csvreader = csv.reader(file)
        header = next(csvreader)
        for row in csvreader:
            isin_downloaded.append(row[1])
    return isin_downloaded

def start_moneycontroller_scraper():
    csv_filter()
    downloaded_isin = isin_downloaded()
    driver = get_driver()
    link = 'https://www.moneycontroller.co.uk/'
    driver.get(link)
    try:
        accept_ele = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//button[@class="iubenda-cs-accept-btn iubenda-cs-btn-primary"]')))
        accept_ele.click()
    except:
        pass
    df = pd.read_csv(data_file,encoding="utf-8")
    df = df.drop_duplicates(subset=['master_id'])
    df = df[~df['symbol'].isin(downloaded_isin)]
    for i,row in df.iterrows():
        isin = row[3]
        master_id = row[0]
        moneycontroller_gen_case(driver,isin,master_id)
    driver.quit()
    csv_filter()

if __name__ == '__main__':
    for file in os.listdir():
        if '(Factsheet & Prospectus)' in file and '.csv' in file:
            data_file = os.getcwd()+'\\'+file
            break  
    start_moneycontroller_scraper()
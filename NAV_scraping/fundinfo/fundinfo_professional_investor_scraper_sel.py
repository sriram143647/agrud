from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import csv
import time
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import os
country_change = 0
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
    # options.add_argument("--incognito")
    # options.add_argument('--headless')
    driver = webdriver.Chrome(service=s,options=options)
    # driver.minimize_window()
    return driver

def prof_investor_scraper(driver,isin,master_id):
    global country_change
    print(isin)
    start_tm = datetime.now()
    nav_price = ''
    nav_date = ''
    try:
        accept_btn = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//*[@id="qc-cmp2-ui"]/div[2]/div/button[2]')))
        accept_btn.click()
    except:
        pass
    country_list = ['Luxembourg','Singapore','Hong&#32;Kong','Switzerland','United&#32;Kingdom','Ireland','Germany','Sweden']
    for country in country_list:
        if country_change == 1:
            try:
                country_change = WebDriverWait(driver,3).until(EC.element_to_be_clickable((By.XPATH,'//*[@class="fund-market selected"]')))
                driver.execute_script("arguments[0].click();", country_change)
            except:
                pass

        try:
            country_select = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//*[@class="country-selector"]')))
            country_select.click()
        except:
            pass

        try:
            select_country = WebDriverWait(driver,3).until(EC.element_to_be_clickable((By.XPATH,f'//*[@data-cname="{country}"]')))
            driver.execute_script("arguments[0].click();", select_country)
        except:
            pass

        try:
            investor_type = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//*[@for="professional"]/span')))
            driver.execute_script("arguments[0].click();", investor_type)
        except Exception as e:
            pass

        try:
            tick_mark = WebDriverWait(driver,3).until(EC.element_to_be_clickable((By.XPATH,'//*[@for="fund-market-checkbox-mandatory"]/span')))
            driver.execute_script("arguments[0].click();", tick_mark)
        except:
            pass
        
        try:
            confirm = WebDriverWait(driver,3).until(EC.element_to_be_clickable((By.XPATH,'//*[@class="btn fundmarket-btn confirm"]')))
            confirm.click()
        except:
            pass

        try:
            search_ele = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//*[@placeholder="Place your search request here ( fund name / ISIN / WKN / Valor No )"]')))
            search_ele.clear()
        except:
            pass

        try:
            search_ele = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//*[@placeholder="Place your search request here ( fund name / ISIN / WKN / Valor No )"]')))
            search_ele.send_keys(str(isin))
        except:
            pass

        try:
            btn = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//*[@class="searchButton"]')))
            btn.click()
        except:
            pass
        
        count = 1
        while count < 5:
            time.sleep(10)
            try:
                drop_down_btn = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//*[@class="collapse collapse-open"]')))
                drop_down_btn.click()
            except:
                pass

            soup = BeautifulSoup(driver.page_source,'html5lib')
            try:
                data = soup.find_all('div',{'class':'cellContent'})[1]
                try:
                    nav_price = list(data.find_all('span',{'class':'nowrap'})[0].stripped_strings)[0].replace(',','')
                except:
                    nav_price = ''

                try:
                    nav_date = list(data.find_all('span',{'class':'nowrap'})[1].stripped_strings)[0]
                except:
                    nav_date = ''
                
                if nav_price != '' and nav_date != '':
                    row = [master_id,isin,nav_price,nav_date]
                    write_output(row)
                    end_tm = datetime.now()
                    print(f'Time taken to run isin {isin} is {end_tm-start_tm}')
                    return 0
                else:
                    country_change = 1
            except:
                country_change = 1
                break
        if country_change == 1:
            continue
    row = [master_id,isin,nav_price,nav_date]
    write_output(row)
    end_tm = datetime.now()
    print(f'Time taken to run isin {isin} is {end_tm-start_tm}')
    return 0
   
def isin_downloaded():
    isin_downloaded = []
    with open(f"{domain}_data.csv","r") as file:
        csvreader = csv.reader(file)
        header = next(csvreader)
        for row in csvreader:
            isin_downloaded.append(row[1])
    return isin_downloaded

def start_fundinfo_prof_scraper():
    csv_filter()
    downloaded_isin = isin_downloaded()
    driver = get_driver()
    link = 'https://www.fundinfo.com/en'
    driver.get(link)
    df = pd.read_csv(data_file,encoding="utf-8")
    df = df.drop_duplicates(subset=['Security ID'])
    for i,row in df.iterrows():
        isin = row[0]
        master_id = row[1]
        if isin not in downloaded_isin and 'SG' not in isin:
            prof_investor_scraper(driver,isin,master_id)
    csv_filter()
            
if __name__ == '__main__':
    for file in os.listdir():
        if 'MF List' in file and '.csv' in file:
            data_file = os.getcwd()+'\\'+file
            break  
    start_fundinfo_prof_scraper()
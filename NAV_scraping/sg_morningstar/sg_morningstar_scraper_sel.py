from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from datetime import datetime
import csv
import time
import pandas as pd
import numpy as np
import os
domain = os.getcwd().split('\\')[-1].replace(' ','_')
output_file = f"{domain}_data.csv"
for file in os.listdir():
    if 'MF List' in file and '.csv' in file:
        data_file = os.getcwd()+'\\'+file
        break
non_scraped_isin_file = f"{domain}_non_scraped_data.csv"
   

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

def get_driver():
    s=Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    # options.add_argument("--incognito")
    options.add_argument('--headless')
    driver = webdriver.Chrome(service=s,options=options)
    # driver.minimize_window()
    return driver

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
        return filtered_df
    except:
        pass

def morningstar_gen_case(driver,isin, master_id):
    nav_price = ''
    nav_date = ''

    try:
        edition_btn = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//input[@id="btn_individual"]')))
        edition_btn.click()
    except:
        pass

    try:
        search_in = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//input[@id="quoteSearch"]')))
        search_in.clear()
    except:
        pass

    try:
        search_in = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//input[@id="quoteSearch"]')))
        search_in.send_keys(isin)
    except:
        pass

    try:
        click_ele = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//li[@class="ac_odd ac_over"]')))
        click_ele.click()
    except Exception as e:
        pass
   
    time.sleep(10)
    soup = BeautifulSoup(driver.page_source,'html5lib')
    try:
        try:
            nav_price = list(soup.find('li',{'class':'sal-snap-panel'}).find('div',{'class':'sal-dp-value'}).stripped_strings)[0].replace('/','').strip()
        except:
            nav_price = ''
        
        try:
            nav_date = soup.find_all('div',{'class':'sal-row'})[2].find_all('span')[1].text.replace('NAV as of','').replace('|','').strip()
            nav_date = datetime.strftime(datetime.strptime(nav_date,'%b %d, %Y'),'%Y-%m-%d')
        except:
            nav_date = ''
    except:
        pass
    if nav_price != '' and nav_date != '':
        row = [master_id,isin,nav_price,nav_date]
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

def start_sg_morningstar_scraper():
    csv_filter()
    downloaded_isin = isin_downloaded()
    df = pd.read_csv(data_file,encoding="utf-8")
    df = df.drop_duplicates(subset=['Master ID'])
    driver = get_driver()
    link = 'https://sg.morningstar.com/sg/'
    driver.get(link)
    start = 0
    for i,row in df.iterrows():
        isin = row[0]
        master_id = row[2]
        if isin not in downloaded_isin and 'SG' in isin:
            morningstar_gen_case(driver,isin,master_id)
    df = csv_filter()

if __name__ == '__main__':
    start_sg_morningstar_scraper()
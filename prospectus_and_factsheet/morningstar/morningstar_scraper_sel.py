from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException
import csv
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import os
client_user = 'phillipssec'
client_pass = '52c4533bcc966857'
domain = os.getcwd().split('\\')[-1].replace(' ','_')
output_file = f'{domain}_data_links.csv'
for file in os.listdir():
    if '(Factsheet & Prospectus)' in file and '.csv' in file:
        data_file = os.getcwd()+'\\'+file
        break  

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

def morningstar_gen_case(isin, master_id,driver=''):
    print(isin)
    factsheet_link = ''
    prospectus_link = ''

    try:
        search_in = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//input[@id="SearchInput"]')))
        search_in.clear()
    except:
        pass

    try:
        search_in = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//input[@id="SearchInput"]')))
        search_in.send_keys(isin)
    except:
        pass

    try:
        click_ele = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//div[@class="ac_results"]')))
        click_ele.click()
    except TimeoutException:
        row = [master_id,isin,factsheet_link,prospectus_link]
        write_output(row)
        return 0
    except: 
        pass

    soup = BeautifulSoup(driver.page_source,'html5lib')
    try:
        rows = soup.find('table',{'id':'listContentBox'}).find_all('tr')    
        for row in rows:
            try:
                if 'prospectus' == row.find('label').text.lower() and 'english' == row.find('td',{'class':'language'}).text.lower():
                    if prospectus_link == '':
                        prospectus_link = 'https://doc.morningstar.com/'+row.find('a',{'class':'g-pdf-icon g-vv'}).get('href')
                if 'factsheet' == row.find('label').text.lower() and 'english' == row.find('td',{'class':'language'}).text.lower():
                    if factsheet_link == '':
                        factsheet_link = 'https://doc.morningstar.com/'+row.find('a',{'class':'g-pdf-icon g-vv'}).get('href')
                if factsheet_link != '' and prospectus_link != '':
                    break
            except:
                continue
    except:
        pass
    row = [master_id,isin,factsheet_link,prospectus_link]
    write_output(row)
    return 0

def morningstar_driver_login(driver):
    link = 'https://doc.morningstar.com/Home.aspx'
    driver.get(link)
    try:
        sign_in = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//*[@id="institutional"]')))
        driver.execute_script("arguments[0].click();",sign_in)
    except:
        pass

    try:
        client_id = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//input[@name="clientid"]')))
        client_id.send_keys(client_user)
    except Exception as e:
        pass

    try:
        password = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//input[@name="key"]')))
        password.send_keys(client_pass)
    except:
        pass

    try:
        sign_in = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//*[@id="submitButton"]')))
        driver.execute_script("arguments[0].click();",sign_in)
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

def start_morningstar_scraper():
    csv_filter()
    downloaded_isin = isin_downloaded()
    df = pd.read_csv(data_file,encoding="utf-8")
    df = df.drop_duplicates(subset=['master_id'])
    df = df[~df['symbol'].isin(downloaded_isin)]
    driver = get_driver()
    morningstar_driver_login(driver)
    for i,row in df.iterrows():
        isin = row[3]
        master_id = row[0]
        morningstar_gen_case(isin,master_id,driver)
    driver.quit()
    csv_filter()


if __name__ == '__main__':
    start_morningstar_scraper()
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import requests
import csv
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
import os
import concurrent.futures
session = requests.session()
client_user = 'phillipssec'
client_pass = '52c4533bcc966857'
login = 0
domain = os.getcwd().split('\\')[-1].replace(' ','_')
output_file = f'{domain}_data_links.csv'
for file in os.listdir():
    if 'Factsheet_Prospectus' in file and '.csv' in file:
        data_file = os.getcwd()+'\\'+file
        break

def write_header():
    with open(output_file,"a",newline="") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(['master id','isin name','factsheet link','prospectus link'])

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
    return driver

def getCookie():
    driver = get_driver()
    morningstar_driver_login(driver)
    cookies_list = driver.get_cookies()
    cookies_json = {}
    for cookie in cookies_list:
        cookies_json[cookie['name']] = cookie['value']
    cookies_string = str(cookies_json).replace("{", "").replace("}", "").replace("'", "").replace(": ", "=").replace(",", ";")
    driver.quit()
    return cookies_string

def get_header():
    header = {
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'cookie': getCookie(),
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
    }
    return header

def get_isin_url(header,isin,master_id):
    tm_stmp = datetime.timestamp(datetime.now())
    url = f'https://doc.morningstar.com/ajaxService/AutoComplete.aspx?q={isin}&limit=150&timestamp={int(tm_stmp)}'
    res = session.get(url,headers=header)
    if res.text != '':
        investment_id =  res.text.replace('\r','').replace('\n','').split('|')[-2]
        main_url = f'https://doc.morningstar.com/dochistory.aspx?secid={investment_id}'
        return [isin,master_id,main_url]

def morningstar_gen_case(header,lst):
    factsheet_link = ''
    prospectus_link = ''
    isin = lst[0]
    master_id = lst[1]
    main_url = lst[2]
    res = session.get(main_url,headers=header)
    soup = BeautifulSoup(res.text,'html5lib')
    for table in soup.find('table',{'id':'listContentBox'}).find_all('tbody'):
        if 'factsheet' in table.text.lower():
            rows = table.find_all('tr')
            for row in rows:
                try:
                    if 'factsheet' == row.find('label').text.lower() and 'english' == row.find('td',{'class':'language'}).text.lower():
                        if factsheet_link == '':
                            factsheet_link = 'https://doc.morningstar.com/'+row.find('a',{'class':'g-pdf-icon g-vv'}).get('href')
                except:
                    continue
        if 'prospectus' in table.text.lower():
            rows = table.find_all('tr')
            for row in rows:
                try:
                    if 'prospectus' == row.find('label').text.lower() and 'english' == row.find('td',{'class':'language'}).text.lower():
                        if prospectus_link == '':
                            prospectus_link = 'https://doc.morningstar.com/'+row.find('a',{'class':'g-pdf-icon g-vv'}).get('href')
                except:
                    continue
    row = [master_id,isin,factsheet_link,prospectus_link]
    with open(output_file,"a",newline="") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(row)
    return 0
    
def start_morningstar_scraper():
    data_lst= []
    csv_filter()
    downloaded_isin = pd.read_csv(output_file)['isin name'].values.tolist()
    header = get_header()
    df = pd.read_csv(data_file,encoding="utf-8")
    df = df.drop_duplicates(subset=['master_id'])
    df = df[~df['symbol'].isin(downloaded_isin)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as link_executor:
        future_list = [link_executor.submit(get_isin_url,header,row[3],row[0]) for i,row in df.iterrows()]
        for f in concurrent.futures.as_completed(future_list):
            data_lst.append(f.result())
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as link_executor:
        [link_executor.submit(morningstar_gen_case,header,lst) for lst in data_lst]
    csv_filter()
            
if __name__ == '__main__':
    start_morningstar_scraper()
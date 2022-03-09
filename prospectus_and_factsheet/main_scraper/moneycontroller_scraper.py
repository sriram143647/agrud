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
from bs4 import BeautifulSoup
import os
domain = os.getcwd().split('\\')[-1].replace(' ','_')

def write_header():
    with open(f"{domain}_data_links.csv","a",newline="") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(['master id','isin name','factsheet link','prospectus link'])

def write_output(data):
    with open(f"{domain}_data_links.csv","a",newline="") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(data)

def get_driver():
    s=Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    driver = webdriver.Chrome(service=s,options=options)
    # driver.minimize_window()
    return driver

def moneycontroller_gen_case(isin,master_id):
    print(isin)
    factsheet_link = ''
    prospectus_link = ''
    driver = get_driver()
    link = 'https://www.moneycontroller.co.uk/'
    driver.get(link)

    try:
        accept_ele = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//button[@class="iubenda-cs-accept-btn iubenda-cs-btn-primary"]')))
        accept_ele.click()
    except:
        pass

    try:
        search_in = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//div[@class="searchform"]/input')))
        search_in.send_keys(isin)
    except:
        pass

    try:
        search_btn = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//div[@class="searchform"]/button')))
        search_btn.click()
    except:
        pass
    
    try:
        fund_link = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//td[@class="nome_fondo"]/a')))
        fund_link.click()
    except TimeoutException:
        driver.quit()
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
            driver.quit()
            return 0
    row = [master_id,isin,factsheet_link,prospectus_link]
    write_output(row)
    driver.quit()
    return 0

def get_data():
    write_header()
    isin_downloaded = []
    with open(f"{domain}_data_links.csv","r") as file:
        csvreader = csv.reader(file)
        header = next(csvreader)
        for row in csvreader:
            isin_downloaded.append(row[1])
    df = pd.read_csv(data_file,encoding="utf-8")
    df = df.drop_duplicates(subset=['Security ID'])
    for i,row in df.iterrows():
        isin = row[4]
        master_id = row[0]
        if isin not in isin_downloaded:
            moneycontroller_gen_case(isin,master_id)

if __name__ == '__main__':
    for file in os.listdir():
        if '(Factsheet & Prospectus)' in file and '.csv' in file:
            data_file = os.getcwd()+'\\'+file
            break  
    get_data()
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException
import csv
import time
import pandas as pd
from bs4 import BeautifulSoup
import os
client_user = 'phillipssec'
client_pass = '52c4533bcc966857'
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

def fundsingapore_gen_case(isin,master_id):
    print(isin)
    factsheet_link = ''
    prospectus_link = ''
    driver = get_driver()
    link = 'https://fundsingapore.com/fund-library'
    driver.get(link)
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
        driver.quit()
        row = [master_id,isin,factsheet_link,prospectus_link]
        write_output(row)
        return 0
    except Exception as e:
        pass
    
    for _ in range(15):
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
        if 'SG' in isin:
            if isin not in isin_downloaded:
                fundsingapore_gen_case(isin,master_id)

if __name__ == '__main__':
    for file in os.listdir():
        if '(Factsheet & Prospectus)' in file and '.csv' in file:
            data_file = os.getcwd()+'\\'+file
            break  
    get_data()
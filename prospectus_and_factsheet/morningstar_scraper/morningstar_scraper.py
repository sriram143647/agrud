from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import csv
import pandas as pd
from bs4 import BeautifulSoup
import os
client_user = 'phillipssec'
client_pass = '52c4533bcc966857'
login = 0
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

def morningstar_gen_case(isin, master_id,driver=''):
    global login
    if login == 0:
        driver = get_driver()
        driver = morningstar_driver_login(driver)
        login = 1
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
    return driver

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
    start = 0
    for i,row in df.iterrows():
        isin = row[4]
        master_id = row[0]
        if isin not in isin_downloaded:
            if start == 0:
                driver = morningstar_gen_case(isin,master_id)
                start = 1
            else:
                morningstar_gen_case(isin,master_id,driver)


if __name__ == '__main__': 
    for file in os.listdir():
        if '(Factsheet & Prospectus)' in file and '.csv' in file:
            data_file = os.getcwd()+'\\'+file
            break 
    get_data()
from timeit import repeat
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
import csv
import time
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import os
import multiprocessing

domain = os.getcwd().split('\\')[-1].replace(' ','_')
for file in os.listdir():
    if 'Factsheet_Prospectus' in file and '.csv' in file:
        data_file = os.getcwd()+'\\'+file
        break

def write_header():
    with open(f"{domain}_data_links.csv","a",newline="") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(['master id','isin name','factsheet link','prospectus link'])

def write_output(data):
    with open(f"{domain}_data_links.csv","a",newline="") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(data)

def write_dataset(dataset):
    with open(f"{domain}_data_links.csv","w",newline="") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        for data in dataset:
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
        df = pd.read_csv(f"{domain}_data_links.csv")
    except FileNotFoundError:
        header_flag = 1
        return header_flag
    for isin,grouped_df in df.groupby(by=['isin name']):
        for i,row in grouped_df.iterrows():
            if row[2] is not np.nan and row[3] is not np.nan:
                if isin not in unique_isin:
                    unique_isin.append(isin)
                    filtered_df = filtered_df.append(pd.DataFrame([row],columns=cols),ignore_index=True)
    try:
        filtered_df.to_csv(f"{domain}_data_links.csv",columns=cols,index=False)
    except:
        pass


def script_exe(driver,xpath):
    try:
        ele = WebDriverWait(driver,3).until(EC.element_to_be_clickable((By.XPATH,str(xpath))))
        driver.execute_script("arguments[0].click();",ele)
    except Exception as e:
        print(f'Exception: {e}')
        pass


def ele_clickable(driver,xpath):
    try:
        ele = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,str(xpath))))
        ele.click()
    except:
        pass

def ele_visible(driver,xpath):
    try:
        ele = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,str(xpath))))
        ele.click()
    except Exception as e:
        print(f'Exception: {e}')
        pass

def hover_ele(driver,xpath):
    try:
        ele = driver.find_element(By.XPATH,str(xpath))
        hover_act = ActionChains(driver).move_to_element(ele)
        hover_act.perform()
    except Exception as e:
        print(f'Exception: {e}')
        pass

def priv_gen_case(args_list):
    isin = args_list[0]
    master_id = args_list[1]
    driver = get_driver()
    print(isin)
    country_change = 0
    factsheet_link = ''
    prospectus_link = ''
    link = 'https://www.fundinfo.com/en'
    driver.get(link)
    accept_btn_xpath = '//*[@id="qc-cmp2-ui"]/div[2]/div/button[2]'
    ele_clickable(driver,accept_btn_xpath)
    country_list = ['Luxembourg','Singapore','Hong&#32;Kong','Switzerland','United&#32;Kingdom','Ireland','Germany','Sweden']
    for country in country_list:
        if country_change == 1:
            country_chnage_xpath = '//*[@class="fund-market selected"]'
            script_exe(driver,country_chnage_xpath)

        country_select_xpath = '//*[@class="country-selector"]'
        ele_visible(driver,country_select_xpath)
        
        select_country_xpath = f'//*[@data-cname="{country}"]'
        script_exe(driver,select_country_xpath)

        # investor_type_xpath = '//*[@for="professional"]/span'
        # script_exe(driver,investor_type_xpath)

        tick_mark_xpath = '//*[@for="fund-market-checkbox-mandatory"]/span'
        script_exe(driver,tick_mark_xpath)

        confirm_btn_xpath = '//*[@class="btn fundmarket-btn confirm"]'
        ele_clickable(driver,confirm_btn_xpath)

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

        search_btn_xpath = '//*[@class="searchButton"]'
        ele_visible(driver,search_btn_xpath)

        count = 1
        while count < 5:
            time.sleep(10)
            hover_ele_xpath_1 = '//*[@data-name="MR"]'
            hover_ele(driver,hover_ele_xpath_1)

            drop_down_btn_xpath = '//*[@class="collapse collapse-open"]'
            ele_visible(driver,drop_down_btn_xpath)

            hover_ele_xpath_2 = '//*[@data-name="PR"]'
            hover_ele(driver,hover_ele_xpath_2)

            soup = BeautifulSoup(driver.page_source,'html5lib')
            try:
                if isin in soup.find('div',{'class':'fund-info'}).find_all('div')[-1].text:                
                    try:
                        if factsheet_link == '':
                            factsheet_link = soup.find('div',{'data-name':'MR'}).find('a').get('href')
                            if f'en_{isin}' not in factsheet_link:
                                factsheet_link = ''
                            print(f'factsheet link found')
                    except:
                        pass
                    
                    try:
                        if prospectus_link == ''    :
                            prospectus_link = soup.find('div',{'data-name':'PR'}).find('a').get('href')
                            if f'en_{isin}' not in factsheet_link:
                                factsheet_link = ''
                            print(f'prospectus link found')
                    except:
                        pass
                    
                    if factsheet_link != '' and prospectus_link != '':
                        country_change = 0
                        row = [master_id,isin,factsheet_link,prospectus_link]
                        write_output(row)
                        driver.quit()
                        return 0
                    else:
                        count += 1
                        country_change = 1
                        continue
            except:
                country_change = 1
                break
        if country_change == 1:
            continue
    row = [master_id,isin,factsheet_link,prospectus_link]
    write_output(row)
    driver.quit()

def prof_gen_case(args_list):
    isin = args_list[0]
    master_id = args_list[1]
    driver = get_driver()
    print(isin)
    country_change = 0
    factsheet_link = ''
    prospectus_link = ''
    link = 'https://www.fundinfo.com/en'
    driver.get(link)
    accept_btn_xpath = '//*[@id="qc-cmp2-ui"]/div[2]/div/button[2]'
    ele_clickable(driver,accept_btn_xpath)
    country_list = ['Luxembourg','Singapore','Hong&#32;Kong','Switzerland','United&#32;Kingdom','Ireland']
    for country in country_list:
        if country_change == 1:
            country_chnage_xpath = '//*[@class="fund-market selected"]'
            script_exe(driver,country_chnage_xpath)

        country_select_xpath = '//*[@class="country-selector"]'
        ele_visible(driver,country_select_xpath)
        
        select_country_xpath = f'//*[@data-cname="{country}"]'
        script_exe(driver,select_country_xpath)

        investor_type_xpath = '//*[@for="professional"]/span'
        script_exe(driver,investor_type_xpath)

        tick_mark_xpath = '//*[@for="fund-market-checkbox-mandatory"]/span'
        script_exe(driver,tick_mark_xpath)

        confirm_btn_xpath = '//*[@class="btn fundmarket-btn confirm"]'
        ele_clickable(driver,confirm_btn_xpath)

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

        search_btn_xpath = '//*[@class="searchButton"]'
        ele_visible(driver,search_btn_xpath)

        count = 1
        while count < 5:
            time.sleep(5)
            hover_ele_xpath_1 = '//*[@data-name="MR"]'
            hover_ele(driver,hover_ele_xpath_1)

            drop_down_btn_xpath = '//*[@class="collapse collapse-open"]'
            ele_visible(driver,drop_down_btn_xpath)

            hover_ele_xpath_2 = '//*[@data-name="PR"]'
            hover_ele(driver,hover_ele_xpath_2)

            soup = BeautifulSoup(driver.page_source,'html5lib')
            try:
                if isin in soup.find('div',{'class':'fund-info'}).find_all('div')[-1].text:                
                    try:
                        if factsheet_link == '':
                            factsheet_link = soup.find('div',{'data-name':'MR'}).find('a').get('href')
                            if f'en_{isin}' not in factsheet_link:
                                factsheet_link = ''
                            print(f'factsheet link found')
                    except:
                        pass
                    
                    try:
                        if prospectus_link == ''    :
                            prospectus_link = soup.find('div',{'data-name':'PR'}).find('a').get('href')
                            if f'en_{isin}' not in factsheet_link:
                                factsheet_link = ''
                            print(f'prospectus link found')
                    except:
                        pass
                    
                    if factsheet_link != '' and prospectus_link != '':
                        country_change = 0
                        row = [master_id,isin,factsheet_link,prospectus_link]
                        write_output(row)
                        driver.quit()
                        return 0
                    else:
                        count += 1
                        country_change = 1
                        continue
            except:
                country_change = 1
                break
        if country_change == 1:
            continue
    row = [master_id,isin,factsheet_link,prospectus_link]
    write_output(row)
    driver.quit()

def process():
    isin_downloaded = []
    with open(f"{domain}_data_links.csv","r") as file:
        csvreader = csv.reader(file)
        header = next(csvreader)
        for row in csvreader:
            isin_downloaded.append(row[1])
    df = pd.read_csv(data_file,encoding="utf-8")
    df = df.drop_duplicates(subset=['master_id'])
    for i,row in df.iterrows():
        isin = row[3]
        master_id = row[0]
        if isin not in isin_downloaded:
            p1 = multiprocessing.Process(target=priv_gen_case,args=([isin,master_id],))
            # p2 = multiprocessing.Process(target=prof_gen_case,args=([isin,master_id],))
            p1.start()
            # p2.start()
            p1.join()
            # p2.join()

if __name__ == '__main__':  
    header_flag = csv_filter()
    if header_flag == 1:
        write_header()
    process()
    csv_filter()
    process()

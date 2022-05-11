from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import pandas as pd
import pyautogui
from PIL import ImageGrab
import pytesseract
from pytesseract import Output
import time
import numpy as nm
import cv2
import csv
file_path = r'D:\\sriram\\agrud\\prospectus_and_factsheet\\'
data_file = file_path+'Global_MF_Factsheet_Prospectus - FINAL GLOBAL MF LIST.csv'
output_file = file_path+'scraped_data_links.csv'
pdf_files = r'D:\\sriram\\agrud\\prospectus_and_factsheet\\factsheet\\'
pytesseract.pytesseract.tesseract_cmd = (r"C:\Program Files\Tesseract-OCR\tesseract")

def get_driver():
    s=Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    # options.add_argument('--headless')
    driver = webdriver.Chrome(service=s,options=options)
    return driver

def new_approach(driver,isin,pdf_url):
    driver.get(pdf_url)
    time.sleep(10)
    pyautogui.hotkey('ctrl', 'f')
    pyautogui.write(isin)
    pyautogui.hotkey('enter')
    time.sleep(5)
    screen =  ImageGrab.grab(bbox =(500, 70, 1000, 200))
    cap = screen.convert('L')
    data=pytesseract.image_to_string(cap)
    print(data)
    if data != '':
        res = [f'isin {isin} found']
        print(res)
        with open('data.csv', mode='a', encoding='utf-8',newline="") as output_file:
            writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writerow(res)
    else:
        res = [f'isin {isin} not found']
        print(res)
        with open('data.csv', mode='a', encoding='utf-8',newline="") as output_file:
            writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writerow(res)        

driver = get_driver()
out_df = pd.read_csv(output_file)
for i,row in out_df.iterrows():
    master_id = row[0]
    isin = row[1]
    factsheet_pdf_url = row[2]
    new_approach(driver,isin,factsheet_pdf_url)
from bs4 import BeautifulSoup
from urllib.parse import unquote
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
from datetime import datetime,timedelta
import requests
from urllib.parse import quote
import json
import pandas as pd
import mysql.connector
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
import logging as log
log_file_path = r'D:\\sriram\\agrud\\daily_scrapers\\weekly_run\\static_bloomberg\\scraper_run_log.txt'
# log_file_path = '/home/ubuntu/rentech/daily_scrapers/daily_run/invoke_bloomberg/scraper_run_log.txt'
log.basicConfig(filename = log_file_path,filemode='a',level=log.INFO)
my_log = log.getLogger()
session = requests.Session()

def get_driver():
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument("--incognito")
    # options.add_argument('--headless')
    driver = webdriver.Chrome(ChromeDriverManager().install(),options=options)
    return driver

def get_cookie():
    url = 'https://www.bloomberg.com/quote/COINXBE:SS'
    driver = get_driver()
    driver.get(url)
    cookies_dict = driver.get_cookies()
    cookies_json = {}
    for cookie in cookies_dict:
        cookies_json[cookie['name']] = cookie['value']
    cookies_string = str(cookies_json).replace("{", "").replace("}", "").replace("'", "").replace(": ", "=").replace(",", ";")
    driver.quit()
    return cookies_string

def get_header():
    header = {
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'cookie':get_cookie(),
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
    }
    return header

def send_email(row_count=0,status=None,err_text=None):
    email_user = 'agrud.scrapersmail123@gmail.com'
    email_password = 'qwerty@123'
    email_send = 'agrud.2021@gmail.com'
    subject = f"bloomberg scraper result: {datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
    msg = MIMEMultipart()
    msg['From'] = email_user
    msg['To'] = email_send
    msg['Subject'] = subject
    body = f"Total records inserted: {row_count}\ncronjob status: {status}\nError:{err_text}"
    msg.attach(MIMEText(body,'plain'))
    text = msg.as_string()
    server = smtplib.SMTP('smtp.gmail.com',587)
    server.starttls()
    server.login(email_user,email_password)
    server.sendmail(email_user,email_send,text)
    server.quit()
    my_log.info('email sent')

def db_insert(result):
    try:
        db_conn = mysql.connector.connect(host='54.237.79.6',database='rentech_db',user='rentech_user',password='N)baegbgqeiheqfi3e9314jnEkekjb',auth_plugin='mysql_native_password')
        cursor = db_conn.cursor()
        sql = """INSERT INTO `raw_data` (`id`, `master_id`, `indicator_id`, `value_data`, `json_data`, `data_type`, `ts_date`, `ts_hour`, `job_id`, `timestamp`) VALUES 
        (NULL, %s, %s, %s, %s, %s, %s, '0:0:0', 2, NOW()) ON DUPLICATE KEY UPDATE 
        master_id = VALUES(master_id), indicator_id = VALUES(indicator_id), value_data = VALUES(value_data), json_data = VALUES(json_data),
        data_type = VALUES(data_type), ts_date = VALUES(ts_date) ,ts_hour = VALUES(ts_hour), job_id = VALUES(job_id), batch_id = VALUES(batch_id);"""
        cursor.executemany(sql, result)
        rows = cursor.rowcount
        db_conn.commit()
        my_log.info(f'{rows} rows inserted successfully')
    except Exception as e:
        my_log.info ("Mysql Error", e)
    finally:
        if (db_conn.is_connected()):
            cursor.close()
            db_conn.close()
            my_log.info("MySQL connection is closed")

def lambda_handler(event):
    rows = []
    symbols = []
    for master_id, Symbol in zip(event['master_id'], event['source_id']):
        symbols.append(Symbol)
    symbols = ','.join(symbols)
    quoted_symbols = quote(symbols)
    page_url = f'https://www.bloomberg.com/markets2/api/datastrip/{quoted_symbols}?locale=en&customTickerList=true'
    my_log.info(f'page url: {page_url}')
    driver = get_driver()
    driver.get(page_url)
    soup = BeautifulSoup(driver.page_source,'html5lib')
    try:
        j_data = json.loads(soup.text)
    except json.JSONDecodeError:
        my_log.info(f'Json error has occured')
        return 0
    event_df = pd.DataFrame.from_dict(event)
    for data in j_data:
        id  = data['id']
        
        master_id = event_df.loc[event_df['source_id'] == id]['master_id'].iloc[0]
        
        site_date = data['lastUpdate'].split('T')[0]
        my_log.info(f'coin: {id}, master id: {master_id} ,site date: {site_date}')     

        if master_id == '86831':
            data_dict = [{'name':'Cryptocurrencies','Equity_%':'100'}]
        else:      
            data_dict = [{'name':'Digital Asset','Equity_%':'100'}]
        j_data_dict = json.dumps(data_dict)
        row = [master_id,'742','0',j_data_dict,'1',site_date]
        rows.append(row)

        data_dict =[{'name':'Others','Net_Assets_%':'100'}]
        j_data_dict = json.dumps(data_dict)
        row = [master_id,'743','0',j_data_dict,'1',site_date]
        rows.append(row)
    
    db_insert(rows)


all_event = {   
    'source_id': ["COINETH:SS","COINXBT:SS","BTCE:GR","COINXBE:SS"], 
    'master_id' : ["84949","84950","86831","86833"]
    }

if __name__ == '__main__':
    my_log.info(f'----------------------started at:{datetime.now()}----------------------------')
    try:
        lambda_handler(all_event)
    except Exception as e:
        my_log.setLevel(log.ERROR)
        my_log.error(f'Error:{e}',exc_info=True)
        # send_email(status='Fail',err_text=str(e))
    my_log.setLevel(log.INFO)
    my_log.info(f'----------------------finished at:{datetime.now()}----------------------------')
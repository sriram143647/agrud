import subprocess
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox import firefox_profile
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from time import sleep
import json
import pandas as pd
import datetime
import json
from decimal import Decimal
import mysql.connector
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import mysql.connector.pooling
import mysql.connector
from mysql.connector import Error
from decimal import Decimal
subprocess.call("sudo rm -rf /tmp/*",shell = True)
import logging as log
#server paths
log_file_path = '/home/ubuntu/agrud-scrapers/daily_run/valueresearch/scraper_run_log.txt'
firefox_profile_path = '/home/ubuntu/agrud-scrapers/tor-browser_en-US/Browser/TorBrowser/Data/Browser/profile.default/'
driver_path = '/home/ubuntu/agrud-scrapers/geckodriver'
log.basicConfig(filename = log_file_path,filemode='a',level=log.INFO)
my_log = log.getLogger()

def get_data():
    mydb = mysql.connector.connect(host='34.67.106.166',database='rentech_db',user='testuser',password='SAdf!AdsWER!@Ew',auth_plugin='mysql_native_password')
    sql = """ SELECT * FROM web_scraping_masters where status =  'Y' and source_name = 'valueresearch' and json_structure_type = 0"""
    df = pd.read_sql(sql, con = mydb)
    risk = df[df['url']== 'https://www.valueresearchonline.com/funds']
    src_list = risk['source_identifier']
    src_list = list(src_list)
    indicator = risk[["master_id","indicator_id","source_indicator_name"]]
    df = indicator.loc[:, ~indicator.columns.duplicated()]
    source_map = {}
    for index, row in df.iterrows():
        if row['master_id'] not in source_map:
            source_map[row['master_id']] = []
        source_map[row['master_id']].append({row['indicator_id']:row['source_indicator_name']})

    master = []
    for i in source_map:
        master.append(i)
    src_list = list([i for j, i in enumerate(src_list) if i not in src_list[:j]]) 
    return src_list,master,source_map

def fetch_data(src_list,master,source_map):
    main_map = {}
    for source,master__id in zip(src_list,master):
        try:
            site_url = "https://www.valueresearchonline.com/funds/" + source

            profile = FirefoxProfile(firefox_profile_path)
            profile.set_preference('network.proxy.type', 5)
            profile.set_preference('network.proxy.socks', '127.0.0.1')
            profile.set_preference('network.proxy.socks_port', 9050)
            profile.set_preference("network.proxy.socks_remote_dns", False)
            profile.update_preferences()
            firefox_options = webdriver.FirefoxOptions()
            firefox_options.add_argument("--headless")
            driver = webdriver.Firefox(executable_path = driver_path, options = firefox_options)

            my_log.info(f"site url:{site_url}")
            driver.get(site_url)
            # my_log.info(driver.page_source)
            wait = WebDriverWait(driver,20)
            # try:
            #     site_date = wait.until(EC.visibility_of_element_located((By.XPATH,'//*[@id="trailing-returns-percentage"]/div/p[2]/small'))).text
            # except:
            #     site_date = wait.until(EC.visibility_of_element_located((By.XPATH,'//*[@id="trailing-returns-as-on-date"]/small'))).text
            # site_date = site_date.split(" ")[-1]
            # site_date = datetime.datetime.strptime(site_date, '%d-%b-%Y').strftime('%Y-%m-%d')
            site_date = '2022-03-31'
	        
            my_log.info(site_date)
            if master__id not in main_map:
                main_map[master__id] = {}

            TIME = "00:00:00"

            main_map[master__id]["time"] = TIME
            main_map[master__id]["date"] = site_date
            main_map[master__id]["indicators"] = {}
            
            my_log.info(master__id)
            my_log.info("----------------------------------------------------------------------")
            total_asset = wait.until(EC.visibility_of_element_located((By.XPATH,'//*[@id="basic-investment-table"]/tbody/tr[7]/td[2]'))).text
            if total_asset == "--":
                total_asset = ""
            else:
                total_asset = total_asset.split(" ")[1]
                total_asset = total_asset.replace(",","")
                total_asset = Decimal(total_asset)*Decimal(10000000)
            total_asset = str(total_asset)
            # my_log.info("total asset 29",total_asset)

            for indicator_id  in source_map[master__id]:
                if list(indicator_id.keys())[0] == 29:
                    main_map[master__id]["indicators"]["29"] = {"value_data":total_asset,"json_data":None}
            
            expense_ratio = wait.until(EC.visibility_of_element_located((By.XPATH,'//*[@id="basic-investment-table"]/tbody/tr[8]/td[2]'))).text
            if expense_ratio == "--":
                expense_ratio = ""
            else:    
                expense_ratio = expense_ratio.split("\n")[0].replace("%","")
                expense_ratio = float(expense_ratio)/100.0
            expense_ratio = str(expense_ratio)
            # my_log.info("expense ratio 13",expense_ratio)
            for indicator_id  in source_map[master__id]:
                if list(indicator_id.keys())[0] == 13:
                    main_map[master__id]["indicators"]["13"] = {"value_data":expense_ratio,"json_data":None}

            standard_deviation = wait.until(EC.visibility_of_element_located((By.XPATH,'//*[@id="risk-measures-table"]/tbody/tr[1]/td[3]'))).text
            if standard_deviation == "--":
                standard_deviation = ""
            standard_deviation = str(standard_deviation)
            # my_log.info("standard deviation 16",standard_deviation)
            for indicator_id  in source_map[master__id]:
                if list(indicator_id.keys())[0] == 16:
                    main_map[master__id]["indicators"]["16"] = {"value_data":standard_deviation,"json_data":None}
            
            sharp_ratio = wait.until(EC.visibility_of_element_located((By.XPATH,'//*[@id="risk-measures-table"]/tbody/tr[1]/td[4]'))).text
            if sharp_ratio == "--":
                sharp_ratio = ""
            sharp_ratio = str(sharp_ratio)
            
            # my_log.info("sharp ratio 18",sharp_ratio)
            for indicator_id  in source_map[master__id]:
                if list(indicator_id.keys())[0] == 18:
                    main_map[master__id]["indicators"]["18"] = {"value_data":sharp_ratio,"json_data":None}


            beta = wait.until(EC.visibility_of_element_located((By.XPATH,'//*[@id="risk-measures-table"]/tbody/tr[1]/td[6]'))).text
            if beta == "--":
                beta = ""
            beta = str(beta)
            # my_log.info("beta 1",beta)
            for indicator_id  in source_map[master__id]:
                if list(indicator_id.keys())[0] == 1:
                    main_map[master__id]["indicators"]["1"] = {"value_data":beta,"json_data":None}
            driver.quit()
        except Error as e:
            my_log.info(e)
            my_log.info(source)
    main_map = json.dumps(main_map)
    return main_map

def send_email(row_count=0,status=None,text=None):
    email_user = 'agrud.scrapersmail123@gmail.com'
    email_password = 'qwerty@123'
    email_send = 'agrud.2021@gmail.com'
    subject = f"valueresearch scraper result: {datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
    msg = MIMEMultipart()
    msg['From'] = email_user
    msg['To'] = email_send
    msg['Subject'] = subject
    body = f"Total records inserted: {row_count}\ncronjob status: {status}\nError:{text}"
    msg.attach(MIMEText(body,'plain'))
    text = msg.as_string()
    server = smtplib.SMTP('smtp.gmail.com',587)
    server.starttls()
    server.login(email_user,email_password)
    server.sendmail(email_user,email_send,text)
    server.quit()
    my_log.info('email sent')

def saveToSql(main_map):
    main_map = json.loads(main_map)
    sql_query_start = "INSERT INTO raw_data ( master_id, indicator_id, value_data, json_data, data_type,ts_date,ts_hour,job_id,batch_id) VALUES "
    sql_query_end = " ON DUPLICATE KEY UPDATE  master_id = VALUES(master_id), indicator_id = VALUES(indicator_id), value_data = VALUES(value_data),json_data = VALUES(json_data),data_type = VALUES(data_type), ts_date = VALUES(ts_date) ,ts_hour = VALUES(ts_hour) ,job_id = VALUES(job_id), batch_id = VALUES(batch_id);"
    query_data = ""
    for masterId in main_map:
        if 'date' in main_map[masterId]:
            db_date = main_map[masterId]['date']
            db_time = main_map[masterId]['time']
            for indicatorId in main_map[masterId]["indicators"]:
                db_value_data = main_map[masterId]["indicators"][indicatorId]["value_data"]
                db_json_data = main_map[masterId]["indicators"][indicatorId]["json_data"]

                query_data = query_data + "('"+str(masterId)+"','"+str(indicatorId)+"','"+str(db_value_data)+"',NULL,'"+str(0)+"','"+str(db_date)+"','"+str(db_time)+"','"+str(3)+"',NULL),"

    try:
        sql = sql_query_start + query_data[:-1] + sql_query_end
        connection_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="pynative_pool", pool_size=5, pool_reset_session=True, host='54.237.79.6',
                                                                      database='rentech_db', user='rentech_user', password='N)baegbgqeiheqfi3e9314jnEkekjb', auth_plugin='mysql_native_password', connect_timeout=600000)
        connection_object = connection_pool.get_connection()
        if connection_object.is_connected():
            cursor = connection_object.cursor()
            cursor.execute(sql)
            rows = cursor.rowcount
            my_log.info(f'{rows} records inserted successfully')
            connection_object.commit()
    except Error as e:
        my_log.info("Error while inserting to MySQL DB ", e)
        status = 'Fail'
        send_email(rows,status)
    finally:
        if(connection_object.is_connected()):
            cursor.close()
            connection_object.close()
            status = 'Success'
            send_email(rows,status)

def start():
    src_list,master,source_map = get_data()
    main_map = fetch_data(src_list,master,source_map)
    saveToSql(main_map)

if __name__ == "__main__":  
    my_log.info(f'----------------------started at:{datetime.datetime.now()}----------------------------')
    try:
        start()
    except Exception as e:
        my_log.setLevel(log.ERROR)
        my_log.error(f'Error:{e}',exc_info=True)
        send_email(status='Fail',text=str(e))
    my_log.setLevel(log.INFO)
    my_log.info(f'----------------------finished at:{datetime.datetime.now()}----------------------------')

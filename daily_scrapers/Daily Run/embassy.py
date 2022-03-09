import datetime
import pandas as pd
# from requests_html import HTMLSession
import mysql.connector.pooling
import mysql.connector
from mysql.connector import Error
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
from mysql.connector.connection import MySQLConnection
from mysql.connector import pooling
from bs4 import BeautifulSoup
import requests
from time import sleep
import json
import logging as log
import traceback
log_file_path = r'D:\\sriram\\agrud\\daily_scrapers\\Daily Run\\scraper_run_log.txt'
# log_file_path = '/home/ubuntu/agrud-scrapers/daily_run/embassy/scraper_run_log.txt'
log.basicConfig(filename = log_file_path,filemode='a',level=log.INFO)
my_log = log.getLogger()


data = {
    "ApiKey": "lstzFDEOhfFNMLikKa0am9mgEKLBl49T"
}
headers = {
    'accept': 'application/json, text/plain, /',
    'apikey': 'lstzFDEOhfFNMLikKa0am9mgEKLBl49T',
    'cookie': '_gcl_au=1.1.1031749567.1620717010; _gid=GA1.2.569350320.1620717014; _fbp=fb.1.1620717052793.1963018176; ELQCOUNTRY=ID; ELOQUA=GUID=E96ABC246973478F96A980F05B94991A; JSESSIONID=45841EEA01C76A0B4C57867089C36DE0; BIGipServerfundapi-dallas=2620594860.20480.0000; _uetsid=ef2b9370b22711ebabda6f28253729dd; _uetvid=ef2bba10b22711eb801f3546a1b928f5; _dc_gtm_UA-141496933-1=1; _ga_G8C0R44VCK=GS1.1.1620802548.3.1.1620803604.59; _ga=GA1.1.265733920.1620717013',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
    'x-api-realtime-e': 'eyJlbmMiOiJBMTI4R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.XmuAS3x5r-0MJuwLDdD4jNC6zjsY7HAFNo2VdvGg6jGcj4hZ4NaJgH20ez313H8An9UJrsUj8ERH0R8UyjQu2UGMUnJ5B1ooXFPla0LQEbN_Em3-IG84YPFcWVmEgcs1Fl2jjlKHVqZp04D21UvtgQ4xyPwQ-QDdTxHqyvSCpcE.ACRnQsNuTh1K_C9R.xpLNZ8Cc9faKoOYhss1CD0A4hG4m0M7-LZQ0fISw7NUHwzQs2AEo9ZXfwOvAj1fCbcE96mbKQo8gr7Oq1a2-piYXM1X5yNMcCxEaYyGinpnf6PGqbdr6zbYZdqyJk0KrxWVhKSQchLJaLGJOts4GlpqujSqJObJQcWWbkJQYKG9K7oKsdtMAKsHIVo5-0BCUbjKVnHJNsYwTsI7xn2Om8zGm4A.nBOuiEDssVFHC_N68tDjVA'
}

def get_data():
    mydb = mysql.connector.connect(host='34.67.106.166', database='rentech_db',user='testuser', password='SAdf!AdsWER!@Ew', auth_plugin='mysql_native_password')
    df_master = pd.read_sql("""
    SELECT *
    FROM web_scraping_masters
    WHERE master_id = 76694 AND source_name = 'Morningstar' AND (exchange_id = '10' AND security_type_id = '14') and json_structure_type = 0
                """, con=mydb)
    return df_master


def fetch_data(df_master):
    site_url = 'https://www.morningstar.com/' +  str(df_master['source_identifier'][0]) + '/quote'
    my_log.info(site_url)
    master__id = str(df_master['master_id'][0])
    my_log.info(master__id)
    main_map = {}
    main_map[master__id] = {}
    dat = "https://api-global.morningstar.com/sal-service/v1/stock/realTime/v3/0P0001H6LJ/data?secExchangeList=&random=0.030918954883963012&clientId=MDC&benchmarkId=category&version=3.31.0"

    for _ in range(3):
        try:
            date_response = requests.request("GET", dat, headers=headers).json()
            break
        except:
            sleep(2)
            continue
         
    site_date = date_response['lastUpdateTime'].split("T")[0]
    # s_date = datetime.datetime.strptime(site_date,'%Y-%m-%d') - datetime.timedelta(days=1)
    # site_date = s_date.strftime('%Y-%m-%d')
    my_log.info(site_date)

    TIME = "00:00:00"

    main_map[master__id]["time"] = TIME
    main_map[master__id]["date"] = site_date
    main_map[master__id]["indicators"] = {}
   
    req = requests.get(site_url).text
    embassyid = req.split('byId:{"')[1].split('":')[0]
   
    
    embas_url = "https://api-global.morningstar.com/sal-service/v1/stock/realTime/v3/{}/data?secExchangeList=&random=0.8744110924975148&clientId=MDC&benchmarkId=category&version=3.31.0".format(embassyid)
    for _ in range(3):
        try:
            embas_response = requests.get(embas_url, headers=headers).json()
            break
        except :
            sleep(2)
            continue

    market_cap = embas_response['marketCap']
    if market_cap == 0 or market_cap == 'None' or market_cap == None:
        pass
    else:
        main_map[master__id]["indicators"]["331"] = {
            "value_data": market_cap, "json_data": None}
    embas_year_low = embas_response['yearRangeLow']
    if embas_year_low == 0 or embas_year_low == 'None' or embas_year_low == None:
        pass
    else:
        main_map[master__id]["indicators"]["325"] = {
            "value_data": embas_year_low, "json_data": None}
    embas_year_high = embas_response['yearRangeHigh']
    if embas_year_high == 0 or embas_year_high == 'None' or embas_year_high == None:
        pass
    else:
        main_map[master__id]["indicators"]["323"] = {
            "value_data": embas_year_high, "json_data": None}

    embas_url_2 = "https://api-global.morningstar.com/sal-service/v1/stock/header/v2/data/{}/securityInfo?clientId=MDC&benchmarkId=category&version=3.31.0".format(
        embassyid)

    for _ in range(3):
        try:
            embas_response2 = requests.get(embas_url_2, headers=headers).json()
            break
        except Exception as e:
            sleep(2)
            continue

    price_sales = embas_response2['priceSale']
    if price_sales == 0 or price_sales == 'None' or price_sales == None:
        pass
    else:
        main_map[master__id]["indicators"]["340"] = {
            "value_data": price_sales, "json_data": None}
    price_book = embas_response2['priceBook']
    if price_book == 0 or price_book == 'None' or price_book == None:
        pass
    else:
        main_map[master__id]["indicators"]["352"] = {
            "value_data": price_book, "json_data": None}
    tr_url = "https://api-global.morningstar.com/sal-service/v1/stock/trailingTotalReturns/{}/data?dataType=d&clientId=MDC&benchmarkId=category&version=3.31.0".format(
        embassyid)

    for _ in range(3):
        try:
            tr_response = requests.get(tr_url, headers=headers).json()
            break
        except Exception as e:
            sleep(2)
            continue

    ytd = tr_response['trailingTotalReturnsList'][0]['trailingYearToDateReturn']
    if ytd == 0 or ytd == 'None' or ytd == None:
        pass
    else:
        main_map[master__id]["indicators"]["348"] = {
            "value_data": ytd, "json_data": None}
    one_yr = tr_response['trailingTotalReturnsList'][0]['trailing1YearReturn']
    if one_yr == 0 or one_yr == 'None' or one_yr == None:
        pass
    else:
        main_map[master__id]["indicators"]["7"] = {
            "value_data": one_yr, "json_data": None}
    pe_url = "https://api-global.morningstar.com/sal-service/v1/stock/keyratios/{}/data?clientId=MDC&benchmarkId=category&version=3.31.0".format(
        embassyid)
    for _ in range(3):
        try:
            pe_response = requests.get(pe_url, headers=headers).json()
            break
        except:
            sleep(3)
            continue

    
    pe_earn = pe_response['valuationRatio']['priceToEPS']
    if pe_earn == 0 or pe_earn == "None" or pe_earn == None:
        pass
    else:
        main_map[master__id]["indicators"]["336"] = {
            "value_data": pe_earn, "json_data": None}

    main_map = json.dumps(main_map)
    return main_map

def send_email(row_count=0,status=None):
    email_user = 'agrud.scrapersmail123@gmail.com'
    email_password = 'qwerty@123'
    email_send = 'agrud.2021@gmail.com'
    subject = f"embassy scraper result: {datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
    msg = MIMEMultipart()
    msg['From'] = email_user
    msg['To'] = email_send
    msg['Subject'] = subject
    body = f"Total new records: {row_count}\ncronjob status: {status}"
    msg.attach(MIMEText(body,'plain'))
    text = msg.as_string()
    server = smtplib.SMTP('smtp.gmail.com',587)
    server.starttls()
    server.login(email_user,email_password)
    server.sendmail(email_user,email_send,text)
    server.quit()
    my_log.info('email sent')

def saveToSql(main_map):
    status = ''
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

                query_data = query_data + "('"+str(masterId)+"','"+str(indicatorId)+"','"+str(
                    db_value_data)+"',NULL,'"+str(0)+"','"+str(db_date)+"','"+str(db_time)+"','"+str(1)+"',NULL),"

                # query_data = query_data + "('"+str(db_date)+"','"+str(db_time)+"','"+str(
                #     masterId)+"','"+str(indicatorId)+"',NULL,'"+str(db_value_data)+"'),"

    try:
        sql = sql_query_start + query_data[:-1] + sql_query_end
        connection_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="pynative_pool", pool_size=5, pool_reset_session=True, host='54.237.79.6',database='rentech_db', user='rentech_user', password='N)baegbgqeiheqfi3e9314jnEkekjb', auth_plugin='mysql_native_password', connect_timeout=600000)
        connection_object = connection_pool.get_connection()
        if connection_object.is_connected():
            cursor = connection_object.cursor()
            cursor.execute(sql)
            rows = cursor.rowcount
            my_log.info(f"{rows} records are inserted into db")
            connection_object.commit()
    except Error as e:
        my_log.info(f"Mysql Error: {e}")
        status = 'Fail'
        send_email(rows,status)
    finally:
        if(connection_object.is_connected()):
            status = 'Success'
            cursor.close()
            connection_object.close()
            my_log.info("MySQL connection is closed")
            send_email(rows,status)


def start():
    df_master = get_data()
    main_map = fetch_data(df_master)
    saveToSql(main_map)

if __name__ == "__main__":  
    my_log.info(f'----------------------started at:{datetime.datetime.now()}----------------------------')
    try:
        start()
    except Exception as e:
        my_log.setLevel(log.ERROR)
        my_log.error(f'Error:{e}',exc_info=True)
        error_stack = ''.join(traceback.format_stack()).strip()
        send_email(status='Fail')
    my_log.setLevel(log.INFO)
    my_log.info(f'----------------------finished at:{datetime.datetime.now()}----------------------------')
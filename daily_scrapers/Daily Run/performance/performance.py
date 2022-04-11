import datetime
import pandas as pd
import mysql.connector.pooling
import mysql.connector
from mysql.connector import Error
from mysql.connector.connection import MySQLConnection
from mysql.connector import pooling
from bs4 import BeautifulSoup
from time import sleep
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
from selenium import webdriver
import json
import logging as log
import traceback
log_file_path = '/home/ubuntu/agrud-scrapers/daily_run/performance/scraper_run_log.txt'
log.basicConfig(filename = log_file_path,filemode='a',level=log.INFO)
my_log = log.getLogger()

headers1 = {
    'apikey': 'lstzFDEOhfFNMLikKa0am9mgEKLBl49T',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
    'x-api-realtime-e': 'eyJlbmMiOiJBMTI4R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.XmuAS3x5r-0MJuwLDdD4jNC6zjsY7HAFNo2VdvGg6jGcj4hZ4NaJgH20ez313H8An9UJrsUj8ERH0R8UyjQu2UGMUnJ5B1ooXFPla0LQEbN_Em3-IG84YPFcWVmEgcs1Fl2jjlKHVqZp04D21UvtgQ4xyPwQ-QDdTxHqyvSCpcE.ACRnQsNuTh1K_C9R.xpLNZ8Cc9faKoOYhss1CD0A4hG4m0M7-LZQ0fISw7NUHwzQs2AEo9ZXfwOvAj1fCbcE96mbKQo8gr7Oq1a2-piYXM1X5yNMcCxEaYyGinpnf6PGqbdr6zbYZdqyJk0KrxWVhKSQchLJaLGJOts4GlpqujSqJObJQcWWbkJQYKG9K7oKsdtMAKsHIVo5-0BCUbjKVnHJNsYwTsI7xn2Om8zGm4A.nBOuiEDssVFHC_N68tDjVA'

}

api_header = {
    "ApiKey": "lstzFDEOhfFNMLikKa0am9mgEKLBl49T"
}

def get_data():
    mydb = mysql.connector.connect(host='34.67.106.166', database='rentech_db', user='testuser', password='SAdf!AdsWER!@Ew',auth_plugin='mysql_native_password')
    df = pd.read_sql("""SELECT *FROM web_scraping_masters where status =  'Y' and source_name = 'Morningstar' and json_structure_type = 0""", con=mydb)
    risk = df[df['url'] == 'https://www.morningstar.com/performance']
    src_list = risk['source_identifier']
    src_list = list(src_list)
    indicator = risk[["master_id", "indicator_id", "source_indicator_name"]]
    df = indicator.loc[:, ~indicator.columns.duplicated()]
    source_map = {}
    for index, row in df.iterrows():
        if row['master_id'] not in source_map:
            source_map[row['master_id']] = []

        source_map[row['master_id']].append(
            {row['indicator_id']: row['source_indicator_name']})

    master_ids = []
    for i in source_map:
        master_ids.append(i)
    src_ids = list([i for j, i in enumerate(src_list) if i not in src_list[:j]])
    return src_ids,master_ids,source_map

def fetch_data(src_ids,master_ids,source_map):
    main_map = {}
    for source_id, master_id in zip(src_ids, master_ids):
        portfolio_url = "https://www.morningstar.com/" + source_id + "/performance"
        site_url = "https://www.morningstar.com/" + source_id + "/quote"
        my_log.info(f'portfolio url: {portfolio_url}')
        my_log.info(f'site url: {site_url}')
        my_log.info(master_id)
        try:
            for _ in range(3):
                try:
                    req = requests.get(portfolio_url).text
                    if len(req.split('byId:{"')[1].split('}},')[0].split(',')) > 1:
                        morning_id = req.split('byId:{"')[1].split('":')[1].split(",")[1].split(":")[0]
                    else :
                        morning_id = req.split('byId:{"')[1].split('":')[0]
                    break
                except:
                    sleep(2)
                    continue
            # my_log.info(f"morning_id: {morning_id}")
            morning_star_realtime_api_url = f"https://api-global.morningstar.com/sal-service/v1/etf/quote/realTime/{morning_id}/data?secExchangeList=&random=0.05994721785880497&clientId=MDC&benchmarkId=category&version=3.31.0"
            # my_log.info(morning_star_realtime_api_url)
            retry = 0
            while retry < 3:
                try:
                    date_response = requests.request("GET", morning_star_realtime_api_url, headers=headers1).json()
                    break
                except Exception as e:
                    retry += 1
                    my_log.info(e)
                    sleep(2)
             
            site_date = date_response['asOfDateLastPriceFund'].split("T")[0]
            # s_date = datetime.datetime.strptime(site_date,'%Y-%m-%d') - datetime.timedelta(days=1)
            # site_date = s_date.strftime('%Y-%m-%d')  
            my_log.info(site_date)
            
            if master_id not in main_map:
                main_map[master_id] = {}
            TIME = "00:00:00"
            main_map[master_id]["time"] = TIME
            main_map[master_id]["date"] = site_date
            main_map[master_id]["indicators"] = {}
            # my_log.info(f'source id{source_id}')

            morning_star_trailingreturn_api_url = f"https://api-global.morningstar.com/sal-service/v1/etf/trailingReturn/v2/{morning_id}/data?locale=en&duration=daily&currency=&languageId=en&locale=en&clientId=MDC&benchmarkId=category&component=sal-components-mip-trailing-return&version=3.31.0"
            retry = 0
            while retry < 3:
                try:
                    json_data = requests.get(morning_star_trailingreturn_api_url, headers=api_header).json()
                    break
                except Exception as e:
                    retry += 1
                    my_log.info(e)
                    sleep(2)

            ytd = json_data['totalReturnPrice'][4]

            # my_log.info("ytd", ytd)
            if ytd == 0 or ytd == 'None' or ytd == None:
                pass
            else:
                for indicator_id in source_map[master_id]:
                    if list(indicator_id.keys())[0] == 348:
                        main_map[master_id]["indicators"]["348"] = {"value_data": ytd, "json_data": None}
            one_yr = json_data['totalReturnPrice'][5]
            # my_log.info("one year", one_yr)
            if one_yr == 0 or one_yr == 'None' or one_yr == None:
                pass
            else:
                for indicator_id in source_map[master_id]:
                    if list(indicator_id.keys())[0] == 7:
                        main_map[master_id]["indicators"]["7"] = {"value_data": one_yr, "json_data": None}
            three_yr = json_data['totalReturnPrice'][6]
            # my_log.info("thrwe year", three_yr)
            if three_yr == 0 or three_yr == 'None' or three_yr == None:
                pass
            else:
                for indicator_id in source_map[master_id]:
                    if list(indicator_id.keys())[0] == 8:
                        main_map[master_id]["indicators"]["8"] = {"value_data": three_yr, "json_data": None}
            five_yr = json_data['totalReturnPrice'][7]
            # my_log.info("five year", five_yr)
            if five_yr == 0 or five_yr == None or five_yr == 'None':
                pass
            else:
                for indicator_id in source_map[master_id]:
                    if list(indicator_id.keys())[0] == 9:
                        main_map[master_id]["indicators"]["9"] = {"value_data": five_yr, "json_data": None}
            ten_yr = json_data['totalReturnPrice'][8]
            # my_log.info("ten year", ten_yr)
            if ten_yr == None or ten_yr == 0 or ten_yr == 'None':
                pass
            else:
                for indicator_id in source_map[master_id]:
                    if list(indicator_id.keys())[0] == 584:
                        main_map[master_id]["indicators"]["584"] = {"value_data": ten_yr, "json_data": None}
            overallmorningstar = json_data['overallMorningstarRating']
            # my_log.info("start", overallmorningstar)
            if overallmorningstar == None or overallmorningstar == 0 or overallmorningstar == 'None':
                pass
            else:
                for indicator_id in source_map[master_id]:
                    if list(indicator_id.keys())[0] == 30:
                        main_map[master_id]["indicators"]["30"] = {"value_data": overallmorningstar, "json_data": None}
            my_log.info('--------------------------------------------------------------------')
        except:
            my_log.info("last except")
    main_map = json.dumps(main_map)
    return main_map

def send_email(row_count=0,status=None,text=None):
    email_user = 'agrud.scrapersmail123@gmail.com'
    email_password = 'qwerty@123'
    email_send = 'agrud.2021@gmail.com'
    subject = f"performance scraper result: {datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
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
                query_data = query_data + "('"+str(masterId)+"','"+str(indicatorId)+"','"+str(db_value_data)+"',NULL,'"+str(0)+"','"+str(db_date)+"','"+str(db_time)+"','"+str(1)+"',NULL),"
                # query_data = query_data + "('"+str(db_date)+"','"+str(db_time)+"','"+str(masterId)+"','"+str(indicatorId)+"',NULL,'"+str(db_value_data)+"'),"
    try:
        sql = sql_query_start + query_data[:-1] + sql_query_end
        connection_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="pynative_pool", pool_size=5, pool_reset_session=True, host='54.237.79.6', database='rentech_db', user='rentech_user', password='N)baegbgqeiheqfi3e9314jnEkekjb', auth_plugin='mysql_native_password', connect_timeout=600000)
        connection_object = connection_pool.get_connection()
        if connection_object.is_connected():
            cursor = connection_object.cursor()
            cursor.execute(sql)
            rows = cursor.rowcount
            my_log.info(f"{rows} records are inserted into db")
            connection_object.commit()
            my_log.info("inserted data in to db")
    except Error as e:
        my_log.info("Error while connecting to MySQL using Connection pool ", e)
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
    src_ids,master_ids,source_map = get_data()
    main_map = fetch_data(src_ids,master_ids,source_map)
    saveToSql(main_map)

if __name__ == "__main__":  
    my_log.info(f'----------------------started at:{datetime.datetime.now()}----------------------------')
    try:
        start()
    except Exception as e:
        my_log.setLevel(log.ERROR)
        my_log.error(f'Error:{e}',exc_info=True)
        error_stack = ''.join(traceback.format_stack()).strip()
        send_email(status='Fail',text=str(e)+'\nerror stacktrace:\n'+error_stack)
    my_log.setLevel(log.INFO)
    my_log.info(f'----------------------finished at:{datetime.datetime.now()}----------------------------')

import datetime
import pandas as pd
import mysql.connector.pooling
import mysql.connector
from mysql.connector import Error
from mysql.connector.connection import MySQLConnection
from mysql.connector import pooling
from bs4 import BeautifulSoup
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import requests
from time import sleep
import json
import traceback
import logging as log
log_file_path = '/home/ubuntu/agrud-scrapers/daily_run/quote/scraper_run_log.txt'
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
    mydb = mysql.connector.connect(host='34.67.106.166', database='rentech_db', user='testuser', password='SAdf!AdsWER!@Ew',auth_plugin='mysql_native_password')
    df = pd.read_sql("""SELECT * FROM web_scraping_masters where status =  'Y' and source_name = 'Morningstar' and json_structure_type = 0;""", con=mydb)
    risk = df[df['url'] == 'https://www.morningstar.com/quote']
    src_list = risk['source_identifier']
    src_list = list(src_list)
    indicator = risk[["master_id", "indicator_id", "source_indicator_name"]]
    df = indicator.loc[:, ~indicator.columns.duplicated()]
    source_map = {}
    for index, row in df.iterrows():
        if row['master_id'] not in source_map:
            source_map[row['master_id']] = []

        source_map[row['master_id']].append({row['indicator_id']: row['source_indicator_name']})

    master = []
    for i in source_map:
        master.append(i)
    src_list = list([i for j, i in enumerate(src_list) if i not in src_list[:j]])
    return src_list,master,source_map

def fetch_data(src_list,master,source_map):
    main_map = {}
    for source, master__id in zip(src_list, master):
        portfolio_url = "https://www.morningstar.com/" + source + "/quote"
        site_url = "https://www.morningstar.com/" + source + "/quote"
        my_log.info(f'portfolio url: {portfolio_url}')
        my_log.info(f'site url: {site_url}')
        try:

            for _ in range(3) :
                try :
                    req = requests.get(portfolio_url).text
                    if len(req.split('byId:{"')[1].split('}},')[0].split(',')) > 1:
                        morning_id = req.split('byId:{"')[1].split('":')[1].split(",")[1].split(":")[0]
                    else :
                        morning_id = req.split('byId:{"')[1].split('":')[0]
                    break
                except :
                    sleep(2)


            _date = "https://api-global.morningstar.com/sal-service/v1/etf/quote/realTime/{}/data?secExchangeList=&random=0.05994721785880497&clientId=MDC&benchmarkId=category&version=3.31.0".format(morning_id)
            for _ in range(3):
                try:
                    date_response = requests.request("GET", _date, headers=headers).json()
                    break
                except Exception as e:
                    sleep(2)
                

            my_log.info(master__id)
            site_date = date_response['asOfDateLastPriceFund'].split("T")[0]
            #s_date = datetime.datetime.strptime(site_date,'%Y-%m-%d') - datetime.timedelta(days=1)
            #site_date = s_date.strftime('%Y-%m-%d')
            my_log.info(site_date)
            # site_date = (datetime.datetime.strptime(site_date, "%Y-%m-%d") + datetime.timedelta(days = -1)).strftime("%Y-%m-%d")
            if master__id not in main_map:
                main_map[master__id] = {}

            TIME = "00:00:00"
            my_log.info('-------------------------------------------------------------')
            main_map[master__id]["time"] = TIME
            main_map[master__id]["date"] = site_date
            main_map[master__id]["indicators"] = {}


            yearhighlow = "https://api-global.morningstar.com/sal-service/v1/etf/quote/realTime/{}/data?secExchangeList=&random=0.9182722587492136&clientId=MDC&benchmarkId=category&version=3.31.0".format(morning_id)
            for _ in range(3):
                try:
                    response = requests.request("GET", yearhighlow, headers=headers).json()
                    break
                except Exception as e:
                    sleep(2)


            yearlow = response['yearRangeLow']
            if yearlow == 0 or yearlow == 'None' or yearlow == None:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 325:
                        main_map[master__id]["indicators"]["325"] = {"value_data":yearlow,"json_data":None}

            yearhigh = response['yearRangeHigh']
            if yearhigh == 0 or yearhigh == 'None' or yearhigh == None:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 323:
                        main_map[master__id]["indicators"]["323"] = {"value_data":yearhigh,"json_data":None}


            expense_ratio = "https://api-global.morningstar.com/sal-service/v1/etf/quote/v1/{}/data?fundServCode=&locale=en&clientId=MDC&benchmarkId=category&version=3.31.0".format(morning_id)
            for _ in range(3):
                try:
                    expense_response = requests.request("GET",expense_ratio,headers=data).json()
                    break
                except :
                    sleep(2)


            expense_ratio_value = expense_response['expenseRatio']
            if expense_ratio_value == 0 or expense_ratio_value == 'None' or expense_ratio_value == None:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 13:
                        main_map[master__id]["indicators"]["13"] = {"value_data":expense_ratio_value,"json_data":None}

        except Exception as e:
            print(f"Error {e} in url : {site_url}")

    main_map = json.dumps(main_map)
    return main_map
        
def send_email(row_count=0,status=None,text=None):
    email_user = 'agrud.scrapersmail123@gmail.com'
    email_password = 'qwerty@123'
    email_send = 'agrud.2021@gmail.com'
    subject = f"quote scraper result: {datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
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

                # query_data = query_data + "('"+str(db_date)+"','"+str(db_time)+"','"+str(
                #     masterId)+"','"+str(indicatorId)+"',NULL,'"+str(db_value_data)+"'),"

    try:
        sql = sql_query_start + query_data[:-1] + sql_query_end
        connection_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="pynative_pool", pool_size=5, pool_reset_session=True, host='54.237.79.6',
                                                                      database='rentech_db', user='rentech_user', password='N)baegbgqeiheqfi3e9314jnEkekjb', auth_plugin='mysql_native_password', connect_timeout=600000)
        connection_object = connection_pool.get_connection()
        if connection_object.is_connected():
            cursor = connection_object.cursor()
            cursor.execute(sql)
            rows = cursor.rowcount
            connection_object.commit()
            my_log.info(f"{rows} records are inserted into db")
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
        error_stack = ''.join(traceback.format_stack()).strip()
        send_email(status='Fail',text=str(e)+'\nerror stacktrace:\n'+error_stack)
    my_log.setLevel(log.INFO)
    my_log.info(f'----------------------finished at:{datetime.datetime.now()}----------------------------')

import mysql.connector
import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import mysql.connector.pooling
import mysql.connector
from mysql.connector import Error
from mysql.connector.connection import MySQLConnection
from mysql.connector import pooling
import logging as log
import traceback
log_file_path = r'D:\\sriram\\agrud\\daily_scrapers\\daily_run\\moneycontrol\\scraper_run_log.txt'
# log_file_path = '/home/ubuntu/agrud-scrapers/daily_run/moneycontrol/scraper_run_log.txt'
log.basicConfig(filename = log_file_path,filemode='a',level=log.INFO)
my_log = log.getLogger()

def get_data():
    mydb = mysql.connector.connect(host='34.67.106.166',database='rentech_db',user='testuser',password='SAdf!AdsWER!@Ew',auth_plugin='mysql_native_password')
    df = pd.read_sql("""
            SELECT *
                FROM web_scraping_masters where status =  'Y' and source_name = 'Moneycontrol' and json_structure_type = 0
                """, con = mydb)
    risk = df[df['url']== 'https://www.moneycontrol.com/india/stockpricequote']
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
    return master,src_list

def fetch_data(master,src_list):
    main_map = {}
    for master__id,source in zip(master,src_list):
        my_log.info(master__id)
        split_scid = source.split('/')[-1]
        try:
            site_url = "https://www.moneycontrol.com/india/stockpricequote/"
            source_url = source
            # driver.get(site_url+source_url)
            # soup = BeautifulSoup(site_url+source_url,'lxml')
            # my_log.info(site_url+source_url)
            # url = 'https://www.moneycontrol.com/india/stockpricequote/computers-software/tataconsultancyservices/TCS'
            # if source == 'miscellaneous/bharatbondetf-april2023/EBBET16222':
            #   techperiod = 'D'
            #   scid = "EBBET16222"
              
            # else:
            #   r = requests.get(site_url+source_url).text
            #   soup = BeautifulSoup(r, "lxml")
            #   script = soup.find_all('script')[33]
            #   techperiod = str(soup.find_all('script')[33]).split('techperiod')[0].split('techPeriod =')[1].split("';")[0].replace("'","")
            #   scid = str(soup.find_all('script')[33]).split('var scid = ')[1].split('";')[0].replace('"','').replace("'"," ")
            r = requests.get(site_url+source_url).text
            soup = BeautifulSoup(r, "lxml")
            # date_site = soup.find('p', class_ = 'nseasondate').text.split(' | ')[0].replace('As on ', '')
            # intdate = int(datetime.datetime.strptime(date,'%d %B, %Y').timestamp())
            # strdate = str(datetime.datetime.strptime(date,'%d %B, %Y').date())
            # strdate = datetime.datetime.strptime(date_site, '%d %b, %Y').strftime('%Y-%m-%d')
            # s_date = datetime.datetime.strptime(strdate,'%Y-%m-%d') - datetime.timedelta(days=1)
            # strdate = s_date.strftime('%Y-%m-%d')
            strdate = '2022-03-30'
            my_log.info(strdate)
            # date = int(date.strftime('%Y-%m-%d'))
            # techperiod = str(soup.find_all('script')[33]).split('techperiod')[0].split('techPeriod =')[1].split("';")[0].replace("'","")
            # scid = str(soup.find_all('script')[33]).split('var scid = ')[1].split('";')[0].replace('"','').replace("'"," ")
            api_url = 'https://priceapi.moneycontrol.com/pricefeed/techindicator/D/'+split_scid+'?fields=sentiments,pivotLevels,sma,ema'
            my_log.info(api_url)
            my_log.info('----------------------------------------------------------------')

            if master__id not in main_map:
                main_map[master__id] = {}

            TIME = "00:00:00"

            main_map[master__id]["time"] = TIME
            main_map[master__id]["date"] = strdate
            main_map[master__id]["indicators"] = {}

            res = requests.get(api_url).json()
            # my_log.info(res)
            fif_sma = res['data']['sma'][3]['value']
            two_hundred = res['data']['sma'][5]['value']
            # my_log.info("fifty Sma",fif_sma)
            # my_log.info("two_hundred",two_hundred)

            main_map[master__id]["indicators"]["530"] = {"value_data":str(fif_sma).strip(),"json_data":None}
            main_map[master__id]["indicators"]["3"] = {"value_data":str(two_hundred).strip(),"json_data":None}
            my_log.info('*' * 50)
        except Exception as e:
            my_log.info(e)
            my_log.info(source)
        
        
    main_map = json.dumps(main_map)
    return main_map

def send_email(row_count=0,status=None,text=None):
    email_user = 'agrud.scrapersmail123@gmail.com'
    email_password = 'qwerty@123'
    email_send = 'agrud.2021@gmail.com'
    subject = f"moneycontrol scraper result: {datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
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
                    db_value_data)+"',NULL,'"+str(0)+"','"+str(db_date)+"','"+str(db_time)+"','"+str(4)+"',NULL),"

    try:
        sql = sql_query_start + query_data[:-1] + sql_query_end
        connection_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="pynative_pool", pool_size=5, pool_reset_session=True, host='54.237.79.6',
                                                                      database='rentech_db', user='rentech_user', password='N)baegbgqeiheqfi3e9314jnEkekjb', auth_plugin='mysql_native_password', connect_timeout=600000)
        connection_object = connection_pool.get_connection()
        if connection_object.is_connected():
            cursor = connection_object.cursor()
            cursor.execute(sql)
            rows = cursor.rowcount
            my_log.info(f"{rows} records are inserted into db")
            connection_object.commit()
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
    master,src_list = get_data()
    main_map = fetch_data(master,src_list)
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

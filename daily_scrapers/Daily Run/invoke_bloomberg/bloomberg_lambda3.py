from urllib.parse import quote
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
from datetime import datetime
import requests
import json
import mysql.connector
import logging as log

from bloomberg_lambda4 import lambda_handler
filepath = r'D:\\sriram\\agrud\\daily_scrapers\\daily_run\\invoke_bloomberg\\'
# filepath = '/home/ubuntu/rentech/daily_scrapers/daily_run/invoke_bloomberg/'
log_file_path = filepath+'scraper_run_log.txt'
log.basicConfig(filename = log_file_path,filemode='a',level=log.INFO)
my_log = log.getLogger()
session = requests.Session()

def get_header():
    with open(filepath+'cookie.csv', mode='r', encoding='utf-8',newline="") as output_file:
        cookie = output_file.readline()
        cookie = cookie.replace('\r','').replace('\n','')
    header = {
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'cookie':cookie,
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
        # send_email(rows,'success')
    except Exception as e:
        my_log.info ("Mysql Error", e)
    finally:
        if (db_conn.is_connected()):
            cursor.close()
            db_conn.close()
            my_log.info("MySQL connection is closed")

def lambda_handler_func(event='',header=''):
    rows = []
    symbols = []
    event_dict = {}
    for master_id, Symbol in zip(event['master_id'], event['source_id']):
        symbols.append(Symbol)
        event_dict.update({Symbol : master_id})
    symbols = ','.join(symbols)
    quoted_symbols = quote(symbols)
    page_url = f'https://www.bloomberg.com/markets2/api/datastrip/{quoted_symbols}?locale=en&customTickerList=true'
    my_log.info(f'page url: {page_url}')
    res = session.get(page_url,headers=header)
    try:
        j_data = json.loads(res.text)
    except json.JSONDecodeError:
        my_log.info(f'Json error has occured')
        return 0
    for data in j_data:
        id  = data['id']
        master_id = event_dict[id]
        
        site_date = data['lastUpdate'].split('T')[0]
        my_log.info(f'coin: {id}, master id: {master_id} ,site date: {site_date}')

        ytd = data['totalReturnYtd']
        row = [master_id,'348',ytd,None,'0',site_date]
        rows.append(row)
        
        one_year = data['totalReturn1Year']
        if one_year != None:
            row = [master_id,'7',one_year,None,'0',site_date]
            rows.append(row)
            
        three_year = data['totalReturn3Year']
        if three_year != None:
            row = [master_id,'8',three_year,None,'0',site_date]
            rows.append(row)
            
        five_year = data['totalReturn5Year']
        if five_year != None:
            row = [master_id,'9',five_year,None,'0',site_date]
            rows.append(row)
            
        total_assets = data['totalAssets']
        row = [master_id,'29',(total_assets*1000000),None,'0',site_date]
        rows.append(row)
        
        expense_ratio = data['expenseRatio']
        row = [master_id,'13',(expense_ratio/100),None,'0',site_date]
        rows.append(row)
    db_insert(rows)

def lambda_handler(all_event):
    my_log.info(f'----------------------started at:{datetime.now()}----------------------------')
    try:
        lambda_handler_func(all_event)
    except Exception as e:
        my_log.setLevel(log.ERROR)
        my_log.error(f'Error:{e}',exc_info=True)
    my_log.setLevel(log.INFO)
    my_log.info(f'----------------------finished at:{datetime.now()}----------------------------')

# all event
all_event = {
  "source_id": [
    "COINETH:SS",
    "COINXBT:SS",
    "BTCE:GR",
    "3072:HK",
    "COINXBE:SS",
    "CYB:SP"
  ],
  "master_id": [
    "84949",
    "84950",
    "86831",
    "86835",
    "86833",
    "86719"
  ]
}

   
if __name__ == '__main__':
    lambda_handler(all_event)
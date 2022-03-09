import pandas as pd
import numpy as np
import json
import mysql.connector
import datetime
import pytz
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import traceback
import logging as log
# server file paths
# publish_date = datetime.datetime.today().strftime('%Y-%m-%d')
# master_id_file = '/home/ubuntu/rentech/cbonds_scrapers/issue_data_entry/Isin_to_masterid.json'
# col_indicator_file = '/home/ubuntu/rentech/cbonds_scrapers/issue_data_entry/Column_to_indicator_map.json'
# data_files_path = '/home/ubuntu/rentech/cbonds_scrapers/zipfiles/datafiles/'+publish_date+'/'
# log_file_path = '/home/ubuntu/rentech/cbonds_scrapers/issue_data_entry/scraper_run_log.txt'

# local file paths
publish_date = '2022-01-11'
master_id_file = r'D:\\sriram\\agrud\\cbonds_data_entry\\server_files\\issue_data_entry\\new_Isin_to_masterid.json'
col_indicator_file = r'D:\\sriram\\agrud\\cbonds_data_entry\\server_files\\issue_data_entry\\Column_to_indicator_map.json'
data_files_path = r'D:\\sriram\\agrud\\cbonds_data_entry\\server_files\\zipfiles\\data_files\\'+publish_date+'\\'
log_file_path = r'D:\\sriram\\agrud\\cbonds_data_entry\\server_files\\issue_data_entry\\scraper_run_log.txt'

with open(master_id_file, 'r') as f:
    isinToMasterid = json.load(f)
with open(col_indicator_file, 'r', encoding="utf-8") as f:
    colToIndicator = json.load(f)
log.basicConfig(filename="scraper_run_log.txt",filemode='a',level=log.INFO)
my_log = log.getLogger()

binaryColumns = ["amortizing security (yes/no)", "floating rate (yes/no)", "mortgage bonds (yes/no)", "perpetual (yes/no)", "restructing (yes/no)", "covered debt (yes/no)", "structured products (yes/no)", "subordinated debt (yes/no)", "dual currency bond", "foreign bond", "the flag that paper green bonds", "pik", "non-market issue (yes/no)", "redemption linked (yes/no)", "retail bonds", "securitisation", "sukuk (yes/no)", "trace-eligible securities"]
dateColumns = ["end of placement date", "start of trading", "start of placement date", "early redemption date", "maturity date", "next offer date (put/call) - to be specified", "settlement date", "current coupon date", "next offer date (call)", "next offer date (put)"]

def get_result():
    ts_date = '2021-12-06'
    try:
        df = pd.read_excel(data_files_path+'issues.xls')
    except:
        df = pd.read_csv(data_files_path+'issues.csv', sep=';', encoding='latin')
    df.columns = df.columns.str.strip().str.lower()

    df['master id'] = df['isin / isin regs'].map(isinToMasterid)

    df[binaryColumns] = df[binaryColumns].replace([[0], [1]], [["No"], ["Yes"]])
    if "сonvertable (yes/no)" in df.columns :
        df['сonvertable (yes/no)'] = df['сonvertable (yes/no)'].map({0 : "No", 1 : "Yes"})
    if "ñonvertable (yes/no)" in df.columns :
        df["ñonvertable (yes/no)"] = df["ñonvertable (yes/no)"].map({0 : "No", 1 : "Yes"})

    datetimeColumns = pd.to_datetime(df[dateColumns].stack(), format="%d.%m.%Y").unstack()
    df[datetimeColumns.columns] = datetimeColumns
    for col in df.select_dtypes('datetime').columns:
        df[col] = df[col]
    df.dropna(subset=['master id'],inplace=True)
    df = df.replace([np.NaN], ['NA'])
    df = df.replace([pd.NaT], ['NA'])

    result = []
    for i, row in df.iterrows():
        row2 = row.to_dict()
        masterId = row2['master id']
        for k, v in row2.items():
            if v == "NA":
                continue
            if k in colToIndicator:
                indicatorId = colToIndicator[k]
                if type(v) == pd.Timestamp:
                    json_data = None
                    dataType = 2
                    date = datetime.datetime.strftime(v,'%Y-%m-%d')
                    gmt = pytz.timezone("GMT")
                    value_data = gmt.localize(datetime.datetime.strptime(date, '%Y-%m-%d')).timestamp()
                elif type(v) == float or type(v) == int or  v.isnumeric(): 
                    dataType = 0
                    value_data = v
                    json_data = None
                elif v.isnumeric() == False:
                    json_data = json.dumps({'TEXT':v})
                    dataType = 3
                    value_data = 0
                result.append([masterId,indicatorId,value_data,json_data,dataType,ts_date])
    my_log.info('result set is created')
    return result

def db_insert(result):
    try:
        db_conn = mysql.connector.connect(host='54.237.79.6',database='rentech_db',user='rentech_user',password='N)baegbgqeiheqfi3e9314jnEkekjb',auth_plugin='mysql_native_password')
        cursor = db_conn.cursor()
        sql = """INSERT INTO `raw_data` (`id`, `master_id`, `indicator_id`, `value_data`, `json_data`, `data_type`, `ts_date`, `ts_hour`, `job_id`, `timestamp`) VALUES 
        (NULL, %s, %s, %s, %s, %s, %s, '0:0:0', 9, NOW()) ON DUPLICATE KEY UPDATE 
        master_id = VALUES(master_id), indicator_id = VALUES(indicator_id), value_data = VALUES(value_data), json_data = VALUES(json_data),
        data_type = VALUES(data_type), ts_date = VALUES(ts_date) ,ts_hour = VALUES(ts_hour), job_id = VALUES(job_id), batch_id = VALUES(batch_id);"""
        cursor.executemany(sql, result)
        rows = cursor.rowcount
        db_conn.commit()
        my_log.info(f'{rows} rows inserted successfully')
    except Exception as e:
        my_log.info ("Mysql Error", e)
        status = 'Fail'
        # send_email(0,status)
    finally:
        if (db_conn.is_connected()):
            status = 'Success'
            cursor.close()
            db_conn.close()
            my_log.info("MySQL connection is closed")
            # send_email(rows,status)

def send_email(row_count,status):
    email_user = 'agrud.scrapersmail123@gmail.com'
    email_password = 'qwerty@123'
    email_send = 'agrud.2021@gmail.com'
    subject = f"issue data ingestion results: {datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
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
    
if __name__ == "__main__":  
  my_log.info(f'----------------------started at:{datetime.datetime.now()}----------------------------')
  try:
    result = get_result()
    db_insert(result)
  except Exception as e:
    my_log.setLevel(log.ERROR)
    my_log.error(f'Error:{e}',exc_info=True)
    error_stack = ''.join(traceback.format_stack()).strip()
    send_email(status='Fail',text=str(e))
  my_log.setLevel(log.INFO)
  my_log.info(f'----------------------finished at:{datetime.datetime.now()}----------------------------')

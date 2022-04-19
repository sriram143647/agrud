import pandas as pd
import numpy as np
import json
import mysql.connector
import datetime
import pytz
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib,ssl
import logging as log
# dates
today_date = datetime.datetime.today().strftime('%Y-%m-%d')
publish_date = (datetime.datetime.strptime(today_date, '%Y-%m-%d').date() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
# today_date = '2022-04-19'
# publish_date = '2022-04-18'


# file paths
# data_files_path = '/home/ubuntu/rentech/cbonds_scrapers/zipfiles/datafiles/'+today_date+'/'
data_files_path = r'D:\\sriram\\agrud\\cbonds_data_entry\\server_files\\zipfiles\\data_files\\'+today_date+'\\'
# path = '/home/ubuntu/rentech/cbonds_scrapers/trading_data_entry/'
path = r'D:\\sriram\\agrud\\cbonds_data_entry\\server_files\\trading_data_entry\\'
master_id_file = path+'daily_missing_isin_to_masterid_map.json'
col_indicator_file = path+'col_to_indicator.json'
log_file_path = path+'scraper_run_log.txt'

log.basicConfig(filename = log_file_path,filemode='a',level=log.INFO)
my_log = log.getLogger()

with open(master_id_file, 'r') as f:
    isinToMasterid = json.load(f)
with open(col_indicator_file, 'r', encoding='utf-8') as f:
    colToIndicator = json.load(f)
stock_exchange_priority = ('Frankfurt S.E.','Stuttgart Exchange','Berlin Exchange','Dusseldorf SE','FINRA TRACE','Cbonds Estimation','Luxembourg S.E','Luxembourg S.E.','MiFID II Source 2 (APA, Post-trade reporting)','MiFID II Source 1 (APA, Post-trade reporting)','Hong Kong S.E.','SGX','US OTC Market','Other sources of prices','London S.E.','Euronext Paris','Taipei Exchange (OTC)','Nasdaq Dubai','Taipei Exchange (Trading System)')
dateColumns = ('trade date','put/сall date','maturity date')
close_price_priority = ['close price', 'indicative price', 'bid (at close)', 'ask (at close)']

def send_email(row_count=0,status=None,err_text=None):
    sender_email = 'agrud.scrapersmail123@gmail.com'
    email_password = 'qwerty@123'
    receivers_email_list = ["prince.chaturvedi@agrud.com","sayan.sinharoy@agrud.com","soumodip.pramanik@agrud.com","vidyut.lakhotia@agrud.com","bhavesh.bansal@agrud.com"]
    subject = f"Nav Scraping data ingestion: {datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ','.join(receivers_email_list)
    msg['Subject'] = subject
    body = f"Total records inserted: {row_count}\ncronjob status: {status}\nError:{err_text}"
    msg.attach(MIMEText(body,'plain'))
    text = msg.as_string()
    context = ssl.create_default_context()
    server = smtplib.SMTP('smtp.gmail.com',587)
    server.starttls(context=context)
    server.login(sender_email,email_password)
    server.sendmail(sender_email,receivers_email_list,text)
    server.quit()
    my_log.info(f'email sent')

def get_result():
    global today_date,publish_date
    try:
        df = pd.read_excel(data_files_path+'tradings.xls')
    except:
        df = pd.read_csv(data_files_path+'tradings.csv', sep=',', encoding='latin')
    df['ts date'] = publish_date
    df.columns = df.columns.str.strip().str.lower()
    df['master id'] = df['isin-code'].map(isinToMasterid)
    df = df[df['stock exchange (eng)'].isin(stock_exchange_priority)]
    df = df.dropna(subset=['master id'])
    df['custom close price col'] = np.nan
    for col in close_price_priority :
        if len(df.loc[df['custom close price col'].isna() & df[col].notna()]):
            df.loc[df['custom close price col'].isna() & df[col].notna(), 'custom close price col'] = df.loc[df['custom close price col'].isna() & df[col].notna(), col]
    preprocessedData = pd.DataFrame()
    issueData = []
    for by, groupedDf in df.groupby(by = ['master id', 'ts date']) :
        master_id = int(by[0])
        ts_date = by[1]
        for stock_exchange in stock_exchange_priority :
            close_price_col = 'custom close price col'
            stock_exchange_wise_df = groupedDf[groupedDf['stock exchange (eng)'] == stock_exchange]
            if (len(stock_exchange_wise_df) > 0) and (stock_exchange_wise_df[close_price_col].notna().sum()):
                preprocessedData = preprocessedData.append(stock_exchange_wise_df)
                break
        else:
            issueData.append([master_id, str(ts_date)])
    close_price_col = 'custom close price col'
    preprocessedData.loc[preprocessedData['open price'].isna(), 'open price'] = preprocessedData.loc[preprocessedData['open price'].isna(), close_price_col]
    preprocessedData.loc[preprocessedData['maximum price'].isna(), 'maximum price'] = preprocessedData.loc[preprocessedData['maximum price'].isna(), close_price_col]
    preprocessedData.loc[preprocessedData['minimum price'].isna(), 'minimum price'] = preprocessedData.loc[preprocessedData['minimum price'].isna(), close_price_col]
    preprocessedData.loc[preprocessedData['close price'].isna(), 'close price'] = preprocessedData.loc[preprocessedData['close price'].isna(), close_price_col]
    preprocessedData = preprocessedData.replace([np.NaN], ['NA'])
    preprocessedData = preprocessedData.replace([pd.NaT], ['NA'])
    result = []
    for i, row in preprocessedData.iterrows():
        row2 = row.to_dict()
        masterId = row2['master id']
        for k, v in row2.items():
            if v == "NA":
                continue
            if k in colToIndicator:
                try:
                    indicatorId = colToIndicator[k]
                    if type(v) == pd.Timestamp:
                        json_data = None
                        dataType = 2
                        gmt = pytz.timezone("GMT")
                        value_data = gmt.localize(v).timestamp()
                    elif type(v) == float or type(v) == int or  v.isnumeric(): 
                        dataType = 0
                        value_data = v
                        json_data = None
                    elif v.isnumeric() == False:
                        json_data = json.dumps({'TEXT':v})
                        dataType = 3
                        value_data = 0
                except Exception as e:
                    print(k)
                    print(v)
                    print(e)
                result.append([masterId,indicatorId,value_data,json_data,dataType,publish_date])
    my_log.info('resultset is created')
    return result

def db_insert(result):
    try:
        db_conn = mysql.connector.connect(host='54.237.79.6',user='rentech_user',database = 'rentech_db',password='N)baegbgqeiheqfi3e9314jnEkekjb',auth_plugin='mysql_native_password')
        cursor = db_conn.cursor()
        sql = """INSERT INTO `raw_data` (`id`, `master_id`, `indicator_id`, `value_data`, `json_data`, `data_type`, `ts_date`, `ts_hour`, `job_id`, `timestamp`) VALUES 
        (NULL, %s, %s, %s, %s, %s, %s, '0:0:0', 9, NOW()) ON DUPLICATE KEY UPDATE  
        master_id = VALUES(master_id), indicator_id = VALUES(indicator_id), value_data = VALUES(value_data), json_data = VALUES(json_data),data_type = VALUES(data_type), ts_date = VALUES(ts_date) ,ts_hour = VALUES(ts_hour),
        job_id = VALUES(job_id), batch_id = VALUES(batch_id);"""
        cursor.executemany(sql, result)
        rows = cursor.rowcount
        my_log.info(f'{rows} records inserted successfully')
        db_conn.commit()
    except Exception as e:
        my_log.setLevel(log.ERROR)
        my_log.error(f'Mysql Error:{e}',exc_info=True)
        send_email(status='Fail',text=str(e))
    finally:
        if (db_conn.is_connected()):
            cursor.close()
            db_conn.close()
            my_log.info("MySQL connection is closed")
            send_email(rows,status = 'Success')
  
if __name__ == "__main__":  
  my_log.info(f'----------------------started at:{datetime.datetime.now()}----------------------------')
  try:
    result = get_result()
    db_insert(result)
  except Exception as e:
    my_log.setLevel(log.ERROR)
    my_log.error(f'Error:{e}',exc_info=True)
    send_email(status='Fail',err_text=str(e))
  my_log.setLevel(log.INFO)
  my_log.info(f'----------------------finished at:{datetime.datetime.now()}----------------------------')

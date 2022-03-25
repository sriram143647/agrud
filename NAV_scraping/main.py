import markets_ft.markets_ft_com_scraper as market
import fundsquare.fundsquare_net_scraper as fundsquare
import fundinfo.fundinfo_private_investor_scraper as priv_fundinfo
import fundinfo.fundinfo_professional_investor_scraper as prof_fundinfo
import sg_morningstar.sg_morningstar_scraper as morningstar
import pandas as pd
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib,ssl
import multiprocessing
import time
import os
import logging as log
file_path = '/home/ubuntu/rentech/nav_scraping/'
# file_path = r'D:\\sriram\\agrud\\NAV_scraping\\server_files\\'
data_file = file_path+'MF List - Final.csv'
output_file = file_path+'scraped_data.csv'
non_scraped_isin_file = file_path+'non_scraped_data.csv'
log_file = file_path+'scraper_run_log.txt'
log.basicConfig(filename = log_file,filemode='a',level=log.INFO)
my_log = log.getLogger()

def send_email(row_count=0,status=None,err_text=None):
    sender_email = 'agrud.scrapersmail123@gmail.com'
    email_password = 'qwerty@123'
    receivers_email_list = ["prince.chaturvedi@agrud.com","sayan.sinharoy@agrud.com","soumodip.pramanik@agrud.com","vidyut.lakhotia@agrud.com"]
    subject = f"Nav Scraping data ingestion: {datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ','.join(receivers_email_list)
    msg['Subject'] = subject
    body = f"Total records inserted: {row_count}\ncronjob status: {status}\nError:{err_text}"
    msg.attach(MIMEText(body,'plain'))
    attach_file_name = non_scraped_isin_file
    with open(attach_file_name,'rb') as send_file:
        payload = MIMEBase('application', 'octate-stream')
        payload.set_payload(send_file.read())
    encoders.encode_base64(payload) 
    payload.add_header('Content-Decomposition',f'attachment; filename={attach_file_name}')
    msg.attach(payload)
    text = msg.as_string()
    context = ssl.create_default_context()
    server = smtplib.SMTP('smtp.gmail.com',587)
    server.starttls(context=context)
    server.login(sender_email,email_password)
    server.sendmail(sender_email,receivers_email_list,text)
    server.quit()
    my_log.info(f'email sent')

def db_insert(df):
    import mysql.connector
    result = df[['master id','price','date']].values.tolist()
    try:
        db_conn = mysql.connector.connect(host='54.237.79.6',user='rentech_user',database = 'rentech_db',password='N)baegbgqeiheqfi3e9314jnEkekjb',auth_plugin='mysql_native_password')
        cursor = db_conn.cursor()
        sql = """INSERT INTO `raw_data` 
        (`id`, `master_id`, `indicator_id`, `value_data`, `json_data`, `data_type`, `ts_date`, `ts_hour`, `job_id`, `batch_id`, `timestamp`) 
        VALUES (NULL, %s, 371, %s, NULL, 0, %s, '0:0:0', 12, NULL, NOW()) ON DUPLICATE KEY UPDATE 
        master_id = VALUES(master_id), indicator_id = VALUES(indicator_id), value_data = VALUES(value_data), json_data = VALUES(json_data),
        data_type = VALUES(data_type), ts_date = VALUES(ts_date) ,ts_hour = VALUES(ts_hour), job_id = VALUES(job_id), batch_id = VALUES(batch_id);"""
        cursor.executemany(sql, result)
        rows = cursor.rowcount
        my_log.info(f'{rows} rows inserted')
        db_conn.commit()
        send_email(row_count=rows,status='success')
    except Exception as e:
        my_log.info(f'Exception: {e}')
    finally:
        if (db_conn.is_connected()):
            cursor.close()
            db_conn.close()
            my_log.info('Connection closed')

def task1():
    # market_ft scraper
    my_log.info(f'{datetime.now()} Task1 market_ft scraping')
    market.output_file = output_file
    market.data_file = data_file
    market.non_scraped_isin_file = non_scraped_isin_file
    market.start_markets_ft_scraper()
    my_log.info(f'{datetime.now()} Task1 market_ft scraping ended')

    # fundsquare scraper
    my_log.info(f'{datetime.now()} Task1 fundsquare scraping')
    fundsquare.output_file = output_file
    fundsquare.data_file = data_file
    fundsquare.non_scraped_isin_file = non_scraped_isin_file
    fundsquare.start_fundsquare_scraper()
    my_log.info(f'{datetime.now()} Task1 fundsquare scraping ended')

    # priv_fundinfo scraper
    my_log.info(f'{datetime.now()} Task1 fundinfo private scraping')
    priv_fundinfo.output_file = output_file
    priv_fundinfo.data_file = data_file
    priv_fundinfo.non_scraped_isin_file = non_scraped_isin_file
    priv_fundinfo.start_fundinfo_priv_scraper(case=1)
    my_log.info(f'{datetime.now()} Task1 fundinfo private scraping ended')

    # prof_fundinfo scraper
    my_log.info(f'{datetime.now()} Task1 fundinfo professional scraping')
    prof_fundinfo.output_file = output_file
    prof_fundinfo.data_file = data_file
    prof_fundinfo.non_scraped_isin_file = non_scraped_isin_file
    prof_fundinfo.start_fundinfo_prof_scraper(case=1)
    my_log.info(f'{datetime.now()} Task1 fundinfo professional scraping ended')

def task2():
    # morningstar scraper
    my_log.info(f'{datetime.now()} Task2 morningstar scraping')
    morningstar.output_file = output_file
    morningstar.data_file = data_file
    morningstar.non_scraped_isin_file = non_scraped_isin_file
    morningstar.start_sg_morningstar_scraper()
    my_log.info(f'{datetime.now()} Task2 morningstar scraping ended')

    # priv_fundinfo scraper
    my_log.info(f'{datetime.now()} Task2 fundinfo private scraping')
    priv_fundinfo.output_file = output_file
    priv_fundinfo.data_file = data_file
    priv_fundinfo.non_scraped_isin_file = non_scraped_isin_file
    priv_fundinfo.start_fundinfo_priv_scraper(case=2)
    my_log.info(f'{datetime.now()} Task2 fundinfo private scraping ended')

    # prof_fundinfo scraper
    my_log.info(f'{datetime.now()} Task2 fundinfo professional scraping')
    prof_fundinfo.output_file = output_file
    prof_fundinfo.data_file = data_file
    prof_fundinfo.non_scraped_isin_file = non_scraped_isin_file
    prof_fundinfo.start_fundinfo_prof_scraper(case=2)
    my_log.info(f'{datetime.now()} Task2 fundinfo professional scraping ended')

def start():
    my_log.info(f'-------------------start time: {datetime.now()}-------------------')
    # # files deletion
    try:
        os.remove(output_file)
    except:
        pass
    
    try:
        os.remove(non_scraped_isin_file)
    except:
        pass
    
    try:
        os.remove(log_file)
    except:
        pass
    
    # process 1
    p1 = multiprocessing.Process(target=task1)
    p1.start()

    # process 2
    p2 = multiprocessing.Process(target=task2)
    p2.start()

    # process join
    p1.join()
    p2.join()
    
    # drop duplicate isin
    time.sleep(10)
    df = pd.read_csv(non_scraped_isin_file,encoding='utf-8',header=None)
    df = df.drop_duplicates()
    df.to_csv(non_scraped_isin_file,encoding='utf-8',index=False,header=None)
    time.sleep(5)

    # db insertion
    df = pd.read_csv(output_file,encoding='utf-8')
    db_insert(df)
    my_log.info(f'-------------------end time: {datetime.now()}-------------------')
    
if __name__ == '__main__':
    try:
        start()
    except:
        start()
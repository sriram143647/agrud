import requests
import zipfile
import io
import shutil
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib,ssl
import datetime
import os
import logging as log
# server paths
# data_folder_path = '/home/ubuntu/rentech/cbonds_scrapers/zipfiles/datafiles/'
# log_file_path = '/home/ubuntu/rentech/cbonds_scrapers/zipfiles/scraper_run_log.txt'

#local paths
data_folder_path = 'D:\\sriram\\agrud\\cbonds_data_entry\\server_files\\zipfiles\\data_files\\'
log_file_path = 'D:\\sriram\\agrud\\cbonds_data_entry\\server_files\\zipfiles\\scraper_run_log.txt'
log.basicConfig(filename=log_file_path,filemode='a',level=log.INFO)
my_log = log.getLogger()

def send_email(row_count=0,status=None,err_text=None):
    sender_email = 'agrud.scrapersmail123@gmail.com'
    email_password = 'qwerty@123'
    receivers_email_list = ["prince.chaturvedi@agrud.com","sayan.sinharoy@agrud.com","soumodip.pramanik@agrud.com","vidyut.lakhotia@agrud.com","bhavesh.bansal@agrud.com"]
    subject = f"Nav Scraping data ingestion: {datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
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

def get_data(today_date):
    zip_file_url = f'https://database.cbonds.info/unloads/agrud_technologies/archive/{today_date}.zip'
    path = data_folder_path+today_date
    isdir = os.path.isdir(path)
    if isdir:
      shutil.rmtree(path)
      my_log.info(f'{today_date} morning run files deleted successfully')
    r = requests.get(zip_file_url, stream=True,auth=('admin@agrud.com', '123456'))
    try:
        z = zipfile.ZipFile(io.BytesIO(r.content))
    except zipfile.BadZipFile:
        my_log.info('zip file not found. please try again')
        return 0
    z.extractall(path)
    my_log.info(f'{today_date} file downloaded successfully')

if __name__ == "__main__":
  my_log.info(f'----------------------started at:{datetime.datetime.now()}----------------------------')
  try:
    # today_date = datetime.datetime.today().strftime('%Y-%m-%d')
    today_date = '2022-04-19'
    get_data(today_date)
    # send_email(status='Success')
  except Exception as e:
    my_log.setLevel(log.ERROR)
    my_log.error(f'Error:{e}',exc_info=True)
    send_email(status='Fail',err_text=str(e))
  my_log.setLevel(log.INFO)
  my_log.info(f'----------------------finished at:{datetime.datetime.now()}----------------------------')

import requests
import zipfile
import io
import shutil
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
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

def send_email(status=None,text=None):
    email_user = 'agrud.scrapersmail123@gmail.com'
    email_password = 'qwerty@123'
    email_send = 'agrud.2021@gmail.com'
    subject = f"cbonds zipfile download result: {datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
    msg = MIMEMultipart()
    msg['From'] = email_user
    msg['To'] = email_send
    msg['Subject'] = subject
    body = f"File download {status}\ncronjob status: {status}\nError: {text}"
    msg.attach(MIMEText(body,'plain'))
    text = msg.as_string()
    server = smtplib.SMTP('smtp.gmail.com',587)
    server.starttls()
    server.login(email_user,email_password)
    server.sendmail(email_user,email_send,text)
    server.quit()
    my_log.info('email sent')

def get_data(today_date):
    zip_file_url = f'https://database.cbonds.info/unloads/agrud_technologies/archive/{today_date}.zip'
    path = data_folder_path+today_date
    isdir = os.path.isdir(path) 
    if isdir:
      shutil.rmtree(path)
      my_log.info(f'{today_date} file deleted successfully')
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
    today_date = datetime.datetime.today().strftime('%Y-%m-%d')
    # today_date = '2022-01-08'
    get_data(today_date)
    send_email(status='Success')
  except Exception as e:
    my_log.setLevel(log.ERROR)
    my_log.error(f'Error:{e}',exc_info=True)
    send_email(status='Fail',text=str(e))
  my_log.setLevel(log.INFO)
  my_log.info(f'----------------------finished at:{datetime.datetime.now()}----------------------------')

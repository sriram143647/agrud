from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import datetime
import logging as log
log_file = r'D:\\sriram\\agrud\\news\\mtwire\\scraper_run_log.txt'
# log_file = '/home/agruduser/news/benzinga/scraper_run_log.txt'
log.basicConfig(filename=log_file,filemode='a',level=log.INFO)
my_log = log.getLogger()
time_format = '%Y-%m-%d %H:%M:%S.%f'
time_format2 = '%Y-%m-%d %H:%M'
# now_time = datetime.datetime.now().strftime(time_format)
now_time = '2021-12-27 03:30:00.645739'
now_time2 = datetime.datetime.strptime(now_time, time_format)
read_time = (datetime.datetime.strptime(now_time, time_format) - datetime.timedelta(hours=1))

def send_email(files,row_count,status):
    body = ''
    email_user = 'agrud.scrapersmail123@gmail.com'
    email_password = 'qwerty@123'
    email_send = 'agrud.2021@gmail.com'
    subject = f"mtnewswire news data ingestion results: {datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
    msg = MIMEMultipart()
    msg['From'] = email_user
    msg['To'] = email_send
    msg['Subject'] = subject
    for file in files:  
        body = body + f'Processed file: {file},\n'
    body = body + f'News inserted: {row_count}\ncronjob status: {status}'
    msg.attach(MIMEText(body,'plain'))
    text = msg.as_string()
    server = smtplib.SMTP('smtp.gmail.com',587)
    server.starttls()
    server.login(email_user,email_password)
    server.sendmail(email_user,email_send,text)
    server.quit()
    my_log.info('email sent')

def file_read():
    data = []
    work = 0
    with open(log_file, mode='r') as file:
        lines = file.read()
        for line in lines.split('\n'):
            if 'started at' in line and work == 0:
                start_time = line.split('started at:')[1].split('---')[0]
                start_time = datetime.datetime.strptime(start_time,time_format)
                if start_time.date() == read_time.date() and start_time.hour == read_time.hour and start_time.minute == read_time.minute:
                    work = 1
            if 'ended at'  in line and work == 1:
                end_time = line.split('ended at:')[1].split('---')[0]
                end_time = datetime.datetime.strptime(end_time,time_format)
                if end_time.date() == now_time2.date() and end_time.hour == now_time2.hour and end_time.minute == now_time2.minute:
                    work = 0
                    break
            if work == 1:
                data.append(line)
        row_count = 0
        files = []
        for d in data:
            if 'processing file' in d:
                file = d.split('processing file:')[1].strip()
                files.append(file)
            if 'records' in d:
                row_count = row_count + int(d.split('INFO:root:')[1].split(' ')[0])
        send_email(files,row_count,'Success')

file_read()
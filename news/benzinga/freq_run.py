import requests
import json
import pandas as pd
import datetime
import re
import hashlib
import mysql.connector
from bs4 import BeautifulSoup
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import datetime
import traceback
import logging as log
tm_format = '%Y-%m-%d %H:%M:%S'
now = datetime.datetime.strftime(datetime.datetime.now(),tm_format)
#local paths
log_file = r'D:\\sriram\\agrud\\news\\benzinga\\scraper_run_log.txt'
csv_file = r'D:\\sriram\\agrud\\news\\benzinga\\benzinga_news_'+now+'.csv'

# server paths
# log_file = '/home/agruduser/news/benzinga/scraper_run_log.txt'
# csv_file = '/home/agruduser/news/benzinga/benzinga_news_'+now+'.csv'
log.basicConfig(filename=log_file,filemode='a',level=log.INFO)
my_log = log.getLogger()
useCols = ["url", "url_hash", "title", "body", "publish_date","date_string", "images", "news_type", "master_id", "job_id"]

def get_map():
  try:
    db_conn = mysql.connector.connect(host = "34.69.145.125", user = "rentechuser", password = "Agdj8ekee9u04IIid", database = "rentech_db")
    cursor = db_conn.cursor()
    sql = "SELECT `provider_symbol`,`master_id` FROM `provider_symbol_mapping_master` WHERE `provider_id` = '13';"
    cursor.execute(sql)
    resultset = cursor.fetchall()
    rows = cursor.rowcount
    my_log.info(f'data map fetched {rows} rows successfully')
    df = pd.DataFrame(resultset)
    map = dict(df.values)
    return map
  except Exception as e:
    my_log.info('Mysql Error: ',e)
  finally:
    cursor.close()
    db_conn.close()

def send_email(row_count=0,status=None,text=None):
    email_user = 'agrud.scrapersmail123@gmail.com'
    email_password = 'qwerty@123'
    email_send = 'agrud.2021@gmail.com'
    subject = f"benzinga news data ingestion results: {datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
    msg = MIMEMultipart()
    msg['From'] = email_user
    msg['To'] = email_send
    msg['Subject'] = subject
    body = f"Total news inserted: {row_count}\ncronjob status: {status}\nError: {text}"
    msg.attach(MIMEText(body,'plain'))
    text = msg.as_string()
    server = smtplib.SMTP('smtp.gmail.com',587)
    server.starttls()
    server.login(email_user,email_password)
    server.sendmail(email_user,email_send,text)
    server.quit()
    my_log.info('email sent')

def findBodyText(bodySoup) :
    if not (bodySoup and str(bodySoup).strip()) or bodySoup.text.lower().strip().startswith("https://"):
        return ""

    if re.search("^[-]{1}\w+$", bodySoup.text) :
        return ""

    bodyLines = []
    for child in bodySoup.children :
        try :
            childText = child.text.replace('\xa0',' ').strip()
        except:
            childText = child

        if not (childText or childText.strip()):
            continue

        if re.search("^(photo:)|(please add event info in article)|(photo)|(photo by)|(image)|(courtesy photo)|(related link)|(related content)|(read next)|(also read)|(see also).*:", childText.lower()) :
            continue

        if re.search("^(check out)|(click here)| (to check out)", childText.lower()):
            continue

        if re.search("^(changes here)", childText.lower()):
            continue

        if re.search("(found here:)$", childText.lower()) or 'found here' in childText.lower():
          continue

        if "here</a>" in str(child) :
            origText = childText
            bodyLine = origText
            linkText = child.find('a').text
            link = child.find('a').get('href', '')
            if link :
                bodyLine = origText.replace(linkText, f"{linkText} {link}")
        else :
            bodyLine = childText.strip()

        bodyLines.append(bodyLine.strip())

    custom_body = "\n".join(bodyLines).strip()
    return custom_body

def data_to_df(result):
  df = pd.DataFrame(result)
  df['master_id'] = df['ticker'].map(get_map())
  df = df[df['master_id'].notna()]
  df = df.drop_duplicates(subset=['url_hash', 'master_id'])
  df2 = df[useCols]
  df2.to_csv(csv_file, index = False)
  return df2

def db_insert(df):
  val = df[useCols].values.tolist()
  try:
      db_conn = mysql.connector.connect(host = "34.69.145.125", user = "rentechuser", password = "Agdj8ekee9u04IIid", database = "rentech_db")
      cursor = db_conn.cursor()
      sql = "INSERT IGNORE INTO `news_raw_data` VALUES(NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP());"
      cursor.executemany(sql, val)
      rows = cursor.rowcount
      my_log.info(f'{rows} news are inserted successfully')
      db_conn.commit()
  except Exception as e:
      my_log.info(f'error',e)
  finally:
      if(db_conn.is_connected()):
          status = 'Success'
          cursor.close()
          db_conn.close()
          my_log.info('connection is closed')
          send_email(rows,status)

def get_data():
  result = []
  news_type = 19
  job_id = 1
  page = 0
  date = datetime.datetime.today()
  while True:
    url = f"https://api.benzinga.com/api/v2/news?token=79f0b5b80a244c0f96a9c6fee4e805dd&displayOutput=full&pageSize=100&page={page}"
    params = {
      "token" : "79f0b5b80a244c0f96a9c6fee4e805dd",
      "displayOutput" : "full",
      "pageSize" : 100,
      "page" : {page},
      "sort" : "updated:desc",
      "dateFrom" : datetime.datetime.strftime(date,'%Y-%m-%d'),
      "dateTo" : datetime.datetime.strftime(date,'%Y-%m-%d'),
    }


    headers = {
      'Accept': 'application/json'
    }
    response = requests.request("GET", url, headers=headers, params=params)
    jData = response.json()
    for data in jData:
      if data['author'] != "Benzinga Insights":
        updated_dt = data['updated']

        title = data['title'].strip()

        teaser = data['teaser'].strip()
        body = data['body'].strip()

        url = data['url'].strip()
        if url == '':
          continue
        url_hash = hashlib.md5(url.encode()).hexdigest()

        if "https://www.benzinga.com/analyst-ratings" not in url :
          continue

        stocks = data['stocks']
        image_json = json.dumps(data['image']).strip()
        if image_json == '[]':
          image_json = None

        for stock in stocks :
          row = {
            "url" : url,
            "url_hash" : url_hash,
            "title" : title,
            "body" : teaser,
            "body" : BeautifulSoup(body,'lxml').text,
            "html_body" : data['body'],
            "body" : findBodyText(BeautifulSoup(body,'lxml').find('body')),
            "publish_date" : None,
            "date_string":updated_dt,
            "images" : image_json,
            "news_type" : news_type,
            "job_id" : job_id,
            "ticker" : stock['name'].strip()
          }
          if row["body"] :
            result.append(row)
    if not jData:
      break
    page += 1
  if len(result) == 0:
    my_log.info('No new news is available')
    return 0
  else:
    my_log.info('news result set is created')
    return result

if __name__ == "__main__":
  my_log.info(f'----------------------started at:{datetime.datetime.now()}----------------------------')
  try:
    result = get_data()
    if result != 0:
      df =  data_to_df(result)
      db_insert(df)
  except Exception as e:
    my_log.setLevel(log.ERROR)
    my_log.error(f'Error:{e}',exc_info=True)
    send_email(status='Fail',text=str(e))
  my_log.setLevel(log.INFO)
  my_log.info(f'----------------------finished at:{datetime.datetime.now()}----------------------------')

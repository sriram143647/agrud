import requests
import json
import pandas as pd
import datetime
import re
import hashlib  
import mysql.connector
from mysql.connector.errors import DatabaseError as db_err
from bs4 import BeautifulSoup
import logging as log
tm_format = '%Y-%m-%d %H:%M:%S'
#local paths
log_file = r'D:\\sriram\\agrud\\news\\benzinga\\scraper_run_log.txt'
# serve paths
# log_file = '/home/agruduser/news/benzinga/scraper_run_log.txt'
log.basicConfig(filename=log_file,filemode='a',level=log.INFO)
my_log = log.getLogger()

def get_map():
    db_conn = mysql.connector.connect(host = "34.69.145.125", user = "rentechuser", password = "Agdj8ekee9u04IIid", database = "rentech_db")
    cursor = db_conn.cursor()
    sql = "SELECT `provider_symbol`,`master_id` FROM `provider_symbol_mapping_master` WHERE `provider_id` = '13';"
    cursor.execute(sql)
    resultset = cursor.fetchall()
    cursor.close()
    db_conn.close()
    my_log.info('data map fetched successfully')
    df = pd.DataFrame(resultset)
    map = dict(df.values)
    return map

def findBodyText(bodySoup) :
    if not (bodySoup and str(bodySoup).strip()) or bodySoup.text.lower().strip().startswith("https://"):
        return ""

    if re.search("^[-]{1}\w+$", bodySoup.text) :
        return ""

    bodyLines = []
    for child in bodySoup.children :
        try :
            childText = child.text.replace('\xa0',' ').strip()
        except :
            # NavString
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
  df = df.drop(columns=['ticker','html_body'])
  
  return df

def db_insert(df):
  useCols = ["url", "url_hash", "title", "body", "publish_date","date_string", "images", "news_type", "master_id", "job_id"]
  val = df[useCols].values.tolist()
  try:
    db_conn = mysql.connector.connect(host = "34.69.145.125", user = "rentechuser", password = "Agdj8ekee9u04IIid", database = "rentech_db")
    cursor = db_conn.cursor()
    sql = "INSERT INTO `news_raw_data_test` VALUES(NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP());"
    cursor.executemany(sql, val)
    db_conn.commit()
    my_log.info(f'{cursor.rowcount} records are inserted successfully')
  except db_err as e:
    my_log.info('Database error: {e}')
  except Exception as e:
    my_log.info(f'Mysql Error {e}')
  finally:
    if(db_conn.is_connected()):
        cursor.close()
        db_conn.close()
        my_log.info('connection is closed')

def get_data():
  result = []
  start_date = datetime.datetime.strptime('2021-12-06','%Y-%m-%d')
  while True:
    if start_date < datetime.datetime.today():
      my_log.info(start_date)
      news_type = 19
      job_id = 1
      page = 0
      while True:
        api_url = f"https://api.benzinga.com/api/v2/news?token=79f0b5b80a244c0f96a9c6fee4e805dd&displayOutput=full&pageSize=100&page={page}"
        my_log.info(f'api page url: {api_url}')
        params = {
          "token" : "79f0b5b80a244c0f96a9c6fee4e805dd",
          "displayOutput" : "full",
          "pageSize" : 100,
          "page" : {page},
          "sort" : "updated:desc",
          "dateFrom" : datetime.datetime.strftime(start_date,'%Y-%m-%d'),
          "dateTo" : datetime.datetime.strftime(start_date+datetime.timedelta(days=5),'%Y-%m-%d'),
        }


        headers = {
          'Accept': 'application/json'
        }
        response = requests.request("GET", api_url, headers=headers, params=params)
        jData = response.json()
        for data in jData :
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
        if not jData :
          break
        page += 1
      start_date = start_date+datetime.timedelta(days=5)
    else:
      break
  return result

def start():
  result = get_data()
  df = data_to_df(result)
  db_insert(df)

start()
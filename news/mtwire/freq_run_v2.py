import subprocess
import pandas as pd
import datetime
import schedule
import time
import mysql.connector
from bs4 import BeautifulSoup as bs
import logging as log
maindf = pd.DataFrame()
#server location
log_file = '/home/agruduser/news/mtnewswire/scraper_run_log.txt'
csv_file = '/home/agruduser/news/mtnewswire/news_data.csv'
log.basicConfig(filename = log_file,filemode='a',level=log.INFO)
my_log = log.getLogger()

def get_map():
    db_conn = mysql.connector.connect(host = "34.69.145.125", user = "rentechuser", password = "Agdj8ekee9u04IIid", database = "rentech_db")
    cursor = db_conn.cursor()
    sql = "SELECT `provider_symbol`,`master_id` FROM `provider_symbol_mapping_master` WHERE `provider_id` = '14';"
    cursor.execute(sql)
    resultset = cursor.fetchall()
    cursor.close()
    db_conn.close()
    my_log.info('data map fetched successfully')
    df = pd.DataFrame(resultset)
    map = dict(df.values)
    return map

def db_insert(df):
  val = df.values.tolist()
  try:
      db_conn = mysql.connector.connect(host = "34.69.145.125", user = "rentechuser", password = "Agdj8ekee9u04IIid", database = "rentech_db")
      cursor = db_conn.cursor()
      sql = "INSERT IGNORE INTO `mtnewswire_raw_data_test` VALUES(NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP());"
      cursor.executemany(sql, val)
      db_conn.commit()
      my_log.info(f'{cursor.rowcount} records are inserted successfully')
  except Exception as e:
      my_log.info(f'Mysql Error',e)
  finally:
      if(db_conn.is_connected()):
          cursor.close()
          db_conn.close()
          my_log.info('connection is closed')

def process_file(file):
    global maindf
    newstype = 20
    job_id = 2
    tags = set()
    with open(file,encoding='utf-8') as f:
        xml = f.read()
        soup = bs(xml, 'lxml')

        tags.update(set([a.name for a in soup.find_all()]))
        documentTag = soup.find('document')
        release_dt_time = documentTag.get("releasetime")
        release_dt_time = datetime.datetime.strptime(release_dt_time, "%Y/%m/%d %H:%M:%S")
        release_dt_time_str = datetime.datetime.strftime(release_dt_time,"%Y-%m-%d %H:%M:%S")

        rev_id = documentTag.get("revisionid")
        title = soup.find("headline").text.strip()
        if title.startswith("--"):
            title = title.replace("--", " ", 1).strip()

        images = None

        tickers = soup.find("tickers").text.replace('^','').strip().split()
        body = "\n".join([_.text for _ in soup.findAll("p")]).strip()
        try:
            body = ''.join(body).split('--')[1].strip()
        except:
            body = ''.join(body).strip()

        cpyright = soup.find('copyright').text
        body = body+' '+cpyright
        if body == '':
            return 0

        file_id = file.split('/')[-1].replace('.xml','')
        my_log.info(f'processing file: {file_id}')

        for ticker in tickers :
            row = {
                "file":file_id,
                "revision_no":rev_id,
                "title" : title,
                "body" : body,
                "publish_date" : None,
                "date_string": release_dt_time_str,
                "images":images,
                "ticker" : ticker,
                "news_type" : newstype,
                "job_id" : job_id,
            }
        maindf = maindf.append(row, ignore_index=True)

def get_data():
    my_log.info(f'----------------started at:{datetime.datetime.now()}--------------------')
    global maindf
    files = subprocess.check_output('find ~/news_data -mmin -5',shell=True)
    files = files.decode('utf-8')
    file_list = files.split('\n')
    for file in file_list[1:-1]:
        process_file(file)
    maindf.insert(7,'masterid',maindf['ticker'].map(get_map()))
    maindf = maindf[maindf['masterid'].notna()]
    use_cols = ['file','revision_no','title','body','publish_date','date_string','images','news_type','masterid','job_id']
    maindf = maindf[use_cols]
    # maindf.to_csv(csv_file, index = False, mode = 'a')
    db_insert(maindf)
    my_log.info(f'----------------ended at:{datetime.datetime.now()}--------------------')

def start():
    schedule.every(5).minutes.do(get_data)
    while True:
        time.sleep(10)
        schedule.run_pending() 

if __name__ == "__main__":
    start()
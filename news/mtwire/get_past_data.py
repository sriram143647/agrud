import os
import pandas as pd
import datetime
import time
import mysql.connector
from bs4 import BeautifulSoup as bs
import logging as log
import multiprocessing as mp
maindf = pd.DataFrame()
comp_flag = 0
# server locations
# log_file = '/home/agruduser/news/mtnewswire/scraper_run_log.txt'
# csv_file = '/home/agruduser/news/mtnewswire/news_data.csv'

#local locations
log_file = r'D:\\sriram\\agrud\\news\\mtwire\\scraper_run_log.txt'
csv_file = r'D:\\sriram\\agrud\\news\\mtwire\\news_data.csv'

log.basicConfig(filename = log_file,filemode='a',level=log.INFO)
my_log = log.getLogger()


def get_map():
    try:
        db_conn = mysql.connector.connect(host = "34.69.145.125", user = "rentechuser", password = "Agdj8ekee9u04IIid", database = "rentech_db")
        cursor = db_conn.cursor()
        sql = "SELECT `provider_symbol`,`master_id` FROM `provider_symbol_mapping_master` WHERE `provider_id` = '14';"
        cursor.execute(sql)
        resultset = cursor.fetchall()
    except Exception as e:
        my_log.info(f'Mysql Error',e)
    finally:
        if(db_conn.is_connected()):
            cursor.close()
            db_conn.close()
            my_log.info('connection is closed')
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
    with open(r'D:\\sriram\\agrud\\news\\mtwire\\xmls\\'+file,encoding='utf-8') as f:
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

        tickers = soup.find("tickers").text.strip().split()
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

def get_data(start_dt,end_dt):
    print(f'----------------started at:{datetime.datetime.now()}--------------------')
    global maindf,comp_flag
    start_dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    end_dt = (datetime.datetime.now()-datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S") 
    linux_cmd = f'find ~/news_data -newerct "{end_dt}" -not -newerct "{start_dt}"'    
    print(f'linxu command:{linux_cmd}')
    files = os.listdir(r'D:\\sriram\\agrud\\news\\mtwire\\xmls')
    for file in files[:25000]:
        process_file(file)
    maindf.insert(7,'masterid',maindf['ticker'].map(get_map()))
    maindf = maindf[maindf['masterid'].notna()]
    use_cols = ['file','revision_no','title','body','publish_date','date_string','images','news_type','masterid','job_id']
    maindf = maindf[use_cols]
    # maindf.to_csv(csv_file, index = False, mode = 'a')
    # db_insert(maindf)
    print(f'----------------ended at:{datetime.datetime.now()}--------------------')
    comp_flag = 1 
    return comp_flag

def run():
    start_dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    end_dt = (datetime.datetime.now()-datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S") 
    p1 = mp.Process(target=get_data,args=(start_dt,end_dt))
    p1.start()
    time.sleep(300)
    if p1.is_alive():
        print("process is not finished!")

if __name__ == '__main__':
    get_data(None,None)
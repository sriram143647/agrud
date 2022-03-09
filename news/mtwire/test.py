from multiprocessing.context import Process
import os
import pandas as pd
import datetime
import time
import os
import schedule
import mysql.connector
from mysql.connector.errors import IntegrityError as int_err
from mysql.connector.errors import DatabaseError as db_err
from bs4 import BeautifulSoup as bs
import logging as log
import multiprocessing as mp
maindf = pd.DataFrame()
# server locations
# log_file = '/home/agruduser/news/mtnewswire/test_run_log.txt'
# csv_file = '/home/agruduser/news/mtnewswire/news_data.csv'

#local locations
log_file = r'D:\\sriram\\agrud\\news\\mtwire\\scraper_run_log.txt'
csv_file = r'D:\\sriram\\agrud\\news\\mtwire\\news_data.csv'

log.basicConfig(filename = log_file,filemode='a',level=log.INFO)
my_log = log.getLogger()
time_delay = 0


def get_map():
    db_conn = mysql.connector.connect(host = "34.69.145.125", user = "rentechuser", password = "Agdj8ekee9u04IIid", database = "rentech_db")
    cursor = db_conn.cursor()
    sql = "SELECT `provider_symbol`,`master_id` FROM `provider_symbol_mapping_master` WHERE `provider_id` = '15';"
    cursor.execute(sql)
    resultset = cursor.fetchall()
    cursor.close()
    db_conn.close()
    my_log.info('data map fetched successfully')
    df = pd.DataFrame(resultset)
    map = dict(df.values)
    return map

def db_insert():
    global maindf
    val = maindf.values.tolist()
    try:
        db_conn = mysql.connector.connect(host = "34.69.145.125", user = "rentechuser", password = "Agdj8ekee9u04IIid", database = "rentech_db")
        cursor = db_conn.cursor()
        sql = "INSERT IGNORE INTO `mtnewswire_raw_data_test` VALUES(NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP());"
        cursor.executemany(sql, val)
        db_conn.commit()
        my_log.info(f'{cursor.rowcount} records are inserted successfully')

    except int_err:
        my_log.info('Duplicate entries please check.')
    except db_err:
        db_insert()
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
        # my_log.info(f'processing file: {file_id}')

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

def get_data(linux_cmd):
    # print(f'----------------started at:{datetime.datetime.now()}--------------------')
    global maindf
    files = os.listdir(r'D:\\sriram\\agrud\\news\\mtwire\\xmls')
    for file in files[:10000]:
        process_file(file)
    try:
        maindf.insert(7,'masterid',maindf['ticker'].map(get_map()))
    except KeyError:
        print('no file are available')
        return 0
    maindf = maindf[maindf['masterid'].notna()]
    use_cols = ['file','revision_no','title','body','publish_date','date_string','images','news_type','masterid','job_id']
    maindf = maindf[use_cols]
    if maindf.empty:
        my_log.info('data frame is empty please check')
    else:
        # maindf.to_csv(csv_file, index = False, mode = 'a')
        # db_insert()
        pass
    # print(f'----------------ended at:{datetime.datetime.now()}--------------------')

def run():
    global time_delay
    if time_delay == 0:
        start_tm = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        end_tm = (datetime.datetime.now()-datetime.timedelta(minutes=6)).strftime("%Y-%m-%d %H:%M:%S") 
    else:
        time_delay = time_delay + 1
        start_tm = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        end_tm = (datetime.datetime.now()-datetime.timedelta(minutes=time_delay)).strftime("%Y-%m-%d %H:%M:%S") 
    linux_cmd = f'find ~/news_data -newerct "{end_tm}" -not -newerct "{start_tm}"'
    print(f'linux command:{linux_cmd}')
    # get_data(linux_cmd)
    p = mp.Process(target=get_data,args=(linux_cmd,))
    p.start()
    # print('new process started')
    # print("process is waiting for 5 minutes to be finished!")
    # sleep for 5 minutes
    time.sleep(60)
    if p.is_alive():
        while True:
            # sleep for next 5 minutes
            time_delay = time_delay + 1
            # print(f'next process is delayed by {time_delay} minutes!')
            time.sleep(60)
            if p.is_alive():
                continue
            else:
                break
        run()
    else:
        time_delay = 0
        run()
    
def start():
    while True:
        run()
        time.sleep(10)

if __name__ == "__main__":
    start()


# notification mail
# agrud.2021@gmail.com
import os 
import json
import datetime
import mysql.connector
from mysql.connector.errors import DatabaseError
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
news_data_path = r'D:\\sriram\\agrud\\news\\mtwire\\xmls'
with open('tickerToMasteridMap.json') as f:
    tickerToMasteridMap = json.load(f)

def get_data():
    count=1
    newstype = 20
    job_id = 2
    df = pd.DataFrame()
    tags = set()
    for file in os.listdir(news_data_path)[0:1000]:
        with open(f"{news_data_path}\\{file}",encoding='utf-8') as f:
            print(f'processing file {count}: {file}')
            count += 1
            xml = f.read()
            soup = BeautifulSoup(xml, 'lxml')
            file_id = file.replace('.xml','')
            
            tags.update(set([a.name for a in soup.find_all()]))
            documentTag = soup.find('document')
            release_dt_time = documentTag.get("releasetime")
            release_dt_time = datetime.datetime.strptime(release_dt_time, "%Y/%m/%d %H:%M:%S")
            release_dt_time_str = datetime.datetime.strftime(release_dt_time,"%Y-%m-%d %H:%M:%S")

            rev_id = documentTag.get("revisionid")
            # gmt time converter
            # gmt_struct_dt_tm = time.gmtime(time.mktime(release_dt_time.timetuple()))
            # gmt_dt_tm = datetime.datetime.fromtimestamp(time.mktime(gmt_struct_dt_tm))
            # gmt_releasetime = datetime.datetime.strftime(gmt_dt_tm,"%Y-%m-%d %H:%M:%S")

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
                continue

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
                df = df.append(row, ignore_index=True)
    return df

def df_to_file(df):
    df.insert(7,'masterid',df['ticker'].map(tickerToMasteridMap))
    df = df[df['masterid'].notna()]
    # df.to_csv("MTWIRE.csv", index = False, mode = 'a')
    # print('MTWIRE.csv file created')
    return df

def db_insert(df):
    # df = pd.read_csv('MTWIRE.csv')
    # df = df.replace([np.nan],[None])
    useCols = ["file", "revision_no", "title", "body", "publish_date", "date_string", "images", "news_type", "masterid", "job_id"]
    vals = df[useCols].values.tolist()
    try:
        db_conn = mysql.connector.connect(host="34.69.145.125",user="rentechuser",password="Agdj8ekee9u04IIid",database="rentech_db")
        cursor = db_conn.cursor()
        sql = "INSERT INTO `mtnewswire_raw_data_test` VALUES(NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,  CURRENT_TIMESTAMP());"
        cursor.executemany(sql, vals)
        print(f'{cursor.rowcount} rows are inserted')
        db_conn.commit()
    except DatabaseError:
        print(f'Mysql Timeout Error')
    finally:
        if(db_conn.is_connected()):
            cursor.close()
            db_conn.close()
            print('connection is closed')

def start():
    df = get_data()
    df = df_to_file(df)
    db_insert(df)

start()
import datetime
import pandas as pd
import mysql.connector.pooling
import mysql.connector
from mysql.connector import Error
from mysql.connector.connection import MySQLConnection
from mysql.connector import pooling
from bs4 import BeautifulSoup
# import requests
from time import sleep
import requests
import funcy
import json
import re


headers1 = {
  'apikey': 'lstzFDEOhfFNMLikKa0am9mgEKLBl49T',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
  'x-api-realtime-e': 'eyJlbmMiOiJBMTI4R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.XmuAS3x5r-0MJuwLDdD4jNC6zjsY7HAFNo2VdvGg6jGcj4hZ4NaJgH20ez313H8An9UJrsUj8ERH0R8UyjQu2UGMUnJ5B1ooXFPla0LQEbN_Em3-IG84YPFcWVmEgcs1Fl2jjlKHVqZp04D21UvtgQ4xyPwQ-QDdTxHqyvSCpcE.ACRnQsNuTh1K_C9R.xpLNZ8Cc9faKoOYhss1CD0A4hG4m0M7-LZQ0fISw7NUHwzQs2AEo9ZXfwOvAj1fCbcE96mbKQo8gr7Oq1a2-piYXM1X5yNMcCxEaYyGinpnf6PGqbdr6zbYZdqyJk0KrxWVhKSQchLJaLGJOts4GlpqujSqJObJQcWWbkJQYKG9K7oKsdtMAKsHIVo5-0BCUbjKVnHJNsYwTsI7xn2Om8zGm4A.nBOuiEDssVFHC_N68tDjVA'

}

headers = {
    "ApiKey": "lstzFDEOhfFNMLikKa0am9mgEKLBl49T"
}

mydb = mysql.connector.connect(host='34.67.106.166',database='rentech_db',user='testuser',password='SAdf!AdsWER!@Ew',auth_plugin='mysql_native_password')
df = pd.read_sql("""
SELECT *
FROM web_scraping_masters
WHERE indicator_id = '742' AND source_name = 'Morningstar' AND (exchange_id != '10' AND exchange_id != '12' AND exchange_id != '11') AND status = 'Y'
            """, con = mydb)
risk = df[df['url']== 'https://www.morningstar.com/portfolio']
src_list = risk['source_identifier']
src_list = list(src_list)
indicator = risk[["master_id","indicator_id","source_indicator_name"]]
df = indicator.loc[:, ~indicator.columns.duplicated()]
source_map = {}
for index, row in df.iterrows():
    if row['master_id'] not in source_map:
        source_map[row['master_id']] = []
    
    source_map[row['master_id']].append({row['indicator_id']:row['source_indicator_name']})

master = []
for i in source_map:
    master.append(i)
src_list = list([i for j, i in enumerate(src_list) if i not in src_list[:j]]) 
print("master",master)
# print("src",src_list)
def fetch_data():
  
    main_map = {}
    for source, master__id in zip(src_list, master):
        print(master__id)
        portfolio_url = "https://www.morningstar.com/" + source + "/performance"
        site_url = "https://www.morningstar.com/" + source + "/quote"
        try:
            req = requests.get(portfolio_url).text
            try :
                if len(req.split('byId:{"')[1].split('}},')[0].split(',')) > 1:
                    morning_id = req.split('byId:{"')[1].split('":')[1].split(",")[1].split(":")[0]
                else :
                    morning_id = req.split('byId:{"')[1].split('":')[0]
            except :
                try:
                    soup = BeautifulSoup(req, 'lxml')
                    try:
                        morning_id = str(soup.find_all('script')[5]).split('}},markets')[0].split('byId:{')[1].split(':')[-2].split(',')[-1].replace('"','')
                    except:
                        morning_id = str(soup.find_all('script')[6]).split('}},markets')[0].split('byId:{')[1].split(':')[-2].split(',')[-1].replace('"','')
                    # print(morning_id)
                    # print(site_url)
                except:
                    try:
                        # print("in except")
                        req = requests.get(portfolio_url).text
                        soup = BeautifulSoup(req, 'lxml')
                        try:
                            morning_id = str(soup.find_all('script')[5]).split('byId:{"')[1].split('}}')[0].split(':')[0]
                        except:
                            morning_id = str(soup.find_all('script')[6]).split('byId:{"')[1].split('}}')[0].split(':')[0]
                        morning_id = morning_id.replace('"', '')
                        # print(morning_id)
                    except:
                        
                        print("IN Except for finding morning_id")
                        print(site_url)
                        continue

            dat = "https://api-global.morningstar.com/sal-service/v1/etf/quote/realTime/{}/data?secExchangeList=&random=0.05994721785880497&clientId=MDC&benchmarkId=category&version=3.31.0".format(morning_id)
            while True:
                try:
                    date_response = requests.request("GET", dat, headers=headers1).json()
                except Exception as e:
                    print(e)
                    sleep(2)
                    date_response = requests.request("GET", dat, headers=headers1).json()
                break
            site_date = date_response['asOfDateLastPriceFund'].split("T")[0] 
            print(site_date)
            if master__id not in main_map:
                main_map[master__id] = {}
            TIME = "00:00:00"

            main_map[master__id]["time"] = TIME
            main_map[master__id]["date"] = site_date
            main_map[master__id]["indicators"] = {}
            json_url = "https://api-global.morningstar.com/sal-service/v1/etf/portfolio/v2/sector/{}/data?locale=en&clientId=MDC&benchmarkId=category&version=3.31.0".format(morning_id)
            while True:
                try:
                    response = requests.get(json_url,headers=headers1).json()
                except Exception as e:
                    response = requests.get(json_url,headers=headers1).json()
                break
            new_dict1 = funcy.omit(response['FIXEDINCOME']['fundPortfolio'], ('portfolioDate'))
            print(new_dict1)
            total_list = []
            for key,value in new_dict1.items():
                # print({key:value})
                dict1 = {"name":re.sub(r"(\w)([A-Z])", r"\1 \2", str(key)).title(),"Fund_%":str(value)}
                total_list.append(dict1)
            main_map[master__id]["indicators"]["742"] = {"value_data":0,"json_data":total_list}
        except Exception as e:
            print(e)
            print(source)
    # print(json.dumps(main_map))
    main_map = json.dumps(main_map)
    saveToSql(main_map)

def saveToSql(main_map):
    main_map = json.loads(main_map)
    sql_query_start = "INSERT INTO raw_data ( master_id, indicator_id, value_data, json_data, data_type,ts_date,ts_hour,job_id,batch_id) VALUES "
    sql_query_end = " ON DUPLICATE KEY UPDATE  master_id = VALUES(master_id), indicator_id = VALUES(indicator_id), value_data = VALUES(value_data),json_data = VALUES(json_data),data_type = VALUES(data_type), ts_date = VALUES(ts_date) ,ts_hour = VALUES(ts_hour) ,job_id = VALUES(job_id), batch_id = VALUES(batch_id);"
    query_data = ""
    for masterId in main_map:
        if 'date' in main_map[masterId]:
            db_date = main_map[masterId]['date']
            db_time = main_map[masterId]['time']
            for indicatorId in main_map[masterId]["indicators"]:
                db_value_data = main_map[masterId]["indicators"][indicatorId]["value_data"]
                db_json_data = main_map[masterId]["indicators"][indicatorId]["json_data"]
                
                # query_data = query_data + "('"+str(db_date)+"','"+str(db_time)+"','"+str(masterId)+"','"+str(indicatorId)+"','"+str(json.dumps(db_json_data))+"',NULL),"
                
                query_data = query_data + "('"+str(masterId)+"','"+str(indicatorId)+"','"+str(0)+"','"+str(json.dumps(db_json_data))+"','"+str(1)+"','"+str(db_date)+"','"+str(db_time)+"','"+str(1)+"',NULL),"
                # print(query_data)
    try:
        sql = sql_query_start + query_data[:-1] + sql_query_end
        # print("sql:::---",sql)
        connection_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="pynative_pool",pool_size=5,pool_reset_session=True,host='54.237.79.6',database='rentech_db',user='rentech_user',password='N)baegbgqeiheqfi3e9314jnEkekjb',connect_timeout=600000,auth_plugin='mysql_native_password')
        connection_object = connection_pool.get_connection()
        if connection_object.is_connected():
            cursor = connection_object.cursor()
            cursor.execute(sql)
            connection_object.commit()
            print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~inserted data in to db~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    except Error as e:
        print ("Error while connecting to MySQL using Connection pool ", e)
    finally:
        if(connection_object.is_connected()):
            cursor.close()
            connection_object.close()
            print("MySQL connection is closed")

def scrape():
    fetch_data()

scrape()

import pandas as pd
import requests
import datetime
import concurrent.futures
import json
import mysql.connector
import logging as log
session = requests.Session()
log.basicConfig(filename="scraper_run_log.txt",filemode='a',level=log.INFO)
my_log = log.getLogger()
to_dt = today = datetime.datetime.today().strftime('%Y-%m-%d')
from_dt = (datetime.datetime.strptime(to_dt, '%Y-%m-%d').date() + datetime.timedelta(days=-3650)).strftime('%Y-%m-%d')
# col_to_indicator_file = '/home/ubuntu/rentech/cfra_scrapers/equity_stars_api1/api1_col_to_indicator_map.json'
# map_file = '/home/ubuntu/rentech/cfra_scrapers/equity_stars_api1/ticker_to_masterid_map.csv'
col_to_indicator_file = r'D:\\sriram\\agrud\\cfra\\CFRA-API\\cfra_server_files\\api1\\api1_col_to_indicator_map.json'
map_file = r'D:\\sriram\\agrud\\cfra\\CFRA-API\\cfra_server_files\\api1\\ticker_to_masterid_map.csv'
hit = 1 
mainDf = pd.DataFrame()

def get_access_token():
    token_URL = "https://auth.cfraresearch.com/oauth2/token"
    client_ID = "3l7nhm06sin88ru1gjs06nvnhv"
    clientSecret = "1s2uvqtvp8fo9scimkarviohdb35fgr5iqmugsh8e6hvpejdj199"
    response = requests.post(
        token_URL,
        data={"grant_type": "client_credentials"},
        auth=(client_ID, clientSecret),
    )
    token = 'Bearer ' + response.json()["access_token"] 
    my_log.info('access token accquired')
    return token

def get_tickers():
    dataset = []
    df = pd.read_csv(map_file)
    for i,row in df.iterrows():
        data = {}
        data['ticker'] = row[0]
        data['exchange_code'] = row[1]
        dataset.append(data)
    return dataset

def get_api_urls():
    api_urls = []
    dataset = get_tickers()
    for input in dataset:
        ticker = input['ticker']
        exchange_code = input['exchange_code']
        api_url = f"https://api.cfraresearch.com/equity/data/stars/ticker/{ticker}/exchange/{exchange_code}?date_from={from_dt}&date_to={to_dt}"
        api_urls.append(api_url)
    return api_urls

def fetch_data(api_url,header):
    global mainDf,hit
    ticker = api_url.split('/')[-3]
    exchange_code = api_url.split('/')[-1].split('?')[0]
    result = session.get(api_url, headers=header).json()
    try:
        if 'CFRA_ENTITY_UNKNOWN' in result['error_code']:
            return 0
    except KeyError:
        pass
    ticker = result['result']['ticker']
    hit += 1
    star_data = result['result']['stars_data']
    df = pd.DataFrame(star_data)
    df.insert(loc = 0,column = 'ticker',value = ticker)
    df.insert(loc = 1,column = 'exchange_code',value = exchange_code)
    mainDf = mainDf.append(df)

def get_result():
    df = mainDf
    df = df.rename(columns={"exchange code" : "exchange_code"}) 
    df = df.dropna(subset=['actual_from','stars_rank','target_price'])
    map = pd.read_csv(map_file)
    map = map.rename(columns={"source_exchange" : "exchange_code"}) 
    map = map.rename(columns={"source_symbol" : "ticker"}) 
    with open(col_to_indicator_file, 'r', encoding="utf-8") as f:
        colToIndicator = json.load(f)
    comb_data = pd.merge(df,map,on=['ticker','exchange_code'],how='left')
    result = []
    for i, row in comb_data.iterrows():
        row2 = row.to_dict()
        master_id = row2['master_id']
        ts_date = row2['actual_from']
        for k, v in row2.items():
            if k in colToIndicator:
                indicatorId = colToIndicator[k]
                if type(v) == float or type(v) == int or  v.isnumeric(): 
                    dataType = 0
                    value_data = v
                    json_data = None
                result.append([master_id,indicatorId,value_data,json_data,dataType,ts_date])
    my_log.info('resultset created successfully')
    return result

def data_to_df(result):
    df2 = pd.DataFrame(result)
    cols=['masterid','indicatorid','value_data','json_data','datatype','ts_date']
    df2.columns = cols
    df2.to_csv('issuers_data.csv',index=False,columns=cols)

def db_insert(result):
    try:
        db_conn = mysql.connector.connect(host='54.237.79.6',database='rentech_db',user='rentech_user',password='N)baegbgqeiheqfi3e9314jnEkekjb',auth_plugin='mysql_native_password')
        cursor = db_conn.cursor()
        sql = """INSERT INTO `raw_data_test_1` (`id`, `master_id`, `indicator_id`, `value_data`, `json_data`, `data_type`, `ts_date`, `ts_hour`, `job_id`, `timestamp`) VALUES 
        (NULL, %s, %s, %s, %s, %s, %s, '0:0:0', 10, NOW()) ON DUPLICATE KEY UPDATE  
        master_id = VALUES(master_id), indicator_id = VALUES(indicator_id), value_data = VALUES(value_data),
        json_data = VALUES(json_data),data_type = VALUES(data_type), ts_date = VALUES(ts_date) ,ts_hour = VALUES(ts_hour) ,
        job_id = VALUES(job_id), batch_id = VALUES(batch_id);"""
        cursor.executemany(sql, result)
        my_log.info(f'{cursor.rowcount} rows inserted.')
        db_conn.commit()
    except Exception as e:
            my_log.info ("MYSQL Error: ", e)
    finally:
        if(db_conn.is_connected()):
            cursor.close()
            db_conn.close()
            my_log.info("MySQL connection is closed")

def scrape():
    global hit
    header = {
        'accept': '*/*',
        'Authorization':get_access_token(),
        'x-api-key': 'z18plRKX0s6BEZuOB34WQZHEDUlgBI1aXXrwP6Ha'
    }
    urls = get_api_urls()
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as link_executor:
        [link_executor.submit(fetch_data,link,header) for link in urls]
    pass
    my_log.info(f'data found for {hit} tickers')

if __name__ == "__main__":
    my_log.info(f'----------------------{datetime.datetime.now()}----------------------------')
    scrape()
    result = get_result()
    db_insert(result)
    my_log.info(f'----------------------{datetime.datetime.now()}----------------------------')
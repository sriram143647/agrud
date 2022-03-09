import pandas as pd
import requests
import json
import  datetime
import mysql.connector
session = requests.session()
# col_to_indicator_file = '/home/ubuntu/rentech/cfra_scrapers/quant_universe_api2/api2_col_to_indicator_map.json'
# map_file = '/home/ubuntu/rentech/cfra_scrapers/quant_universe_api2/ticker_to_masterid_map.csv'
# data_file = '/home/ubuntu/rentech/cfra_scrapers/quant_universe_api2/'
col_to_indicator_file = r'D:\\sriram\\agrud\\cfra\\CFRA-API\\cfra_server_files\\api2\\api2_col_to_indicator_map.json'
map_file = r'D:\\sriram\\agrud\\cfra\\CFRA-API\\cfra_server_files\\api2\\ticker_to_masterid_map.csv'
data_file = r'D:\\sriram\\agrud\\cfra\\CFRA-API\\cfra_server_files\\api2\\'

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
  print('access token acquired') 
  return token

def get_result(df):
    comb_data = df
    comb_data = comb_data[['recommendation','recommendation_date','risk_eval','risk_eval_date','master_id']]
    with open(col_to_indicator_file, 'r', encoding="utf-8") as f:
        colToIndicator = json.load(f)
    result = []
    for i, row in comb_data.iterrows():
        row2 = row.to_dict()
        master_id = row2['master_id']
        value_data = 0
        keys = list(row2.keys())
        for k in keys:
            if k in colToIndicator and k == 'recommendation':
                indicatorId = colToIndicator[k]
                json_data = json.dumps({'TEXT':row2[k]})
                dataType = 3
                ts_date = row2['recommendation_date'].split('T')[0]
                result.append([master_id,indicatorId,value_data,json_data,dataType,ts_date])
            elif k in colToIndicator and k == 'risk_eval':
                indicatorId = colToIndicator[k]
                json_data = json.dumps({'TEXT':row2[k]})
                dataType = 3
                ts_date = row2['risk_eval_date'].split('T')[0]
                result.append([master_id,indicatorId,value_data,json_data,dataType,ts_date])
    print('result set created successfully')
    return result

def db_insert(result):
    try:
        db_conn = mysql.connector.connect(host='54.237.79.6',database='rentech_db',user='rentech_user',password='N)baegbgqeiheqfi3e9314jnEkekjb',auth_plugin='mysql_native_password')
        cursor = db_conn.cursor()
        sql = """INSERT INTO `raw_data_test_1` (`id`, `master_id`, `indicator_id`, `value_data`, `json_data`, `data_type`, `ts_date`, `ts_hour`, `job_id`, `timestamp`) VALUES 
        (NULL, %s, %s, %s, %s, %s, %s, '0:0:0', 10, NOW())
        ON DUPLICATE KEY UPDATE  master_id = VALUES(master_id), indicator_id = VALUES(indicator_id), value_data = VALUES(value_data),
        json_data = VALUES(json_data),data_type = VALUES(data_type), ts_date = VALUES(ts_date) ,ts_hour = VALUES(ts_hour) ,
        job_id = VALUES(job_id), batch_id = VALUES(batch_id);"""
        cursor.executemany(sql, result)
        print(f'{cursor.rowcount} rows inserted.')
        db_conn.commit()
    except Exception as e:
            print ("MYSQL Error: ",e)
    finally:
        if(db_conn.is_connected()):
            cursor.close()
            db_conn.close()
            print("MySQL connection is closed")

def scrape():
    headers = {
    'accept': '*/*',
    'Authorization': get_access_token(),
    'x-api-key': 'z18plRKX0s6BEZuOB34WQZHEDUlgBI1aXXrwP6Ha'
    }
    mainDf = pd.DataFrame()
    opts = ['strong_sell', 'sell', 'hold', 'buy', 'strong_buy']
    for p in opts:
        api_url = f"https://api.cfraresearch.com/equity/quant/universe?recommendation={p}"
        result = session.get(api_url, headers=headers).json()
        df = pd.DataFrame(result['result'])
        df['recommendation'] = p
        mainDf = mainDf.append(df)
    print('all api urls data fetched successfully')
    df1 = pd.read_csv(map_file)
    df2 = mainDf
    df3 = df1.merge(df2, how = "left", left_on = ["source_symbol", "source_exchange"], right_on=["ticker", "exchange_code"])
    df3 = df3.dropna()
    df3.to_csv('api2.csv', index=False)
    return df3

if __name__ == "__main__":
    print(f'-------------------{datetime.datetime.now()}-------------------------')
    df = scrape()
    result = get_result(df)
    # db_insert(result)
    print(f'-------------------{datetime.datetime.now()}-------------------------')

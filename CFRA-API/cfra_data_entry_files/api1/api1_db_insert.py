import pandas as pd
import json
import mysql.connector
import os
col_to_indicator_file = os.getcwd()+'\\api1_col_to_indicator_map.json'
map_file = os.getcwd()+'\\ticker_to_masterid_map.csv'

def get_result():
    df = pd.read_csv('api1_data.csv',encoding='utf-8')
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
    return result

def db_insert(result):
    #server db connection
    try:
        db_conn = mysql.connector.connect(host='54.237.79.6',database='rentech_db',user='rentech_user',password='N)baegbgqeiheqfi3e9314jnEkekjb',auth_plugin='mysql_native_password')
        cursor = db_conn.cursor()
        sql = """INSERT INTO `raw_data_test_1` (`id`, `master_id`, `indicator_id`, `value_data`, `json_data`, `data_type`, `ts_date`, `ts_hour`, `job_id`, `timestamp`) VALUES 
        (NULL, %s, %s, %s, %s, %s, %s, '0:0:0', 10, NOW()) ON DUPLICATE KEY UPDATE  
        master_id = VALUES(master_id), indicator_id = VALUES(indicator_id), value_data = VALUES(value_data),
        json_data = VALUES(json_data),data_type = VALUES(data_type), ts_date = VALUES(ts_date) ,ts_hour = VALUES(ts_hour) ,
        job_id = VALUES(job_id), batch_id = VALUES(batch_id);"""
        cursor.executemany(sql, result)
        print(cursor.rowcount, "records affected.")
        db_conn.commit()
    except Exception as e:
            print ("Error while connecting to MySQL using Connection pool ", e)
    finally:
        if(db_conn.is_connected()):
            cursor.close()
            db_conn.close()
            print("MySQL connection is closed")

def start():
    result = get_result()
    db_insert(result)

if __name__ == '__main__':
    start()
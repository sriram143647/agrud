import pandas as pd
import json
import mysql.connector
import os
col_to_indicator_file = os.getcwd()+'\\api2_col_to_indicator_map.json'
map_file = os.getcwd()+'\\ticker_to_masterid_map.csv'


def get_result():
    df = pd.read_csv('api2_temp_data.csv',encoding='utf-8')
    map = pd.read_csv(map_file)
    map = map.rename(columns={"source_exchange" : "exchange_code","source_symbol" : "ticker"}) 
    with open(col_to_indicator_file, 'r', encoding="utf-8") as f:
        colToIndicator = json.load(f)
    df2 = pd.merge(df,map, how = "left", left_on = ["ticker", "exchange_code"], right_on=["ticker", "exchange_code"])
    comb_data = df2.dropna()
    comb_data = comb_data[['recommendation','recommendation_date','risk_eval','risk_eval_date','master_id']]
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
    return result

def db_insert(result):
    #server db connection
    try:
        db_conn = mysql.connector.connect(host='54.237.79.6',database='rentech_db',user='rentech_user',password='N)baegbgqeiheqfi3e9314jnEkekjb',auth_plugin='mysql_native_password')
        cursor = db_conn.cursor()
        sql = """INSERT INTO `raw_data_test_1` (`id`, `master_id`, `indicator_id`, `value_data`, `json_data`, `data_type`, `ts_date`, `ts_hour`, `job_id`, `timestamp`) VALUES 
        (NULL, %s, %s, %s, %s, %s, %s, '0:0:0', 10, NOW())
        ON DUPLICATE KEY UPDATE  master_id = VALUES(master_id), indicator_id = VALUES(indicator_id), value_data = VALUES(value_data),
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
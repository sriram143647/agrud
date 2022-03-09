import pandas as pd
import numpy as np
import json
import mysql.connector
import os
col_to_indicator_file = os.getcwd()+'\\api3_col_to_indicator_map.json'
map_file = os.getcwd()+'\\ticker_to_masterid_map.csv'   

def get_result():
    df = pd.read_csv('api3_data.csv',encoding='utf-8')
    df = df.rename(columns={"exchange code" : "exchange_code"}) 
    map = pd.read_csv(map_file)
    map = map.rename(columns={"source_exchange" : "exchange_code"}) 
    map = map.rename(columns={"source_symbol" : "ticker"}) 
    with open(col_to_indicator_file, 'r', encoding="utf-8") as f:
        colToIndicator = json.load(f)
    df[['price_to_oper_eps_2022E', 'price_to_oper_eps_2021E']] = df[['price_to_oper_eps_2022E', 'price_to_oper_eps_2021E']].replace("NM", np.nan)
    comb_data = pd.merge(df,map,on=['ticker','exchange_code'],how='left')
    pub_dt = [
        "3_yr_proj_eps_cagr_prcntg","analyst","business_summary","fair_value","income_estimates","insider_activity","pdf_url","research_notes",
        "price_at_publication","publication_date","quality_risk_assessment","reporting_frequency","revenue_estimates","sub_industry_outlook",
        "summary","trading price","technical_evaluation"
    ]
    result = []
    for i, row in comb_data.iterrows():
        row2 = row.to_dict()
        master_id = row2['master_id']
        for k, v in row2.items():
            if pd.isna(v):
                continue
            if k not in colToIndicator:
                continue
            if k in pub_dt: 
                indicatorId = colToIndicator[k]
                ts_date = row2['publication_date'].split('T')[0]
                if k in ['income_estimates','revenue_estimates','fair_value']:
                    json_data = json.dumps([json.loads(v)])
                    dataType = 1
                    value_data = 0 
                    result.append([master_id,indicatorId,value_data,json_data,dataType,ts_date])
                elif k in ['3_yr_proj_eps_cagr_prcntg','price_at_publication']: 
                    dataType = 0
                    value_data = v
                    json_data = None
                    result.append([master_id,indicatorId,value_data,json_data,dataType,ts_date])
                elif type(v) == str: 
                    json_data = json.dumps({'TEXT':v})
                    dataType = 3
                    value_data = 0
                    result.append([master_id,indicatorId,value_data,json_data,dataType,ts_date])
            elif k in ['investment_risk_rationale']:
                indicatorId = colToIndicator[k]
                if pd.isna(row2['investment_risk_rationale_date']):
                    continue
                ts_date = row2['investment_risk_rationale_date'].split('T')[0]
                json_data = json.dumps({'TEXT':v})
                dataType = 3
                value_data = 0
                result.append([master_id,indicatorId,value_data,json_data,dataType,ts_date])
            elif k in ['highlights']:
                indicatorId = colToIndicator[k]
                if pd.isna(row2['highlights_date']):
                    continue
                ts_date = row2['highlights_date'].split('T')[0]
                json_data = json.dumps({'TEXT':v})
                dataType = 3
                value_data = 0
                result.append([master_id,indicatorId,value_data,json_data,dataType,ts_date])
            elif k in ['oper_eps_2022E','price_to_oper_eps_2022E']:
                indicatorId = colToIndicator[k]
                ts_date = '2022-12-31'
                json_data = None
                dataType = 0
                value_data = v
                result.append([master_id,indicatorId,value_data,json_data,dataType,ts_date])
            elif k in ['oper_eps_2023E']:
                indicatorId = colToIndicator[k]
                ts_date = '2023-12-31'
                json_data = None
                dataType = 0
                value_data = v
                result.append([master_id,indicatorId,value_data,json_data,dataType,ts_date])
            elif k in ['oper_eps_2021E','price_to_oper_eps_2021E']:
                indicatorId = colToIndicator[k]
                ts_date = '2021-12-31'
                json_data = None
                dataType = 0
                value_data = v
                result.append([master_id,indicatorId,value_data,json_data,dataType,ts_date])
            
            if float(result[-1][2]) < 0:
                result = result[:-1]
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
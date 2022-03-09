import pandas as pd
import numpy as np
import requests
import json
import os
import csv
import datetime
import mysql.connector
import concurrent.futures
from bs4 import BeautifulSoup
session = requests.Session()
# col_to_indicator_file = '/home/ubuntu/rentech/cfra_scrapers/equity_research_api3/api3_col_to_indicator_map.json'
# map_file = '/home/ubuntu/rentech/cfra_scrapers/equity_research_api3/ticker_to_masterid_map.csv'
# data_file = '/home/ubuntu/rentech/cfra_scrapers/equity_research_api3/'
col_to_indicator_file = r'D:\\sriram\\agrud\\cfra\\CFRA-API\\cfra_server_files\\api3\\api3_col_to_indicator_map.json'
map_file = r'D:\\sriram\\agrud\\cfra\\CFRA-API\\cfra_server_files\\api3\\ticker_to_masterid_map.csv'
data_file = r'D:\\sriram\\agrud\\cfra\\CFRA-API\\cfra_server_files\\api3\\'
mainDf = pd.DataFrame()
hit = 0 

def write_header(): 
    with open(data_file+'api3_data.csv', mode='a', encoding='utf-8',newline="") as output_file:
        writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow([
          "3_yr_proj_eps_cagr_prcntg","analyst","business_summary","fair_value","highlights","highlights_date","income_estimates",
          "insider_activity","investment_risk_rationale","investment_risk_rationale_date","oper_eps_2022E","oper_eps_2023E",
          "pdf_url","price_at_publication","publication_date","price_to_oper_eps_2022E","quality_risk_assessment","reporting_frequency",
          "revenue_estimates","sub_industry_outlook","summary","trading price","technical_evaluation","oper_eps_2021E",
          "price_to_oper_eps_2021E","ticker","exchange_code"
        ])

def write_data(data):
    with open(data_file+'api3_data.csv', mode='a', encoding='utf-8', newline="") as output_file:
        writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(data)

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

def get_result():
    df = mainDf
    cols = [ "3_yr_proj_eps_cagr_prcntg","analyst","business_summary","fair_value","highlights","highlights_date","income_estimates",
          "insider_activity","investment_risk_rationale","investment_risk_rationale_date","oper_eps_2022E","oper_eps_2023E",
          "pdf_url","price_at_publication","publication_date","price_to_oper_eps_2022E","quality_risk_assessment","reporting_frequency",
          "revenue_estimates","sub_industry_outlook","summary","trading price","technical_evaluation","oper_eps_2021E",
          "price_to_oper_eps_2021E","ticker","exchange_code"
        ]
    df.columns = cols
    df = df.rename(columns={"exchange code" : "exchange_code"}) 
    map = pd.read_csv(map_file)
    map = map.rename(columns={"source_exchange" : "exchange_code"}) 
    map = map.rename(columns={"source_symbol" : "ticker"}) 
    with open(col_to_indicator_file, 'r', encoding="utf-8") as f:
        colToIndicator = json.load(f)
    df[['price_to_oper_eps_2022E', 'price_to_oper_eps_2021E']] = df[['price_to_oper_eps_2022E', 'price_to_oper_eps_2021E']].replace("NM", np.nan)
    comb_data = pd.merge(df,map,on=['ticker','exchange_code'],how='left')
    comb_data = comb_data.replace([np.nan],[None])
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
            if v == '' or v == None:
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
    
    print('resultset created successfully')
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
        print(cursor.rowcount, "rows inserted.")
        db_conn.commit()
    except Exception as e:
            print("MYSQL Error: ",e)
    finally:
        if(db_conn.is_connected()):
            cursor.close()
            db_conn.close()
            print("MySQL connection is closed")

def get_data():
    dataset = []
    df = pd.read_csv(map_file)
    for index,row in df.iterrows():
        data = {}
        data['ticker'] = row[0]
        data['exchange_code'] = row[1]
        dataset.append(data)
    return dataset

def get_api_urls():
    api_urls = []
    dataset = get_data()
    for input in dataset[:]:
        ticker = input['ticker']
        exchange_code = input['exchange_code']
        api_url = f"https://api.cfraresearch.com/equity/research/stars-full/ticker/{ticker}/exchange/{exchange_code}"
        api_urls.append(api_url)
    return api_urls

def fetch_data(api_url,header):
    global mainDf,hit
    ticker = api_url.split('/')[-3]
    exchange_code = api_url.split('/')[-1]
    result = session.get(api_url, headers=header).json()
    try:
        if 'CFRA_ENTITY_UNKNOWN' in result['error_code']:
            return 0
    except KeyError:
        pass
    result = result['result']
    try:
        ticker = result['ticker']
    except KeyError:
        return 0
    try:
        exchange_code = result['exchange']
    except KeyError:
        return 0
    hit += 1
    try:
        three_yrs_eps_per = result['3_yr_proj_eps_cagr_prcntg']
    except KeyError:
        three_yrs_eps_per = ''
    try:
        analyst = result['analyst']
    except KeyError:
        analyst = ''
    try:
        busi_summ = BeautifulSoup(result['business_summary'],'html5lib').text
    except KeyError:
        busi_summ = ''
    try:
        f_value = json.dumps(result['fair_value'])
    except KeyError:
        f_value = ''
    try:
        high_lights = BeautifulSoup(result['highlights'],'html5lib').text
    except KeyError:
        high_lights = ''
    try:
        high_lights_date = result['highlights_date']
    except KeyError:
        high_lights_date = ''
    try:
        income_est = json.dumps(result['income_estimates'])
    except KeyError:
        income_est = ''
    try:
        isn_act = result['insider_activity']
    except KeyError:
        isn_act = ''
    try:
        inves_risk_rational = BeautifulSoup(result['investment_risk_rationale'],'html5lib').text
    except KeyError:
        inves_risk_rational = ''
    try:
        inves_risk_rational_dt = result['investment_risk_rationale_date']
    except KeyError:
        inves_risk_rational_dt = ''
    try:
        oper_2022 = result['oper_eps_2022E']
    except KeyError:
        oper_2022 = ''
    try:
        oper_2023 = result['oper_eps_2023E']
    except KeyError:
        oper_2023 = ''
    try:
        pdf_url = result['pdf_url']
    except KeyError:
        pdf_url = ''
    try:
        price_pub = result['price_at_publication']
    except KeyError:
        price_pub = ''
    try:
        pub_date = result['publication_date']
    except KeyError:
        pub_date = ''
    try:
        price_oper_2022 = result['price_to_oper_eps_2022E']
    except KeyError:
        price_oper_2022 = ''
    try:
        qual_risk_asses = BeautifulSoup(result['quality_risk_assessment']['text'],'html5lib').text
    except KeyError:
        qual_risk_asses = ''
    try:
        report_freq = result['reporting_frequency']
    except KeyError:
        report_freq = ''
    try:
        rev_est =  json.dumps(result['revenue_estimates'])
    except KeyError:
        rev_est = ''
    try:
        sub_ind_outlook = BeautifulSoup(result['sub_industry_outlook'],'html5lib').text
    except KeyError:
        sub_ind_outlook = ''
    try:
        summ = BeautifulSoup(result['summary'],'html5lib').text
    except KeyError:
        summ = ''
    try:
        trading_price = result['trading_price']
    except KeyError:
        trading_price = ''
    tech_eval = result['technical_evaluation']
    try:
        oper_2021 = result['oper_eps_2021E']
    except KeyError:
        oper_2021 = ''
    try:
        price_oper_2021 = result['price_to_oper_eps_2021E']
    except KeyError:
        price_oper_2021 = ''
    data = [
        three_yrs_eps_per,analyst,busi_summ,f_value,high_lights,high_lights_date,income_est,isn_act,inves_risk_rational,inves_risk_rational_dt,
        oper_2022,oper_2023,pdf_url,price_pub,pub_date,price_oper_2022,qual_risk_asses,report_freq,rev_est,sub_ind_outlook,summ,
        trading_price,tech_eval,oper_2021,price_oper_2021,ticker,exchange_code
    ]
    mainDf = mainDf.append([data]) 
    write_data(data)


def scrape():
    header = {
    'accept': '*/*',
    'Authorization': get_access_token(),
    'x-api-key': 'z18plRKX0s6BEZuOB34WQZHEDUlgBI1aXXrwP6Ha'
    }
    write_header()
    urls = get_api_urls()
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as link_executor:
        [link_executor.submit(fetch_data,link,header) for link in urls]
    pass
    print(f'data found for {hit} tikcers')

if __name__ == "__main__":
    print(f'----------------------stared at:{datetime.datetime.now()}----------------------------')
    scrape()
    result = get_result()
    # db_insert(result)
    print(f'----------------------finished at:{datetime.datetime.now()}----------------------------')

import mysql.connector.pooling
import mysql.connector
from mysql.connector import Error
from mysql.connector.connection import MySQLConnection
from bs4 import BeautifulSoup
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
from time import sleep
import json
import datetime
import pandas as pd
import traceback
import logging as log
log_file_path = '/home/ubuntu/agrud-scrapers/daily_run/portfolio/scraper_run_log.txt'
log.basicConfig(filename = log_file_path,filemode='a',level=log.INFO)
my_log = log.getLogger()

headers = {
    "ApiKey": "lstzFDEOhfFNMLikKa0am9mgEKLBl49T"
}
headers1 = {
  'apikey': 'lstzFDEOhfFNMLikKa0am9mgEKLBl49T',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
  'x-api-realtime-e': 'eyJlbmMiOiJBMTI4R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.XmuAS3x5r-0MJuwLDdD4jNC6zjsY7HAFNo2VdvGg6jGcj4hZ4NaJgH20ez313H8An9UJrsUj8ERH0R8UyjQu2UGMUnJ5B1ooXFPla0LQEbN_Em3-IG84YPFcWVmEgcs1Fl2jjlKHVqZp04D21UvtgQ4xyPwQ-QDdTxHqyvSCpcE.ACRnQsNuTh1K_C9R.xpLNZ8Cc9faKoOYhss1CD0A4hG4m0M7-LZQ0fISw7NUHwzQs2AEo9ZXfwOvAj1fCbcE96mbKQo8gr7Oq1a2-piYXM1X5yNMcCxEaYyGinpnf6PGqbdr6zbYZdqyJk0KrxWVhKSQchLJaLGJOts4GlpqujSqJObJQcWWbkJQYKG9K7oKsdtMAKsHIVo5-0BCUbjKVnHJNsYwTsI7xn2Om8zGm4A.nBOuiEDssVFHC_N68tDjVA'

}

def get_data():
    mydb = mysql.connector.connect(host='34.67.106.166',database='rentech_db',user='testuser',password='SAdf!AdsWER!@Ew',auth_plugin='mysql_native_password')
    df = pd.read_sql("""
            SELECT *
                FROM web_scraping_masters where status =  'Y' and source_name = 'Morningstar' and json_structure_type = 0
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
    return src_list,master,source_map

def fetch_data(src_list,master,source_map):
    main_map = {}
    for source,master__id in zip(src_list,master):
        portfolio_url = "https://www.morningstar.com/" + source + "/portfolio"
        site_url = "https://www.morningstar.com/" + source + "/quote"
        try:
            for _ in range(3):
                try:
                    req = requests.get(portfolio_url).text
                    if len(req.split('byId:{"')[1].split('}},')[0].split(',')) > 1:
                        morning_id = req.split('byId:{"')[1].split('":')[1].split(",")[1].split(":")[0]
                    else :
                        morning_id = req.split('byId:{"')[1].split('":')[0]
                    break
                except:
                    sleep(2)
                    continue
                

            dat = "https://api-global.morningstar.com/sal-service/v1/etf/quote/realTime/{}/data?secExchangeList=&random=0.05994721785880497&clientId=MDC&benchmarkId=category&version=3.31.0".format(morning_id)
            for _ in range(5):
                try:
                    date_response = requests.get(dat, headers=headers1).json()
                    break
                except:
                    sleep(2)
                    continue
                

            my_log.info(master__id)
            site_date = date_response['asOfDateLastPriceFund'].split("T")[0]
            #s_date = datetime.datetime.strptime(site_date,'%Y-%m-%d') - datetime.timedelta(days=1)
            #site_date = s_date.strftime('%Y-%m-%d')
            my_log.info(site_date)
            # site_date = (datetime.datetime.strptime(site_date, "%Y-%m-%d") + datetime.timedelta(days = -1)).strftime("%Y-%m-%d")
            my_log.info('-------------------------------------------')
            if master__id not in main_map:
                main_map[master__id] = {}

            TIME = "00:00:00"

            main_map[master__id]["time"] = TIME
            main_map[master__id]["date"] = site_date
            main_map[master__id]["indicators"] = {}

            avg_mkt_cap_url = "https://api-global.morningstar.com/sal-service/v1/etf/process/marketCap/{}/data?languageId=en&locale=en&clientId=MDC&benchmarkId=category&component=sal-components-mip-market-cap&version=3.31.0".format(morning_id)

            for _ in range(3):
                try:
                    mkt_json = requests.get(avg_mkt_cap_url,headers=headers).json()
                    break
                except :
                    sleep(2)
                    continue
        
            avgMarketCap = mkt_json['fund']['avgMarketCap']

            if avgMarketCap == 'None' or avgMarketCap == 0 or avgMarketCap == None or avgMarketCap == "" or avgMarketCap == 0:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 537:
                        main_map[master__id]["indicators"]["537"] = {"value_data":avgMarketCap,"json_data":None}

            tab_url = "https://api-global.morningstar.com/sal-service/v1/etf/process/stockStyle/v2/{}/data?languageId=en&locale=en&clientId=MDC&benchmarkId=category&component=sal-components-mip-measures&version=3.31.0".format(morning_id)
            
            for _ in range(3):
                try:
                    tab_json = requests.get(tab_url,headers=headers).json()
                    break
                except:
                    sleep(2)
                    continue
                    
            prc_earning = tab_json['fund']['prospectiveEarningsYield']

            if prc_earning == 0 or prc_earning == 'None' or prc_earning == '0' or prc_earning == None:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 574:
                        main_map[master__id]["indicators"]["574"] = {"value_data":prc_earning,"json_data":None}

            prc_book = tab_json['fund']['prospectiveBookValueYield']

            if prc_book == '0' or prc_book == 'None' or prc_book == None or prc_book == "" or prc_book == 0:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 575:
                        main_map[master__id]["indicators"]["575"] = {"value_data":prc_book,"json_data":None}

            prc_sales = tab_json['fund']['prospectiveRevenueYield']

            if prc_sales == 'None' or prc_sales == 0 or prc_sales == '0' or prc_sales == None:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 576:
                        main_map[master__id]["indicators"]["576"] = {"value_data":prc_sales,"json_data":None}


            prc_cash_flow = str(tab_json['fund']['prospectiveCashFlowYield']).replace("None",'0')

            if prc_cash_flow == '0' or prc_cash_flow == 'None' or prc_cash_flow == '0' or prc_cash_flow == None:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 577:
                        main_map[master__id]["indicators"]["577"] = {"value_data":prc_cash_flow,"json_data":None}

            dividend_yield = tab_json['fund']['prospectiveDividendYield']

            if dividend_yield == 0 or dividend_yield == 'None' or dividend_yield == '0' or dividend_yield == None:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 578:
                        main_map[master__id]["indicators"]["578"] = {"value_data":dividend_yield,"json_data":None}

            long_term = tab_json['fund']['forecasted5YearEarningsGrowth']


            if long_term == 'None' or long_term == 0 or long_term == '0' or long_term == None:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 19:
                        main_map[master__id]["indicators"]["19"] = {"value_data":long_term,"json_data":None}

            historical_earning = tab_json['fund']['forecastedEarningsGrowth']

            if historical_earning == 0 or historical_earning == 'None' or historical_earning == '0' or historical_earning == None:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 20:
                        main_map[master__id]["indicators"]["20"] = {"value_data":historical_earning,"json_data":None}

            sales_growth = tab_json['fund']['forecastedRevenueGrowth']


            if sales_growth == 0 or sales_growth == 'None' or sales_growth == '0' or sales_growth == None:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 21:
                        main_map[master__id]["indicators"]["21"] = {"value_data":sales_growth,"json_data":None}


            cash_flow = str(tab_json['fund']['forecastedCashFlowGrowth']).replace("None",'0')

            if cash_flow == 0 or cash_flow == 'None' or cash_flow == '0' or cash_flow == None:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 22:
                        main_map[master__id]["indicators"]["22"] = {"value_data":cash_flow,"json_data":None}

            book_value = tab_json['fund']['forecastedBookValueGrowth']

            if book_value == 0 or book_value == 'None' or book_value == '0' or book_value == None:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 23:
                        main_map[master__id]["indicators"]["23"] = {"value_data":book_value,"json_data":None}

            # capture detail from category
            category_price_earning = tab_json['categoryAverage']['prospectiveEarningsYield']  

            if category_price_earning == 0 or category_price_earning == '0' or category_price_earning == None or category_price_earning == 'None':
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 579:
                        main_map[master__id]["indicators"]["579"] = {"value_data":category_price_earning,"json_data":None}

            category_price_book = tab_json['categoryAverage']['prospectiveBookValueYield']
        
            if category_price_book == 0 or category_price_book == 'None' or category_price_book == '0' or category_price_book == None:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 580:
                        main_map[master__id]["indicators"]["580"] = {"value_data":category_price_book,"json_data":None}


            category_price_sales = tab_json['categoryAverage']['prospectiveRevenueYield']

            if category_price_sales == 0 or category_price_sales == 'None' or category_price_sales == '0' or category_price_sales == None:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 581:
                        main_map[master__id]["indicators"]["581"] = {"value_data":category_price_sales,"json_data":None}

            category_price_cash_flow = str(tab_json['categoryAverage']['prospectiveCashFlowYield']).replace("None",'0')
         
            if category_price_cash_flow == 0 or category_price_cash_flow == 'None' or category_price_cash_flow == '0' or category_price_cash_flow == None:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 582:
                        main_map[master__id]["indicators"]["582"] = {"value_data":category_price_cash_flow,"json_data":None}

            category_dividend_yield = tab_json['categoryAverage']['prospectiveDividendYield']
         
            if category_dividend_yield == 0 or category_dividend_yield == 'None' or category_dividend_yield == None or category_dividend_yield == '0':
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 583:
                        main_map[master__id]["indicators"]["583"] = {"value_data":category_dividend_yield,"json_data":None}

            category_long_term = tab_json['categoryAverage']['forecasted5YearEarningsGrowth']
         

            if category_long_term == 0 or category_long_term == 'None' or category_long_term == None or category_long_term == '0':
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 24:
                        main_map[master__id]["indicators"]["24"] = {"value_data":category_long_term,"json_data":None}

            category_historical_earning = tab_json['categoryAverage']['forecastedEarningsGrowth']
           
            if category_historical_earning == 0 or category_historical_earning == 'None' or category_historical_earning == '0' or category_historical_earning == None:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 25: 
                        main_map[master__id]["indicators"]["25"] = {"value_data":category_historical_earning,"json_data":None}


            category_sales_growth = tab_json['categoryAverage']['forecastedRevenueGrowth']
       
            if category_sales_growth == 0 or category_sales_growth == 'None' or category_sales_growth == '0' or category_sales_growth == None:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 26:
                        main_map[master__id]["indicators"]["26"] = {"value_data":category_sales_growth,"json_data":None}

            category_cash_flow = str(tab_json['categoryAverage']['forecastedCashFlowGrowth']).replace("None",'0')
        
            if category_cash_flow == 0 or category_cash_flow == 'None' or category_cash_flow == '0' or category_cash_flow == None:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 27:
                        main_map[master__id]["indicators"]["27"] = {"value_data":category_cash_flow,"json_data":None}

            category_book_value = tab_json['categoryAverage']['forecastedBookValueGrowth']
         
            if category_book_value == 0 or category_book_value == 'None' or category_book_value == '0' or category_book_value == None:
                pass
            else:
                for indicator_id  in source_map[master__id]:
                    if list(indicator_id.keys())[0] == 28:
                        main_map[master__id]["indicators"]["28"] = {"value_data":category_book_value,"json_data":None}

            my_log.info(portfolio_url)
            my_log.info("***********************************************")
        except Error as e:
            print(site_url)
            print(e)
            pass
    return main_map

def send_email(row_count=0,status=None,text=None):
    email_user = 'agrud.scrapersmail123@gmail.com'
    email_password = 'qwerty@123'
    email_send = 'agrud.2021@gmail.com'
    subject = f"portfolio scraper result: {datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')}"
    msg = MIMEMultipart()
    msg['From'] = email_user
    msg['To'] = email_send
    msg['Subject'] = subject
    body = f"Total records inserted: {row_count}\ncronjob status: {status}\nError:{text}"
    msg.attach(MIMEText(body,'plain'))
    text = msg.as_string()
    server = smtplib.SMTP('smtp.gmail.com',587)
    server.starttls()
    server.login(email_user,email_password)
    server.sendmail(email_user,email_send,text)
    server.quit()
    my_log.info('email sent')

def saveToSql(main_map):  
    sql_query_start = "INSERT INTO raw_data ( master_id, indicator_id, value_data, json_data, data_type,ts_date,ts_hour,job_id,batch_id) VALUES "
    sql_query_end = " ON DUPLICATE KEY UPDATE  master_id = VALUES(master_id), indicator_id = VALUES(indicator_id), value_data = VALUES(value_data),json_data = VALUES(json_data),data_type = VALUES(data_type), ts_date = VALUES(ts_date) ,ts_hour = VALUES(ts_hour) ,job_id = VALUES(job_id), batch_id = VALUES(batch_id);"
    query_data = ""
    for masterId in main_map:
        if 'date' in main_map[masterId]:
            db_date = main_map[masterId]['date']
            db_time = main_map[masterId]['time']
            for indicatorId in main_map[masterId]["indicators"]:
                db_value_data = main_map[masterId]["indicators"][indicatorId]["value_data"]

                # db_json_data = main_map[masterId]["indicators"][indicatorId]["json_data"]

                query_data = query_data + "('"+str(masterId)+"','"+str(indicatorId)+"','"+str(
                    db_value_data)+"',NULL,'"+str(0)+"','"+str(db_date)+"','"+str(db_time)+"','"+str(1)+"',NULL),"

                # query_data = query_data + "('"+str(db_date)+"','"+str(db_time)+"','"+str(
                #     masterId)+"','"+str(indicatorId)+"',NULL,'"+str(db_value_data)+"'),"

    try:
        sql = sql_query_start + query_data[:-1] + sql_query_end
        connection_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="pynative_pool", pool_size=5, pool_reset_session=True, host='54.237.79.6',database='rentech_db', user='rentech_user', password='N)baegbgqeiheqfi3e9314jnEkekjb', auth_plugin='mysql_native_password', connect_timeout=600000)
        connection_object = connection_pool.get_connection()
        if connection_object.is_connected():
            cursor = connection_object.cursor()
            cursor.execute(sql)
            rows = cursor.rowcount
            connection_object.commit()
            my_log.info(f"{rows} records are inserted into db")
    except Error as e:
        my_log.info("Error while connecting to MySQL using Connection pool ", e)
        status = 'Fail'
        send_email(rows,status)
    finally:
        if(connection_object.is_connected()):
            status = 'Success'
            cursor.close()
            connection_object.close()
            my_log.info("MySQL connection is closed")
            send_email(rows,status)

def start():
    src_list,master,source_map = get_data()
    main_map = fetch_data(src_list,master,source_map)
    saveToSql(main_map)

if __name__ == "__main__":  
    my_log.info(f'----------------------started at:{datetime.datetime.now()}----------------------------')
    try:
        start()
    except Exception as e:
        my_log.setLevel(log.ERROR)
        my_log.error(f'Error:{e}',exc_info=True)
        error_stack = ''.join(traceback.format_stack()).strip()
        send_email(status='Fail',text=str(e)+'\nerror stacktrace:\n'+error_stack)
    my_log.setLevel(log.INFO)
    my_log.info(f'----------------------finished at:{datetime.datetime.now()}----------------------------')


import markets_ft.markets_ft_com_scraper as market
import fundsquare.fundsquare_net_scraper  as fundsquare
import fundinfo.fundinfo_private_investor_scraper as priv_fundinfo
import fundinfo.fundinfo_professional_investor_scraper as prof_fundinfo
import sg_morningstar.sg_morningstar_scraper_sel as morningstar
import multiprocessing
import pandas as pd
import logging as log
file_path = r'D:\\sriram\\agrud\\NAV_scraping\\'
data_file = file_path+'MF List - Final.csv'
output_file = file_path+file_path.split('\\\\')[-2]+'_data.csv'
log_file_path = r'D:\\sriram\\agrud\\NAV_scraping\\scraper_run_log.txt'
log.basicConfig(filename = log_file_path,filemode='a',level=log.INFO)
my_log = log.getLogger()

def db_insert(df):
    import mysql.connector
    result = df[['master id','price','date']].values.tolist()
    try:
        db_conn = mysql.connector.connect(host='54.237.79.6',user='rentech_user',database = 'rentech_db',password='N)baegbgqeiheqfi3e9314jnEkekjb',auth_plugin='mysql_native_password')
        cursor = db_conn.cursor()
        sql = """INSERT INTO `raw_data_test` 
        (`id`, `master_id`, `indicator_id`, `value_data`, `json_data`, `data_type`, `ts_date`, `ts_hour`, `job_id`, `batch_id`, `timestamp`) 
        VALUES (NULL, %s, 371, %s, NULL, 2, %s, '0:0:0', 12, NULL, NOW()) ON DUPLICATE KEY UPDATE 
        master_id = VALUES(master_id), indicator_id = VALUES(indicator_id), value_data = VALUES(value_data), json_data = VALUES(json_data),
        data_type = VALUES(data_type), ts_date = VALUES(ts_date) ,ts_hour = VALUES(ts_hour), job_id = VALUES(job_id), batch_id = VALUES(batch_id);"""
        cursor.executemany(sql, result)
        rows = cursor.rowcount
        print(f'{rows} rows inserted')
        db_conn.commit()
    except Exception as e:
        print(f'Exception: {e}')
    finally:
        if (db_conn.is_connected()):
            cursor.close()
            db_conn.close()
            print('Connection closed')

def task1():
    # market_ft scraper
    market.output_file = output_file
    market.data_file = data_file
    market.log_file_path = log_file_path
    market.start_markets_ft_scraper()
    
    # fundsquare scraper
    fundsquare.output_file = output_file
    fundsquare.data_file = data_file
    fundsquare.log_file_path = log_file_path
    fundsquare.start_fundsquare_scraper()
    
    # priv_fundinfo scraper
    priv_fundinfo.output_file = output_file
    priv_fundinfo.data_file = data_file
    priv_fundinfo.log_file_path = log_file_path
    priv_fundinfo.start_fundinfo_priv_scraper(case=1)
    
    # prof_fundinfo scraper
    prof_fundinfo.output_file = output_file
    prof_fundinfo.data_file = data_file
    prof_fundinfo.log_file_path = log_file_path
    prof_fundinfo.start_fundinfo_prof_scraper(case=1)

def task2():
    # priv_fundinfo scraper
    priv_fundinfo.output_file = output_file
    priv_fundinfo.data_file = data_file
    priv_fundinfo.log_file_path = log_file_path
    priv_fundinfo.start_fundinfo_priv_scraper(case=2)
    
    # prof_fundinfo scraper
    prof_fundinfo.output_file = output_file
    prof_fundinfo.data_file = data_file
    prof_fundinfo.log_file_path = log_file_path
    prof_fundinfo.start_fundinfo_prof_scraper(case=2)
    
    # morningstar scraper
    morningstar.output_file = output_file
    morningstar.data_file = data_file
    morningstar.log_file_path = log_file_path
    morningstar.start_sg_morningstar_scraper()

if __name__ == '__main__':
    p1 = multiprocessing.Process(target=task1)
    p1.start()

    p2 = multiprocessing.Process(target=task2)
    p2.start()

    p1.join()
    p2.join()
    # db insertion
    df = pd.read_csv(output_file,encoding='utf-8')
    db_insert(df)
    
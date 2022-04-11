import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
import re
import mysql.connector
from datetime import datetime
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def get_driver():
    s=Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument("--incognito")
    # options.add_argument('--headless')
    # options.add_argument('--no-sandbox')
    # options.add_argument('--single-process')
    # options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=s,options=options)
    # driver.minimize_window()
    return driver

# def get_driver():
#     chrome_options = webdriver.ChromeOptions()
#     options = Options()
#     # options.add_argument("--kiosk")
#     # if sys.platform == "darwin":
#     #   options.binary_location = os.path.normpath(os.getcwd() + os.sep + os.pardir)+"/lambda-layers/lambda-layer-scraper-chromium-bin/bin_mac/chromium.app/Contents/MacOS/Chromium"
#     #   # options.binary_location = os.getcwd() + "../lambda-layer-scraper-chromium-bin/bin/chromium.app/Contents/MacOS/Chromium"
#     # else:
#     options.binary_location = '/opt/bin/headless-chromium'
#     options.add_argument('--headless')
#     options.add_argument('--no-sandbox')
#     options.add_argument('--single-process')
#     options.add_argument('--disable-dev-shm-usage')
#     options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36')
#     # driver = webdriver.Chrome('/opt/bin/chromedriver',chrome_options=options)
#     # options.setBinary(os.getcwd() + "/bin/headless-chromium")
    
#     # if sys.platform == "darwin":
#     #   # binary_location = os.getcwd() + "/bin/chromedriver"
#     #   binary_location = os.path.normpath(os.getcwd() + os.sep + os.pardir)+"/lambda-layers/lambda-layer-scraper-chromium-bin/bin_mac/chromedriver"
#     # else:
#     binary_location = "/opt/bin/chromedriver"
#     driver = webdriver.Chrome(binary_location, chrome_options=options)
#     return driver

def db_insert(result):
    try:
        db_conn = mysql.connector.connect(host='54.237.79.6',database='rentech_db',user='rentech_user',password='N)baegbgqeiheqfi3e9314jnEkekjb',auth_plugin='mysql_native_password')
        cursor = db_conn.cursor()
        sql = """INSERT INTO `raw_data` (`id`, `master_id`, `indicator_id`, `value_data`, `json_data`, `data_type`, `ts_date`, `ts_hour`, `job_id`, `timestamp`) VALUES 
        (NULL, %s, %s, %s, %s, %s, %s, '0:0:0', 2, NOW()) ON DUPLICATE KEY UPDATE 
        master_id = VALUES(master_id), indicator_id = VALUES(indicator_id), value_data = VALUES(value_data), json_data = VALUES(json_data),
        data_type = VALUES(data_type), ts_date = VALUES(ts_date) ,ts_hour = VALUES(ts_hour), job_id = VALUES(job_id), batch_id = VALUES(batch_id);"""
        cursor.executemany(sql, result)
        rows = cursor.rowcount
        db_conn.commit()
        print(f'{rows} rows inserted successfully')
    except Exception as e:
        print ("Mysql Error", e)
    finally:
        if (db_conn.is_connected()):
            cursor.close()
            db_conn.close()
            print("MySQL connection is closed")

def lambda_handler(event, context=''):
    main_map = {}
    for master_id, Symbol in zip(event['master_id'], event['source_id']):
        page_url = "https://www.bloomberg.com/quote/" + Symbol
        if master_id not in main_map:
            main_map[master_id] = {}

        # tor folder location
        driver = get_driver()
        driver.get(page_url)
        
        try:
            WebDriverWait(driver,3).until(EC.frame_to_be_available_and_switch_to_it(driver.find_element((By.XPATH,"//iframe[@id='sp_message_iframe_597169']"))))
        except:
            pass

        try:
            accept_btn = WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,'//button[@title="Yes, I Accept"]')))
            accept_btn.click()
        except Exception as e:
            pass
        try:
            driver.switch_to.default_content()
        except:
            pass
        
        driver.execute_script("window.stop();")
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            # Scroll down to bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            # Calculate new scroll height and compare with last scroll height
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        time.sleep(10)
        soup = BeautifulSoup(driver.page_source, 'html5lib')

        driver.quit()
        print('page_url:', page_url)

        print('coin:', Symbol)

        print('master id:', master_id)

        # date
        try:
            site_date = soup.find('div', {'data-testid': 'closed'}).find_next_sibling('div').text
        except Exception as e:
            try:
                site_date = soup.find('div', {'data-testid': 'open'}).find_next_sibling('div').text
            except Exception as e:
                print('Error while scraping symbol {} Error: {}'.format(Symbol,e))
                continue
        site_date = site_date.split(" ")
        site_date = site_date[-2].replace('/', '-')
        site_date = datetime.strptime(site_date, '%m-%d-%Y').strftime('%Y-%m-%d')
        print("site date:", site_date)
        

        TIME = "00:00:00"

        main_map[master_id]["time"] = TIME
        main_map[master_id]["date"] = site_date
        main_map[master_id]["indicators"] = {}

        # ytd
        ytd = soup.find_all('span', {'class': 'fieldValue__5149a11475'})[2].text
        ytd = ytd.replace('%', '').replace(',', '')
        ytd = float(ytd)
        if ytd == "--":
            pass
        else:
            if Symbol == "COINETH:SS" or Symbol == "COINXBT:SS" or Symbol == "BTCE:GR" or Symbol == "3072:HK" or Symbol == "COINXBE:SS" or Symbol == "CYB:SP":
                main_map[master_id]["indicators"]["348"] = {"value_data": ytd, "json_data": None}
                print('ytd:', ytd)

        # 1yr
        one_year = soup.find_all('span', {'class': 'fieldValue__5149a11475'})[3].text.replace('--', "0")
        one_year = one_year.replace('%', '').replace(',', '')
        if one_year == "--" or one_year == "0" or one_year == "0.0":
            pass
        else:
            if Symbol == "COINETH:SS" or Symbol == "COINXBT:SS" or Symbol == "BTCE:GR" or Symbol == "3072:HK" or Symbol == "COINXBE:SS" or Symbol == "CYB:SP":
                main_map[master_id]["indicators"]["7"] = {"value_data": one_year, "json_data": None}
                print('1year:', one_year)

        # 3yr
        three_year = soup.find_all('span', {'class': 'fieldValue__5149a11475'})[4].text.replace('--', "0")
        three_year = three_year.replace('%', '').replace(',', '')
        if three_year == "--" or three_year == "0" or three_year == "0.0":
            pass
        else:
            if Symbol == "COINETH:SS" or Symbol == "COINXBT:SS" or Symbol == "BTCE:GR" or Symbol == "3072:HK" or Symbol == "COINXBE:SS" or Symbol == "CYB:SP":
                main_map[master_id]["indicators"]["8"] = {"value_data": three_year, "json_data": None}
                print('3year:', three_year)

        # 5yr
        five_year = soup.find_all('span', {'class': 'fieldValue__5149a11475'})[5].text.replace('--', "0")
        five_year = five_year.replace('%', '').replace(',', '')
        if five_year == "--" or five_year == "0" or five_year == "0.0":
            pass
        else:
            if Symbol == "COINETH:SS" or Symbol == "COINXBT:SS" or Symbol == "BTCE:GR" or Symbol == "3072:HK" or Symbol == "COINXBE:SS" or Symbol == "CYB:SP":
                main_map[master_id]["indicators"]["9"] = {"value_data": five_year, "json_data": None}
                print('5year:', five_year)

        # total assets
        total = soup.find_all('span', {'class': 'fieldLabel__a284a93b14'})[7].text
        if re.findall("\([M]", total):
            total_assets = soup.find_all('span', {'class': 'fieldValue__5149a11475'})[7].text.replace(',', '')
            total_assets = float(total_assets)
            total_assets = total_assets*1000000

        if re.findall("\([B]", total):
            total_assets = soup.find_all('span', {'class': 'fieldValue__5149a11475'})[7].text.replace(',', '')
            total_assets = float(total_assets)
            total_assets = total_assets*100000000

        if re.findall("\([T]", total):
            total_assets = soup.find_all('span', {'class': 'fieldValue__5149a11475'})[7].text.replace(',', '')
            total_assets = float(total_assets)
            total_assets = total_assets*1000000000000
        
        if total_assets == "--":
            pass
        else:
            if Symbol == "COINETH:SS" or Symbol == "COINXBT:SS" or Symbol == "BTCE:GR" or Symbol == "3072:HK" or Symbol == "COINXBE:SS" or Symbol == "CYB:SP":
                main_map[master_id]["indicators"]["29"] = {"value_data": str(total_assets).split(".")[0], "json_data": None}
                print('total assets: ', total_assets)
        
        # expense ratio
        expense_ratio = soup.find_all('span', {'class': 'fieldValue__5149a11475'})[-1].text
        expense_ratio = expense_ratio.split('%')[0]
        expense_ratio = float(expense_ratio)
        expense_ratio = expense_ratio/100
        
        if expense_ratio == "--":
            pass
        else:
            main_map[master_id]["indicators"]["13"] = {"value_data": expense_ratio, "json_data": None}
            print('expense ratio',expense_ratio)
        resultset = []
        for ind in list(main_map[master_id]['indicators'].keys()):
            value = main_map[master_id]['indicators'][str(ind)]['value_data']
            json_data = None
            datatype = 0
            ts_date = site_date
            result = [master_id,ind,value,json_data,datatype,ts_date]
            resultset.append(result)
        # db_insert(resultset)

# all event
all_event = {
  "source_id": [
    "COINETH:SS",
    "COINXBT:SS",
    "BTCE:GR",
    "3072:HK",
    "COINXBE:SS",
    "CYB:SP"
  ],
  "master_id": [
    "84949",
    "84950",
    "86831",
    "86835",
    "86833",
    "86719"
  ]
}

# single test
# single_event = {
#     "source_id": [
#         "3072:HK"
#     ],
#   "master_id": [
#     86835
#     ]
# }

if __name__ == '__main__':
    lambda_handler(all_event)


from datetime import datetime,timedelta
import requests
import json
import time
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
import csv
filepath = r'D:\\sriram\\agrud\\daily_scrapers\\daily_run\\invoke_bloomberg\\'
# filepath = '/home/ubuntu/rentech/daily_scrapers/daily_run/invoke_bloomberg/'

def get_driver():
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    # options.add_argument("--incognito")
    options.add_argument('--headless')
    driver = webdriver.Chrome(ChromeDriverManager().install(),options=options)
    return driver

def get_creds():
    header = {
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
        }
    url = 'https://coordinator.cm.bloomberg.com/coordination?currentResource=Fund|COINETH:SS&metadata.paywall.device=Desktop|Windows_10|chrome&metadata.consent=true&metadata.paywall.referrer=Direct&limit=100'
    res = requests.get(url,headers=header)
    j_data = json.loads(res.text)
    agent_id = j_data['userCredentials']['agentId']
    session_id = j_data['userCredentials']['sessionId']
    session_key = j_data['userCredentials']['sessionKey']
    gatehouse_id = j_data['userCredentials']['gatehouseId']
    geo_info = j_data['geoIP']
    return agent_id,session_id,session_key,gatehouse_id,geo_info

def get_header():
    agent_id,session_id,session_key,gatehouse_id,geo_info = get_creds()
    exp_tm_stmp = int(datetime.timestamp(datetime.now() + timedelta(days=7)))
    curr_tm_stmp = int(datetime.timestamp(datetime.now()))
    refresh_datetime = datetime.now().date().strftime('%Y-%m-%d %H:%M')
    cookie_p1 = f'__sppvid=1ac7095f-de90-4814-81d0-4bacfb6f4761; _liChk=0.8809333228506295; gatehouse_id={gatehouse_id}; _pxvid=eb4b21a4-67d8-11ec-be46-5376474c734f; agent_id={agent_id}; session_id={session_id}; session_key={session_key}; ccpaUUID=0a28b86d-d96a-4c8f-9f98-a7773dc769b7; dnsDisplayed=true; signedLspa=false; ccpaApplies=true; _sp_krux=false; consentUUID=fe94a3a5-2626-4e7e-b5f9-c64ed5d263bb; bbgconsentstring=req1fun1pad1; bdfpc=004.2102205711.1640694190922; _gcl_au=1.1.859808239.1640694191; _ga=GA1.2.1725334073.1640694191; _fbp=fb.1.1640694191461.1972474150; _scid=bdfdf855-7380-408c-a70d-d71983e60691; _rdt_uuid=1640694191728.601c446f-6436-478a-b14b-f9a8216876c9; trc_cookie_storage=taboola%20global%3Auser-id=84e2b71c-e286-4a1a-8ae4-73975b7325ef-tuct8a76750; _lc2_fpi=b1166d620485--01fr0fx6tbbhk7htf35cd1e544; __gads=ID=97836a5a9a428957:T=1640694192:S=ALNI_Ma11SJSPUtnIvhQCCVjpEE5tMTHUA; _cc_id=6800f657f1b08471fe908107462ac209; com.bloomberg.player.volume.level=1; optimizelyEndUserId=oeu1642420433528r0.30425511745910594; _gcl_aw=GCL.1642420434.CjwKCAiAxJSPBhAoEiwAeO_fPxTJn6K9ug_hCwOX1DPjClGELXu3rfEZqrLxgYSkGtKl_QhEhp7QnRoCdRYQAvD_BwE; _gcl_dc=GCL.1642420434.CjwKCAiAxJSPBhAoEiwAeO_fPxTJn6K9ug_hCwOX1DPjClGELXu3rfEZqrLxgYSkGtKl_QhEhp7QnRoCdRYQAvD_BwE; _gac_UA-11413116-1=1.1642420434.CjwKCAiAxJSPBhAoEiwAeO_fPxTJn6K9ug_hCwOX1DPjClGELXu3rfEZqrLxgYSkGtKl_QhEhp7QnRoCdRYQAvD_BwE; _sp_v1_uid=1:482:17d811b0-99fa-4711-a250-944369f36dec; _sp_v1_ss=1:H4sIAAAAAAAAAItWqo5RKimOUbLKK83J0YlRSkVil4AlqmtrlXRGlaEqiwUAXwon3zkBAAA=; _sp_v1_opt=1:; _sp_v1_csv=null; _sp_v1_lt=1:; exp_pref=AMER; pxcts=41871a5f-a8f3-11ec-a5f4-65757a77595a; '
    cookie_p2 = f'geo_info={geo_info}|'
    cookie_p3 = f'1648457127311; _reg-csrf=s:rIRddJT8cc_IeM4Ah3pnjnRZ.FpedlOXCQK8mZaSibKJK3FmLol5YxSqmwqThX1Q1s5Q; _user-status=anonymous; _gid=GA1.2.683593150.1647852328; _li_dcdm_c=.bloomberg.com; panoramaId=3027d6ce81b7b98dfac827847f1c16d53938fb71c02aa7a5b8b8a694626f1083; _sctr=1|{curr_tm_stmp}; '
    cookie_p4 = f'geo_info={geo_info}|{exp_tm_stmp}; '
    parsely_visitor = {"id":"pid=de26adca958166b3aedd1964ebee7798","session_count":5,"last_session_ts":{curr_tm_stmp}}
    parsely_slot_click = {"url":"https://www.bloomberg.com/crypto","x":112,"y":172,"xpath":"//*[@id=\"navi-search-companies-1647861488313-0\"]","href":"https://www.bloomberg.com/quote/COINETH:SS"}
    cookie_p5 = f'_parsely_visitor={parsely_visitor}; __sppvid=e79ae517-cc3a-4535-9e54-3c5d6a8fdca0; _parsely_slot_click={parsely_slot_click}; com.bloomberg.player.volume.muted=true; _sp_v1_data=2:439307:1645418751:0:70:0:70:0:0:_:-1; _dc_gtm_UA-11413116-1=1; ',
    cookie_p6 = f'_last-refresh={refresh_datetime}; _uetsid=4208b5e0a8f311ec896861372c61c84c; _uetvid=ec60b2d067d811ec88d99d40ad2d9c29; _reg-csrf-token=BkcSvyUp-Xz5apFUVzAUj5oByobmXrU93778; _px2=eyJ1IjoiNTEwMTFiZjAtYTkxNC0xMWVjLThhNTAtOTU5NDE3NmVlM2ZjIiwidiI6ImViNGIyMWE0LTY3ZDgtMTFlYy1iZTQ2LTUzNzY0NzRjNzM0ZiIsInQiOjE2NDc4NjY4Mjg2MjMsImgiOiI5MmQyOWQ1NGIyZDFlYWM3Y2M5NDg1ZjM0YzUzNWFjMTgwYzMzOTI0OWJkNGNmYjFjNDg0NjkwMTNiYWQ3NzQxIn0=; _px3=52803cb727c44258d06b6a63d796d6802783699a975e33ba9b16bc3c89984d61:u8wTpMGyDXsbtGtzGiHODPwlcpayPQStyfxKpFmdQxLyd0+HdgxAW6Dg/+VXALMdOTqKG0e086ONceNTuHwtDg==:1000:5aiSIk2Q5qc+sm7zf1HL4KSvAmkvG50QxGqF07JJ63EUOFlppkGsFMgpqmh6fqs2Y6++D8iPILJkDW2k8b/DiLSahi+NaiByYuMQV2SgxCUHm20DCuzJ1jQexZHZjKwpz1H+iekJetaK6QVpt6djI8FtKyIkMZRo2RZubCTsFXopjOis6GfY2xCaq+y5X6a8rB2pn3Mw4R345v9aMUf7xw==; lotame_domain_check=bloomberg.com; '
    cookie_p7 = f'panoramaId_expiry={exp_tm_stmp}; _gat_UA-11413116-1=1; _pxde=62e57d981e149e941709644c77d4ba24f9729b86db935a1aa764d2d7e992a82c:eyJ0aW1lc3RhbXAiOjE2NDc4NjY1MzUyNDMsImZfa2IiOjAsImlwY19pZCI6W119'
    header = {
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'cookie': cookie_p1+cookie_p2+cookie_p3+cookie_p4+cookie_p5[0]+cookie_p6+cookie_p7,
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
    }
    return header


def get_cookie_from_driver():
    url = 'https://www.bloomberg.com/quote/COINETH:SS'
    driver = get_driver()
    driver.get(url)
    cookies_dict = driver.get_cookies()
    cookies_json = {}
    for cookie in cookies_dict:
        cookies_json[cookie['name']] = cookie['value']
    cookies_string = str(cookies_json).replace("{", "").replace("}", "").replace("'", "").replace(": ", "=").replace(",", ";")
    driver.quit()
    return cookies_string


from pandas.errors import EmptyDataError as emptydf
import requests
import json
import hashlib
import datetime
import csv
import pandas as pd
from bs4 import BeautifulSoup
session = requests.session()
next_token_file = r'D:\\sriram\\agrud\\news\\ap_news\\next_url_list.csv'
data_file = r'D:\\sriram\\agrud\\news\\ap_news\\news_data.csv'
days = '50'
prod_size = '100'
prod_id = '46841'
header = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'upgrade-insecure-requests': '1',
    'x-apikey':'1boei5u6gm6qh4cw7k5xkqx71e',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36'
    }

def write_header(): 
    with open(data_file, mode='a', encoding='utf-8',newline="") as output_file:
        writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(["url","url_hash","headline","body","publish_date", "news_type","job_id", "master_id"])

def write_data(data):
    with open(data_file, mode='a', encoding='utf-8',newline="") as output_file:
        writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(data)

def next_url(data):
    with open(next_token_file, mode='a', encoding='utf-8',newline="") as output_file:
        writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(data)

def body_filter(bodySoup):
    bodyLines = []
    for child in bodySoup.find_all():
        try:
            childText = child.text.replace('\xa0',' ').strip()
        except:
            childText = child
        if not (childText or childText.strip()):
            continue
        elif 'view the full releases here' in childText.lower() or 'more information is available at' in childText.lower() or 'learn more at' in childText.lower():
            continue
        elif 'for further information' in childText.lower() or 'contact' in childText.lower() or 'this press release features multimedia' in childText.lower() or 'attachment' in childText.lower():
            continue
        else:
            bodyLine = childText.strip()
        bodyLines.append(bodyLine.strip())

    custom_body = "\n".join(bodyLines).strip()
    return custom_body

def get_news_by_days(res_url):
    write_header()
    print(f'page url: {res_url}')
    res = session.get(res_url,headers=header)
    j_data = json.loads(res.text)
    next_page = j_data['data']['next_page']
    items = j_data['data']['items']
    if len(items) == 0:
        next_api_url = next_page+'&page_size=100&include=*'
        next_url([next_api_url])
        return 0
    for item in items:
        try:
            url = item['item']['links'][0]['href']
            # print(f'news url: {url}')
        except KeyError:
            continue
        url_hash = hashlib.md5(url.encode()).hexdigest()
        headline = item['item']['headline']
        publish_date = datetime.datetime.strptime(item['item']['firstcreated'],'%Y-%m-%dT%H:%M:%SZ')
        news_type = 21
        job_id = 3
        master_id =''
        content_id = item['item']['renditions']['nitf']['contentid']
        content_api_url = item['item']['renditions']['nitf']['href']
        res2 = session.get(content_api_url,headers=header)
        body = BeautifulSoup(res2.text,'html5lib').find('body.content').find('block')
        body = body_filter(body)
        data = [url,url_hash,headline,body,publish_date,news_type,job_id,master_id]
        write_data(data)
    next_api_url = next_page+'&page_size=100&include=*'
    next_url([next_api_url])
    get_news_by_days(next_api_url)

def start():
    try:
        df = pd.read_csv(next_token_file, encoding='utf-8')
        val1 = df.iloc[-1,0]
        url = val1
    except FileNotFoundError:
        url = f'https://api.ap.org/media/v2.1/content/feed?q=productid:{prod_id}+AND+mindate:%3Enow-{days}d&page_size={prod_size}&include=*'
    except emptydf:
        print('There are no url in next url file please check')
        exit()
    flag = get_news_by_days(res_url = url)
    if flag == None:
        print('your feeds are upto date')
    else:
        print('Error please check')

start()
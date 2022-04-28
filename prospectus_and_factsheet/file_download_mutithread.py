from base64 import encode
from datetime import datetime
import requests
import pandas as pd
from datetime import datetime
import os
import base64
import threading
import time

try:
    dir = os.getcwd()+'\\factsheet'
    os.mkdir(dir)
except:
    pass
try:
    dir = os.getcwd()+'\\prospectus'
    os.mkdir(dir)
except:
    pass
for file in os.listdir():
    if os.path.isfile(file):
        if 'data_links' in file and '.csv' in file:
            in_file_path = os.getcwd()+'\\'+file
    if os.path.isdir(file):
        if 'factsheet' in file:
            fact_sheet_file_path = os.getcwd()+'\\'+file+'\\'
        if 'prospectus' in file:
            prospectus_file_path = os.getcwd()+'\\'+file+'\\'
date = datetime.today()

header = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36',
    }

def file_download(file_path,link,master_id,i):  
    if os.path.exists(file_path):
        print(str(i)+' '+str(master_id)+' file already exists')
        return 0
    res = requests.get(link,verify=False,stream=True,headers=header)
    with open(file_path, mode='wb') as f:
        f.write(res.content)
    # with open(file_path, "wb") as pdf_file:
    #     base64.b64encode(pdf_file.write(res.content))
    if '\\factsheet\\' in file_path:
        print(f"------------{master_id} factsheet downloaded----------------")
    else:
        print(f"------------{master_id} prospectus downloaded----------------")
    
def get_files():
    df = pd.read_csv(in_file_path)
    for i,row in df.iterrows():
        master_id = row[0]
        date2 = date.strftime('%Y%m%d')
        file_name = f'{master_id}_{date2}'
        
        # fact link pdf download
        try:
            link = row[2]
            file_path = fact_sheet_file_path+file_name+'.pdf'
            # file_download(file_path,link,master_id,i)
            download_thread = threading.Thread(target=file_download, args=(file_path,link,master_id,i))
            download_thread.start()
        except Exception as e:
            pass

        # # pros link pdf download
        # try:
        #     link = row[3]
        #     file_path = prospectus_file_path+file_name+'.pdf'
        #     download_thread = threading.Thread(target=file_download, args=(file_path,link,master_id,i))
        #     download_thread.start()
        # except:
        #     pass
        time.sleep(0.5)

if __name__ == '__main__':
    get_files()
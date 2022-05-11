import fitz
import re
import csv
import time
import pandas as pd
file_path = r'D:\\sriram\\agrud\\prospectus_and_factsheet\\'
data_file = file_path+'Global_MF_Factsheet_Prospectus - FINAL GLOBAL MF LIST.csv'
pdf_files = r'D:\\sriram\\agrud\\prospectus_and_factsheet\\factsheet\\'


def approach5(file,isin):
    flag = 0
    # load document
    try:
        doc = fitz.open(file)
    except Exception as e:
        print(e)
        return 0

    # get text, search for string and print count on page.
    for page in doc:
        text = ''
        text += page.get_text()
        if len(re.findall(isin, text)) == 1:
            flag = 1
            break
        elif isin in text:
            flag = 1
            break
    file = file.split('\\')[-1].replace('.pdf','')
    if flag == 1:
        res = f'file: {file} isin:{isin} found on page: {page.number+1}'
    else:
        res = f'file: {file} isin {isin} not found'
    with open('data.csv', mode='a', encoding='utf-8',newline="") as output_file:
        writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow([res])

def start_check():
    date2 = '20220428'
    data_df = pd.read_csv(data_file,encoding="utf-8")[23:]
    data_df = data_df.drop_duplicates(subset=['master_id'])
    for i,row in data_df.iterrows():
        isin = row[3]
        master_id = row[0]
        file_name = f'{master_id}_{date2}'
        file = pdf_files+file_name+'.pdf'
        approach5(file,isin)
        time.sleep(1)
        
    
start_check()
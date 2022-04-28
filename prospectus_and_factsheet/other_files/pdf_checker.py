import string
import pandas as pd
import os
import re
import PyPDF4
import base64
from datetime import datetime, timedelta
import textract
import sys
from io import StringIO
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser
sys.path.append('../..')
file_path = r'D:\\sriram\\agrud\\prospectus_and_factsheet\\'
data_file = file_path+'Global _MF_Factsheet_Prospectus - FINAL GLOBAL MF LIST.csv'
output_file = file_path+'scraped_data_links.csv'
pdf_files = r'D:\\sriram\\agrud\\prospectus_and_factsheet\\factsheet\\'


def approach2(file,isin):
    f_name = file.split('\\')[-1]
    master_id = f_name.split('_')[0]
    encoder = 'latin-1'        
    pdf_obj = PyPDF4.PdfFileReader(file,'rb')
    NumPages = pdf_obj.getNumPages()
    for i in range(0, NumPages):
        PageObj = pdf_obj.getPage(i)
        try:
            pdf_text = PageObj.extractText() 
            if re.search(str(isin),pdf_text):
                # print(f"Isin {isin} Found in pdf {f_name.replace('.pdf','')}")
                return 1
            else:
                print(f"Isin {isin} Not Found in pdf {f_name.replace('.pdf','')}")
                return 0
        except:
            pass

def approach1(file,isin):
    f_name = file.split('\\')[-1]
    master_id = f_name.split('_')[0]  
    string = isin
    output_string = StringIO()
    rsrcmgr = PDFResourceManager(caching=True)
    text_converter = TextConverter(rsrcmgr, output_string, laparams=LAParams())
    interpreter = PDFPageInterpreter(rsrcmgr, text_converter)
    with open(file, 'rb') as in_file:
        for page in PDFPage.get_pages(in_file):
            try:
                interpreter.process_page(page)
            except Exception as e:
                pass
    pdf_text = output_string.getvalue()
    if string in pdf_text:
        # print(f"Isin {isin} Found in pdf {f_name.replace('.pdf','')}")
        return 1
    else:
        # print(f"Isin {isin} Not Found in pdf {f_name.replace('.pdf','')}")
        return 0


def start_check():
    date = datetime.today()-timedelta(days=1)
    date2 = date.strftime('%Y%m%d')
    new_df = pd.DataFrame()
    out_df = pd.read_csv(output_file)
    data_df = pd.read_csv(data_file,encoding="utf-8")
    data_df = data_df.drop_duplicates(subset=['master_id'])
    for i,row in data_df.iterrows():
        isin = row[3]
        master_id = row[0]
        file_name = f'{master_id}_{date2}'
        file = pdf_files+file_name+'.pdf'
        try:
            flag = approach1(file,isin)
            if flag == 0:
                approach2(file,isin)
            if flag == 1:
                try:
                    temp_df = out_df[out_df['master id'].isin([master_id])]
                    new_df = new_df.append(temp_df,ignore_index=True)
                except TypeError:
                    pass
        except FileNotFoundError:
            print(f'file {file_name} not found')
            continue
    print('---------')
    new_df.to_csv(output_file,index=False,header=['master id','isin name','factsheet link','prospectus link'])
    
start_check()
import pandas as pd
import os
from datetime import datetime, timedelta
file_path = r'D:\\sriram\\agrud\\prospectus_and_factsheet\\fundinfo\\'
data_file = file_path+'Global _MF_Factsheet_Prospectus - FINAL GLOBAL MF LIST.csv'
output_file = file_path+'scraped_data_links.csv'
pdf_files = 'D:\\sriram\\agrud\\prospectus_and_factsheet\\fundinfo\\factsheet_part1\\'

def pdf_check(file,isin):
    f = open(file, "r",encoding='latin-1')
    a = f.read()
    f_name = file.split('\\')[-1]
    master_id = f_name.split('_')[0]
    if str(isin) in a: 
        print(f"Isin {isin} Found in pdf {f_name}")
        f.close()
        return master_id
    else: 
        print(f"Isin {isin} Not Found in pdf {f_name}")
        print(f'{f_name} is deleted')
        f.close()
        os.remove(file)

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
            master_id = pdf_check(file,isin)
            try:
                temp_df = out_df[out_df['master id'].isin([eval(master_id)])]
                new_df = new_df.append(temp_df,ignore_index=True)
            except TypeError:
                pass
        except FileNotFoundError:
            print(f'file {file_name} not found')
            continue
    new_df.to_csv(output_file,index=False)
    
start_check()
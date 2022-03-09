from morningstar_scraper import morningstar_gen_case
from fundinfo_private_investor_scraper import priv_investor_gen_case
from fundinfo_professional_investor_scraper import prof_investor_gen_case
from fundsingapore_scraper import fundsingapore_gen_case
from moneycontroller_scraper import moneycontroller_gen_case
import pandas as pd
import numpy as np
import csv
import os
domain = os.getcwd().split('\\')[-1].replace(' ','_')

def write_header():
    with open(f"{domain}_data_links.csv","a",newline="") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerow(['master id','isin name','factsheet link','prospectus link'])

def csv_filter():
    filtered_df = pd.DataFrame()
    unique_isin = []
    cols = ["master id","isin name","factsheet link","prospectus link"]
    try:
        df = pd.read_csv(f"{domain}_data_links.csv")
    except FileNotFoundError:
        write_header()
        return 0
    for isin,grouped_df in df.groupby(by=['isin name']):
        for i,row in grouped_df.iterrows():
            if row[2] is not np.nan and row[3] is not np.nan:
                if isin not in unique_isin:
                    unique_isin.append(isin)
                    filtered_df = filtered_df.append(pd.DataFrame([row],columns=cols),ignore_index=True)
    try:
        filtered_df.to_csv(f"{domain}_data_links.csv",columns=cols,index=False)
    except:
        pass

def isin_downloaded():
    isin_downloaded = []
    with open(f"{domain}_data_links.csv","r") as file:
        csvreader = csv.reader(file)
        header = next(csvreader)
        for row in csvreader:
            isin_downloaded.append(row[1])
    return isin_downloaded

def get_data():
    case = 0
    csv_filter()
    sites = ['morningstar','fundinfo','moneycontroller','fundsingapore']
    for site in sites:
        if site == 'morningstar':
            case = 1
        elif site == 'fundinfo':
            case = 2
        elif site == 'fundsingapore':
            case = 3            
        elif site == 'moneycontroller':
            case = 4
        df = pd.read_csv(data_file,encoding="utf-8")
        df = df.drop_duplicates(subset=['master_id'])
        if case == 1:
            downloaded_isin = isin_downloaded()
            start = 0
            for i,row in df.iterrows():
                isin = row[7]
                master_id = row[0]
                if isin not in downloaded_isin:
                    if start == 0:
                        driver = morningstar_gen_case(isin,master_id)
                        start = 1
                    else:
                        morningstar_gen_case(isin,master_id,driver)
        if case == 2:
            downloaded_isin = isin_downloaded()
            for i,row in df.iterrows():
                isin = row[7]
                master_id = row[0]
                if isin not in downloaded_isin:
                    priv_investor_gen_case(isin,master_id)
            csv_filter()
            downloaded_isin = isin_downloaded()
            for i,row in df.iterrows():
                isin = row[7]
                master_id = row[0]
                if isin not in downloaded_isin:
                    prof_investor_gen_case(isin,master_id)
        if case == 3:
            downloaded_isin = isin_downloaded()
            for i,row in df.iterrows():
                isin = row[7]
                master_id = row[0]
                if 'SG' in isin:
                    if isin not in downloaded_isin:
                        fundsingapore_gen_case(isin,master_id)
        if case == 4:
            downloaded_isin = isin_downloaded()
            for i,row in df.iterrows():
                isin = row[7]
                master_id = row[0]
                if isin not in downloaded_isin:
                    moneycontroller_gen_case(isin,master_id)
        csv_filter()

if __name__ == '__main__': 
    for file in os.listdir():
        if 'Factsheet_Prospectus' in file and '.csv' in file:
            data_file = os.getcwd()+'\\'+file
            break
    get_data()

# sites = ['morningstar','fundinfo','moneycontroller','fundsingapore']
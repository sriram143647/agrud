from datetime import datetime
import fundinfo.fundinfo_private_investor_scraper as priv_fundinfo
import fundinfo.fundinfo_professional_investor_scraper as prof_fundinfo
import morningstar.morningstar_scraper as morningstar
import moneycontroller.moneycontroller_scraper as moneycontroller
import fundsingapore.fundsingapore_scraper as fundsingapore
file_path = r'D:\\sriram\\agrud\\prospectus_and_factsheet\\'
data_file = file_path+'Global _MF_Factsheet_Prospectus - FINAL GLOBAL MF LIST.csv'
output_file = file_path+'scraped_data_links.csv'

def start_scrapers():
    priv_fundinfo.data_file = data_file
    priv_fundinfo.output_file = output_file
    priv_fundinfo.start_fundinfo_priv_scraper()

    morningstar.data_file = data_file
    morningstar.output_file = output_file
    morningstar.start_morningstar_scraper()

    prof_fundinfo.data_file = data_file
    prof_fundinfo.output_file = output_file
    prof_fundinfo.start_fundinfo_prof_scraper()

    moneycontroller.data_file = data_file
    moneycontroller.output_file = output_file
    moneycontroller.start_moneycontroller_scraper()

    fundsingapore.data_file = data_file
    fundsingapore.output_file = output_file
    fundsingapore.start_fundsingapore_scraper()

if __name__ == '__main__':
    start_scrapers()
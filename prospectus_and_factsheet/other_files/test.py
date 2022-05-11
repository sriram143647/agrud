from io import StringIO
from pdfminer3.converter import TextConverter
from pdfminer3.layout import LAParams
from pdfminer3.pdfdocument import PDFDocument
from pdfminer3.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer3.pdfpage import PDFPage
from pdfminer3.pdfparser import PDFParser
import pdfplumber
import PyPDF4
import string
import re
import scraperwiki
import urllib.request as req
from pdfminer import high_level
import pandas as pd
import requests
import fitz
from bs4 import BeautifulSoup
session = requests.session() 
file = r'D:\\sriram\\agrud\\prospectus_and_factsheet\\factsheet\\137983_20220428.pdf'
f_name = file.split('\\')[-1]
isin = 'LU1548497426'
file_path = r'D:\\sriram\\agrud\\prospectus_and_factsheet\\'
output_file = file_path+'scraped_data_links.csv'

def approach5():
    # load document
    doc = fitz.open(file)

    # get text, search for string and print count on page.
    for page in doc:
        text = ''
        text += page.get_text()
        if len(re.findall(isin, text)) == 1:
            print(f'isin {isin} found on page{page.number+1}')
            break

approach5()

exit()
# approach4
def parse_HTML_tree(url):
    #returns a navigatibale tree, which you can iterate through
    pageContent = req.urlopen(url).read()
    pdfToObject = scraperwiki.pdftoxml(pageContent.decode('utf-8'))
    soup = BeautifulSoup(pdfToObject)
    return soup

def approach4():
    out_df = pd.read_csv(output_file)
    for i,row in out_df.iterrows():
        master_id = row[0]
        isin = row[1]
        factsheet_pdf_url = row[2]    
        pdf_url = factsheet_pdf_url+'.pdf'
        pdfToSoup = parse_HTML_tree(pdf_url)
        soupToArray = pdfToSoup.findAll('text')
        for line in soupToArray:
            print(line)
            
# approach1
def  approach1():
    string = isin
    output_string = StringIO()
    rsrcmgr = PDFResourceManager(caching=True)
    pdf_laparams = LAParams(char_margin=1.0, word_margin=0.1, detect_vertical=True)
    text_converter = TextConverter(rsrcmgr, output_string, laparams=pdf_laparams)
    interpreter = PDFPageInterpreter(rsrcmgr, text_converter)
    with open(file, 'rb') as in_file:
        parser = PDFParser(in_file)
        document = PDFDocument(parser, password="")
        for page in PDFPage.create_pages(document):
            try:
                interpreter.process_page(page)
            except Exception as e:
                print(e)
    pdf_text = output_string.getvalue()
    if string in pdf_text: 
        print(f"Isin {isin} Found in pdf {f_name.replace('.pdf','')}")
        return 1
    elif pdf_text == '':
        print(f"unable to fetech text from pdf {f_name.replace('.pdf','')}")
        return 0
    else:
        print(f"Isin {isin} Not Found in pdf {f_name.replace('.pdf','')}")
        return 0

# approach2
def approach2():
    flag = 0
    pdf_obj = PyPDF4.PdfFileReader(file,'rb')
    NumPages = pdf_obj.getNumPages()
    for i in range(0, NumPages):
        PageObj = pdf_obj.getPage(i)
        try:
            text = PageObj.extractText() 
            if re.search(str(isin),text):
                flag = 1
            else:
                flag = 0
        except:
            pass
    if flag == 1:
        print(f"Isin {isin} Found in pdf {f_name.replace('.pdf','')}")
        return 1
    else:
        print(f"Isin {isin} not Found in pdf {f_name.replace('.pdf','')}")
        return 0
 
# approach3
def approach3():
    string = isin
    with pdfplumber.open(file) as pdf_file:
        pages = pdf_file.pages
        for i in range(len(pages)):
            page = pages[i]
            pdf_text = page.extract_text()
    if string in pdf_text: 
        print(f"Isin {isin} Found in pdf {f_name.replace('.pdf','')}")
        return 1
    elif pdf_text == '':
        print(f"unable to fetech text from pdf {f_name.replace('.pdf','')}")
        return 0
    else:
        print(f"Isin {isin} Not Found in pdf {f_name.replace('.pdf','')}")
        return 0
    

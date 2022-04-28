from io import StringIO
from types import new_class
from pdfminer3.converter import TextConverter
from pdfminer3.layout import LAParams
from pdfminer3.pdfdocument import PDFDocument
from pdfminer3.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer3.pdfpage import PDFPage
from pdfminer3.pdfparser import PDFParser
import pdfplumber
import PyPDF4
import camelot
import os
import re
file = r'D:\\sriram\\agrud\\prospectus_and_factsheet\\factsheet\\137986_20220426.pdf'
f_name = file.split('\\')[-1]
isin = 'DE0008475021'

def  approach1():
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
        print(f"Isin {isin} Found in pdf {f_name.replace('.pdf','')}")
        return 1
    elif pdf_text == '':
        print(f"unable to fetech text from pdf {f_name.replace('.pdf','')}")
        return 0
    else:
        print(f"Isin {isin} Not Found in pdf {f_name.replace('.pdf','')}")
        return 0

def approach2():
    pdf_obj = PyPDF4.PdfFileReader(file,'rb')
    NumPages = pdf_obj.getNumPages()
    for i in range(0, NumPages):
        PageObj = pdf_obj.getPage(i)
        try:
            text = PageObj.extractText() 
            if re.search(str(isin),text):
                print(f"Isin {isin} Found in pdf {f_name.replace('.pdf','')}")
                return 1
            else:
                print(f"Isin {isin} not Found in pdf {f_name.replace('.pdf','')}")
                return 0
        except:
            pass

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

def new_approach():
    tables = camelot.read_pdf(file)
    tables

approach1()
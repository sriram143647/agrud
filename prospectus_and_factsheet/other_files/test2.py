from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import pyautogui

def get_driver():
    s=Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    # options.add_argument('--headless')
    driver = webdriver.Chrome(service=s,options=options)
    return driver

def new_approach():
    pdf_url = 'https://doc.morningstar.com/Document/7cecaac993230dae7aaf56adb8b71922.msdoc?key=518310188c1ffc58da86dca01083a8740b02deb2148b2e80c33f169d78a84004'
    driver = get_driver()
    driver.get(pdf_url)
    time.sleep(10)
    pyautogui.hotkey('ctrl', 'f')
    pyautogui.write(isin)
    time.sleep(5)
    try:
        d = pyautogui.locateAllOnScreen(isin)
        print('isin found')
    except Exception as e:
        print('isin not found')
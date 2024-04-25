import requests
import json
import pandas as pd
from typing import Dict
import datetime
import sys
import time 

from seleniumwire import webdriver
from seleniumwire.utils import decode
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from seleniumwire.webdriver import ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC

from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from twocaptcha import TwoCaptcha

def get_contract_bank(start_date, end_date):
    print("Authenticating site")

    url_main = 'https://site1.com'
    url_extraction = "https://site2.com"

    # Configuracao serviço do WebDriver
    s = Service('/tmp/chrome/latest/chromedriver_linux64/chromedriver')    
    chrome_options = webdriver.ChromeOptions()
    chrome_options.binary_location = '/tmp/chrome/latest/chrome-linux/chrome'
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--remote-debugging-port=9222')

    # Criação do driver do Chrome
    driver = webdriver.Chrome(service=s, options=chrome_options) 
    driver.get(url_main)

    print('Begging process...')
    try:
        #wait for the page to load
        wait = WebDriverWait(driver, 20)

        wait.until(lambda driver: driver.find_element(By.ID, "usuario"))

        print("Authenticanting StormFin -> Typing Email...")
        driver.find_element(By.ID, "usuario").send_keys("user")

        print("Authentication StromFin -> Typing Password...")
        driver.find_element(By.ID, "senha").send_keys("pass")


        is_captcha_ok = False
        for i in range(1, 4):
            if is_captcha_ok == False:
                print(f"Solving Captcha {i}/3")
                solver = TwoCaptcha("x")
                try:
                    result = solver.recaptcha(
                        sitekey = 'y',
                        url = url_main,
                        enterprise=1
                    )        
                    is_captcha_ok = True
                    print('Captha Solved!')
                except Exception as e:
                    is_captcha_ok = False
                    print(f"Captcha Error: {str(e)}")
                    print('Waiting 5 seconds to retry captha...')
                    time.sleep(5)

        driver.execute_script(f"""document.getElementById("g-recaptcha-response").value="{result['code']}";""")

        print("Autheticating StormFin -> Clicking Login Botton...")
        login_form = driver.find_element(By.ID, "form_login")
        login_button = login_form.find_element(By.CLASS_NAME, "btn")
        login_button.click()

        print("Login OK. Accessing extraction site...")
        time.sleep(5)
        driver.get(url_extraction)    
        time.sleep(5)

        # wait.until(EC.visibility_of_element_located((By.NAME, "cliente")))
        print('Inserting start date...')
        time.sleep(5)
        campo_data = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//input[@name='cliente']"))
        )
        driver.execute_script("arguments[0].setAttribute('value', arguments[1])", campo_data, start_date)

        print('Inserting end date...')
        campo_data_end = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//input[@name='clienteEnd']"))
        )
        driver.execute_script("arguments[0].setAttribute('value', arguments[1])", campo_data_end, end_date)
        print('Dates are ready...')
        time.sleep(3)

        print('Pressing report download button...')
        create_report_button = driver.find_element(By.CSS_SELECTOR, "input[value='CRIAR RELATÓRIO']")
        create_report_button.click()
        time.sleep(3) 
        print('Download done!')
    
    except Exception as e:
        raise(e)
        # driver.quit()
    finally:
        driver.quit()
        print('ending script...')

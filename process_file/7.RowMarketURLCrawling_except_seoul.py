import csv ,datetime, sys, os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
import time
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
current_date = datetime.datetime.now().strftime('%Y-%m-%d')

# 옵션 생성, 창 숨기는 옵션 추가
options = webdriver.ChromeOptions()
options.add_argument('--no-sandbox')        
options.add_argument('--headless')       
options.add_argument('--disable-dev-shm-usage')
options.add_argument("--disable-setuid-sandbox") 
options.add_argument('--disable-gpu')
options.add_argument("--window-size=1920,1080")
options.add_argument('--ignore-certificate-errors')
options.add_argument('--allow-running-insecure-content')

mart_url_list = {}
mart_list = []
#링크 및 마트 정보 크롤링

No_url_mart_num = 1
service = ChromeService(executable_path = "/usr/bin/chromedriver")
driver = webdriver.Chrome(service=service, options=options)

for page in range(1,21):
    url = 'https://dnmart.co.kr/web/web_mart_select.html?page='+ str(page)
    driver.get(url)
    time.sleep(1)

    table = driver.find_element(By.CLASS_NAME, 'tbl_list_ro')
    tbody = table.find_element(By.TAG_NAME, 'tbody')
    tr = tbody.find_elements(By.TAG_NAME, 'tr')

    for i in range(len(tr)):
        table = driver.find_element(By.CLASS_NAME, 'tbl_list_ro')
        tbody = table.find_element(By.TAG_NAME, 'tbody')
        tr = tbody.find_elements(By.TAG_NAME, 'tr')
        td = tr[i].find_elements(By.TAG_NAME, 'td')
        if not '(오픈준비중)' in td[0].text and '서울' not in td[1].text:
            mart_name = td[0].text
            mart_address = td[1].text
            mart_call = td[2].text

            try:
                mart_url_element = td[-1].find_element(By.TAG_NAME, 'span')
                mart_url_element = mart_url_element.find_element(By.TAG_NAME, 'a')
                mart_url = mart_url_element.get_attribute('href')
                mart_num = mart_url.split('shop_id=')
                mart_num = mart_num[1]
                mart_num = mart_num.replace('A','')

                if mart_num == 'plusmart':
                    mart_num = 8
                            
                # print(mart_url)
                mart_url_list[int(mart_num)]=mart_url

            except:
                mart_url_list[No_url_mart_num]=mart_url_element.text
                mart_num = No_url_mart_num
                No_url_mart_num +=1
    
            mart_info = [mart_num, mart_name, mart_address, mart_call] 
            mart_list.append(mart_info)               
            

with open(f"/home/ubuntu/csvfile/mart_list_except_seoul_{current_date}.csv", mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerows(mart_list)

with open(f"/home/ubuntu/csvfile/mart_Link_except_seoul_{current_date}.csv", mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerows(mart_url_list.items())

print('마트 정보 및 url 크롤링 완료')
import re, csv, time, datetime, sys, os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
current_date = datetime.datetime.now().strftime('%Y-%m-%d')

# 옵션 생성, 창 숨기는 옵션 추가
options = webdriver.ChromeOptions()
# options.add_argument('--no-sandbox')        
options.add_argument('--headless')       
# options.add_argument('--disable-dev-shm-usage')
# options.add_argument("--disable-setuid-sandbox") 
# options.add_argument('--disable-gpu')
options.add_argument("--window-size=1920,1080")
options.add_argument('--ignore-certificate-errors')
options.add_argument('--allow-running-insecure-content')


service = ChromeService(executable_path = "/usr/bin/chromedriver")
driver = webdriver.Chrome(service=service, options=options)
# driver = webdriver.Chrome(options=options)

columns = ['product_id','product_code', 'mart_id', 'name', 'capacity', 'original_price', 'sale_price', 'detail_url', 'img_url','add_date']
current_date = datetime.datetime.now().strftime('%Y-%m-%d')
products_list = pd.DataFrame(columns=columns)

mart_url_list = {}
error_url_list = {}

def get_url():
    with open(f'/home/ubuntu/csvfile/mart_Link_{current_date}.csv', mode='r', encoding="utf-8") as inp:
        reader = csv.reader(inp)
        mart_url_list = {rows[0]:rows[1] for rows in reader}
    return mart_url_list

def get_data(mart_url_list, products_list):
    
    for mart_num, url in list(mart_url_list.items()):
        if '로마켓' in url:
            continue

        print(f'마트번호:{mart_num}')
        driver.get(url)

        # 스크롤 높이
        last_height = driver.execute_script("return document.body.scrollHeight")

        while True:
            # 끝까지 스크롤
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)

            # 스크롤 길이 비교로 끝까지 갔는지 확인
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
            time.sleep(1)

        print("페이지 내리기 완료")

        # 상품 정보 스크롤링

        try:
            print('상품 크롤링중')
            if mart_num in error_url_list:
                del error_url_list[mart_num]

            # element가 모두 나올떄 까지 기다리는 코드
            wait = WebDriverWait(driver, 10)
            wait.until(EC.presence_of_element_located((By.ID, "div_goods_detail_main")))

            element = driver.find_element(By.ID, "div_goods_detail_main")
            li_elements = element.find_elements(By.TAG_NAME, "li")

            total_products = 0

            for li in li_elements:
                try:
                    price_element = li.find_element(By.CLASS_NAME, "price")
                    price = price_element.text
                except:
                    price = ''
                    
                product_name_element = li.find_element(By.CLASS_NAME, "tit01")
                product_name_element = product_name_element.find_element(By.TAG_NAME, "a")
                
                try:
                    product_name_element = product_name_element.find_element(By.TAG_NAME,'div')
                    product_name = product_name_element.text
                
                except:
                    product_name = product_name_element.text

                product_unit_element = li.find_element(By.CLASS_NAME, "tit02")
                
                try:
                    product_unit_element = product_unit_element.find_element(By.TAG_NAME, "a")
                    product_unit = product_unit_element.text
                except:
                    product_unit = ''

                thumb_element = li.find_element(By.CLASS_NAME, "thumb")
                
                try:
                    picture_element = thumb_element.find_element(By.TAG_NAME, "a")
                    picture_element = picture_element.find_element(By.TAG_NAME, "img")
                    product_picture = picture_element.get_attribute('src')
                except:
                    product_picture = ''

                try:
                    url_element = thumb_element.find_element(By.TAG_NAME,'a')
                    url_element = url_element.get_attribute('onclick')
                except:
                    url_element = ''

                if price and product_name:
                    price = price.replace(',', '').replace('원', '').replace('(','').replace(')', ',')
                    price = price.split(',')
                    before_discount_price = price[0] if len(price) >= 1 else ''
                    after_discount_price = price[1] if len(price) >= 2 else price[0]
                    total_products += 1

                    url_element = re.findall(r'\d+', url_element)
                    url_element = [int(i) for i in url_element]
                    product_url = f'https://dnmart.co.kr/web2/web_pop_show_goods_detail_entire.html?shop_seq={url_element[0]}&goods_seq={url_element[1]}'

                    unique_id = int(str(mart_num) + str(url_element[1]))

                    products_info = [unique_id, url_element[1], mart_num, product_name, product_unit, before_discount_price, after_discount_price, product_url, product_picture, current_date]
                    new_df = pd.DataFrame([products_info], columns=columns)

                    products_list = pd.concat([products_list,new_df])
                    
            
            if len(li_elements) != total_products:
                print(f'{len(li_elements)}개 상품 중 {total_products}개 상품 크롤링 완료')

            print('크롤링 완료!')
            
        except Exception as e:
            error_url_list[mart_num]=url
            print('마트번호:', mart_num)
            print("An error occurred:", e)

    return error_url_list, products_list


def total_process(products_list):
    mart_url_list = get_url()
    error_url_list, products_list  = get_data(mart_url_list,products_list)
    while error_url_list:
        error_url_list, products_list = get_data(error_url_list,products_list)
    products_list.to_csv(f'/home/ubuntu/csvfile/products_list_{current_date}.csv', index=False)
    print("Finish")

total_process(products_list)

import pandas as pd
import requests, datetime

current_date = datetime.datetime.now().strftime('%Y-%m-%d')

address_csv = pd.read_csv(f"/home/ubuntu/csvfile/mart_list_{current_date}.csv", encoding='utf-8', index_col=0, header=None, names = ['mart_id','name','address','phone'])
mart_address=address_csv['mart_address']

apiurl = "https://api.vworld.kr/req/address?"

# print(address_csv)
for mart_num, address in mart_address.items():
    params = {
        "service": "address",
        "request": "getcoord",
        "crs": "epsg:4326",
        "address": f"{address}",
        "format": "json",
        "type": "parcel",
        "key": 'BDAFDD17-2CBF-3F3C-B32A-A8BBC534351B'
    }
    response = requests.get(apiurl, params=params)
    try:
        if response.status_code == 200:
            x = response.json()['response']['result']['point']['x']
            y = response.json()['response']['result']['point']['y']
    
    except:
        params = {
            "service": "address",
            "request": "getcoord",
            "crs": "epsg:4326",
            "address": f"{address}",
            "format": "json",
            "type": "road",
            "key": 'BDAFDD17-2CBF-3F3C-B32A-A8BBC534351B'
        }
        response = requests.get(apiurl, params=params)
        try:
            if response.status_code == 200:
                x = response.json()['response']['result']['point']['x']
                y = response.json()['response']['result']['point']['y']

        except Exception as e:
            print(e)
            x = ''
            y = ''

    # x좌표와 y좌표 컬럼 
    address_csv.loc[address_csv['mart_address'] == address, 'latitude'] = y
    address_csv.loc[address_csv['mart_address'] == address, 'longitude'] = x

# CSV 파일로 저장
address_csv.to_csv(f'/home/ubuntu/csvfile/mart_list_with_x_y_{current_date}.csv', encoding='utf-8')
print("finsh change address")


    

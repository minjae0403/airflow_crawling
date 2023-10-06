import pandas as pd
import requests, datetime

current_date = datetime.datetime.now().strftime('%Y-%m-%d')

address_csv = pd.read_csv(f"/home/ubuntu/csvfile/mart_list_{current_date}.csv", encoding='utf-8', header=None, names = ['mart_id','name','address','phone'])
mart_address=address_csv['address']

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
            x = 0
            y = 0

    # x좌표와 y좌표 컬럼 
    address_csv.loc[address_csv['address'] == address, 'latitude'] = y
    address_csv.loc[address_csv['address'] == address, 'longitude'] = x

# CSV 파일로 저장
address_csv.to_csv(f'/home/ubuntu/csvfile/mart_list_with_x_y_{current_date}.csv', encoding='utf-8')

address_list = address_csv.to_numpy().tolist()

def split_adress(response):
    zipcode = response.json()['response']['result'][0]['zipcode']
    structure = response.json()['response']['result'][0]['structure']
    country = structure['level0']
    Cities = structure['level1']
    county = structure['level2']
    district = structure['level3']
    dong = structure['level4A']

    return zipcode, country, Cities, county, district, dong

for info in address_list:

    mart_id = info[0]
    mart_longitude = info[4]
    mart_latitudes = info[5]

    params = {
        "service": "address",
        "version": "2.0",
        "request": "GetAddress",
        "crs": "epsg:4326",
        "point": f"{mart_latitudes}, {mart_longitude}",
        "format": "json",
        "type": "PARCEL",
        "zipcode": "true",
        "key": 'BDAFDD17-2CBF-3F3C-B32A-A8BBC534351B'
    }
    response = requests.get(apiurl, params=params)
    try:
         if response.status_code == 200:
             zipcode, country, Cities, county, district, dong = split_adress(response)        
    
    except:
        response = requests.get(apiurl, params=params)
        try:
            if response.status_code == 200:
                zipcode, country, Cities, county, district, dong = split_adress(response)

        except Exception as e:
            print(e)

    # x좌표와 y좌표 컬럼
    address_csv.loc[(address_csv['mart_id'] == mart_id), 'zipcode'] = zipcode
    address_csv.loc[(address_csv['mart_id'] == mart_id), 'country'] = country
    address_csv.loc[(address_csv['mart_id'] == mart_id), 'cities'] = Cities
    address_csv.loc[(address_csv['mart_id'] == mart_id), 'county'] = county
    address_csv.loc[(address_csv['mart_id'] == mart_id), 'dong'] = dong

address_csv.fillna('No_data')
# CSV 파일로 저장
address_csv.to_csv(f'/home/ubuntu/csvfile/mart_list_with_location_{current_date}.csv', encoding='utf-8', index=False)
print("finsh change address")


    

import pandas as pd
import re, datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pandas as pd

current_date = datetime.datetime.now().strftime('%Y-%m-%d')

spark = SparkSession.builder \
        .master("local") \
        .appName("Spark") \
        .config("spark.sql.repl.eagerEval.enabled", True) \
        .getOrCreate()

products_name_df = pd.read_csv(f'/home/ubuntu/csvfile/products_list_{current_date}.csv', encoding='utf-8')

for name in products_name_df['name']:
    if '크라운' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '크라운'
    elif '코카' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '코카콜라'
    elif 'cj' in name or 'CJ' in name or '씨제이' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = 'CJ'
    elif 'P&G' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = 'P&G'
    elif '광동' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '광동'
    elif '농심' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '농심'
    elif 'cj' in name or 'CJ' in name or '씨제이' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = 'CJ'
    elif '동서' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '동서'
    elif '덴마크' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '덴마크'
    elif '동아' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '동아'
    elif '동원' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '동원'
    elif '동화' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '동화'
    elif '롯데' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '롯데'
    elif '맥심' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '맥심'
    elif '빙그레' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '빙그레'
    elif '맥심' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '맥심'
    elif '삼양' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '삼양'
    elif '샘표' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '샘표'
    elif '서울' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '서울'
    elif '스타벅스' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '스타벅스'
    elif '애경' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '애경'
    elif 'LG' in name or '엘지' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = 'LG'
    elif '오뚜기' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '오뚜기'
    elif '오리온' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '오리온'
    elif '웅진' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '웅진'
    elif '오리온' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '오리온'
    elif '청정원' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '청정원'
    elif '풀무원' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '풀무원'
    elif '하림' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '하림' 
    elif '해태' in name:
        products_name_df.loc[products_name_df['name'] == name,'manufacture'] = '해태'
    else:
        products_name_df.loc[products_name_df['name'] == name, 'manufacture'] = 'No_data'
        
unique_manufacturer = []
for i in products_name_df['name']:
    i = re.split(r'[/)]',i)    
    if i[-1] == '':
        i = i[0]+i[1]+')'
    if len(i) == 2:
        product_manufacturer = i[0]
        product_name = i[1]
        unique_manufacturer.append(i[0])
    elif len(i) == 3:
        product_manufacturer = i[0]
        product_name = i[1]+i[2]
        unique_manufacturer.append(i[0])
    else:
        try:
            i = re.split(r'[`]',i)
        except:
            pass
        product_name = i[0]
        
for i in products_name_df['capacity']:
    List_i = re.split(r'[()]',str(i))
    if len(List_i) == 1:
        if '당' in str(i):
            List_i = i.split(' 당 ')
            products_name_df.loc[products_name_df['capacity'] == i, 'capacity'] = List_i[1]
            products_name_df.loc[products_name_df['capacity'] == i, 'capacity_2'] = i        
        else:
            products_name_df.loc[products_name_df['capacity'] == i, 'capacity'] = List_i[0]
            products_name_df.loc[products_name_df['capacity'] == i, 'capacity_2'] = 'No_data'
    elif len(List_i) == 2 or len(List_i) == 3:
        products_name_df.loc[products_name_df['capacity'] == i, 'capacity'] = List_i[0]
        products_name_df.loc[products_name_df['capacity'] == i, 'capacity_2'] = List_i[1]
    elif len(List_i) == 4:
        products_name_df.loc[products_name_df['capacity'] == i, 'capacity'] = List_i[0]
        products_name_df.loc[products_name_df['capacity'] == i, 'capacity_2'] = List_i[2]
    elif len(List_i) == 5:
        if '25*35' in str(i) or '찰진밥' in str(i):
            i_split = str(i).split('(')
            condition = (products_name_df['capacity'] == str(i))
            products_name_df.loc[condition, 'capacity'] = i_split[0]+'('+i_split[1]
            products_name_df.loc[condition, 'capacity_2'] = '('+i_split[2]
        else:
            try:
                List_i = str(i).split(')(')
                products_name_df.loc[products_name_df['capacity'] == i, 'capacity'] = List_i[0] +')'
                products_name_df.loc[products_name_df['capacity'] == i, 'capacity_2'] = List_i[1].replace(')','')
            except:
                List_i = str(i).split('(')
                input_string = '(' + List_i[1]
                input_string = re.sub(r'\([^)]*\)', '', input_string)
                # print(List_i)
                # print(input_string)
                # print(List_i[2].replace(')',''))
                # print('============')
                products_name_df.loc[products_name_df['capacity'] == i, 'capacity'] = input_string
                products_name_df.loc[products_name_df['capacity'] == i, 'capacity_2'] = List_i[2].replace(')','')
    else:
        print(List_i)

products_name_df = products_name_df.fillna('No_data')
products_name_df = products_name_df.replace(to_replace = '', value = 'No_data')

originProductDF = spark.createDataFrame(products_name_df)

id_count_df = originProductDF.groupby('product_code').count()
id_list = id_count_df.select('product_code').rdd.flatMap(lambda x: x).collect()
id_count = id_count_df.select('count').rdd.flatMap(lambda x: x).collect()

schema = originProductDF.schema
newDF = spark.createDataFrame([], schema)

newProductDF = originProductDF.toPandas()

try:
    for i in range(len(id_list)):
        first_name = newProductDF[newProductDF['product_code'] == id_list[i]].name
        # print(first_name)
        first_manufacture = 'No_data'
        a = 0
        try:
            while(first_manufacture == 'No_data'):
                first_manufacture = newProductDF[newProductDF['product_code'] == id_list[i]].iloc[a].manufacture
                a += 1
        except:
            first_manufacture = 'No_data'
            a += 1
        a = 0
        first_capacity_2 = 'No_data'
        try:
            while(first_capacity_2 == 'No_data'):
                first_capacity_2 = newProductDF[newProductDF['product_code'] == id_list[i]].iloc[a].capacity_2
                a += 1
        except:
            first_capacity_2 = 'No_data'
            a += 1
        for j in range(id_count[i]):
            newProductDF.loc[newProductDF['product_code'] == id_list[i], 'name'] = first_name
            newProductDF.loc[newProductDF['product_code'] == id_list[i], 'manufacture'] = first_manufacture
            newProductDF.loc[newProductDF['product_code'] == id_list[i], 'capacity_2'] = first_capacity_2
except Exception as e:
    print(e)

newProductDF.to_csv('first_modified_product_list.csv',index=False, encoding='utf-8')

# 식자재 csv데이터 로드
originProductDF = spark.read.option("header","True").csv("first_modified_product_list.csv")

id_count_df = originProductDF.groupBy('product_id').count()
id_list = id_count_df.select('product_id').rdd.flatMap(lambda x: x).collect()
id_count = id_count_df.select('count').rdd.flatMap(lambda x: x).collect()

schema = originProductDF.schema
newDF = spark.createDataFrame([], schema)
newProductDF = originProductDF.toPandas()

try:
    for i in range(len(id_list)):
        first_name = newProductDF[newProductDF['product_id'] == id_list[i]].name
        first_manufacture = 'No_data'
        a = 0
        try:
            while(first_manufacture == 'No_data'):
                first_manufacture = newProductDF[newProductDF['product_id'] == id_list[i]].iloc[a].manufacture
                a += 1
        except:
            first_manufacture = 'No_data'
            a += 1
        a = 0
        first_product_capacity_2 = 'No_data'
        try:
            while(first_product_capacity_2 == 'No_data'):
                first_product_capacity_2 = newProductDF[newProductDF['product_id'] == id_list[i]].iloc[a].capacity_2
                a += 1
        except:
            first_product_capacity_2 = 'No_data'
            a += 1
        for j in range(id_count[i]):
            newProductDF.loc[newProductDF['product_id'] == id_list[i], 'name'] = first_name
            newProductDF.loc[newProductDF['product_id'] == id_list[i], 'manufacture'] = first_manufacture
            newProductDF.loc[newProductDF['product_id'] == id_list[i], 'capacity_2'] = first_product_capacity_2
except Exception as e:
    print(e)

newProductDF=newProductDF.drop_duplicates(['product_id'], keep='first')

newProductDF.to_csv(f'/home/ubuntu/csvfile/modified_products_list_{current_date}.csv',index=False, encoding='utf-8')
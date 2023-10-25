import datetime, csv, sys, os, pymysql
import pandas as pd
from sqlalchemy import create_engine, text ,insert, delete, ForeignKey
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.inspection import inspect

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
current_date = datetime.datetime.now().strftime('%Y-%m-%d')

# MySQL 접속 정보
db_host = 'mysqltest.c5aznfazxhi5.ap-northeast-2.rds.amazonaws.com'
db_user = 'admin'
db_password = 'qwer1234'
db_database = 'jpatest'
table_name = 'product'

# MySQL 데이터베이스에 연결
db_engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_database}')

# CSV 파일을 데이터프레임으로 읽어오기
csv_file = f'/home/ubuntu/csvfile/products_list_{current_date}.csv'

current_date = datetime.datetime.now().strftime('%Y-%m-%d')
Base = declarative_base()

# table 형태 미리 정의
class User(Base):
    __tablename__ = table_name
    product_id = Column(Integer, primary_key=True, index=True)
    product_code = Column(Integer, index=True)
    mart_id = Column(Integer, ForeignKey('mart.mart_id'), index=True)
    name = Column(String(255), index=True)
    capacity = Column(String(255), index=True)
    original_price = Column(Float, index=True)
    sale_price = Column(Integer, index=True)
    detail_url = Column(String(255), index=True)
    img_url = Column(String(255), index=True)
    add_date = Column(String(255), index=True)
   
def db_table():
    if not inspect(db_engine).has_table(User.__tablename__):
        Base.metadata.create_all(db_engine)


#csv to map
def to_map(csv_file_path):
    try:
        csvfile = pd.read_csv(csv_file, encoding='utf-8')
        csvfile = csvfile.fillna('No_data')
        csvfile = csvfile.drop_duplicates(['product_id'], keep='first')
        data_list = csvfile.to_numpy().tolist()

        return data_list
    
    except Exception as e:
        print(e)

def import_data(data_list):
    try:
        connection = pymysql.connect(host = db_host, user = db_user, password = db_password, db = db_database)
        cursor = connection.cursor()

        query =f"""
        INSERT INTO {table_name} (
            `product_id`,
            `product_code`,
            `mart_id`,
            `name`,
            `capacity`,
            `original_price`,
            `sale_price`,
            `detail_url`,
            `img_url`,
            `add_date`            
            )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            `product_id`=VALUES(`product_id`),
            `product_code`=VALUES(`product_code`),
            `mart_id`=VALUES(`mart_id`),
            `name`=VALUES(`name`),
            `capacity`=VALUES(`capacity`),
            `original_price`=VALUES(`original_price`),
            `sale_price`=VALUES(`sale_price`),
            `detail_url`=VALUES(`detail_url`),
            `img_url`=VALUES(`img_url`),
            `add_date`=VALUES(`add_date`)
            
        """

        # sql 쿼리 적용
        cursor.executemany(query, data_list)

        # sql 쿼리 실행
        connection.commit()

        # DB연결 종료
        connection.close()

        print("Finish Import.")
    
    except Exception as e:
        print(e)

db_table()
data_list = to_map(csv_file)
import_data(data_list)
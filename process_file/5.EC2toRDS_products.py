import datetime, csv, sys, os
import pandas as pd
from sqlalchemy import create_engine, text ,insert, delete
from sqlalchemy import Column, Integer, String,  Float
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
csv_file = f'/home/ubuntu/csvfile/modified_products_list_{current_date}.csv'

current_date = datetime.datetime.now().strftime('%Y-%m-%d')
Base = declarative_base()

# table 형태 미리 정의
class User(Base):
    __tablename__ = table_name
    product_id = Column(Integer, primary_key=True, index=True)
    product_code = Column(Integer, index=True)
    mart_id = Column(Integer, index=True)
    name = Column(String(255), index=True)
    capacity = Column(String(255), index=True)
    original_price = Column(Integer, index=True)
    sale_price = Column(Integer, index=True)
    detail_url = Column(String(255), index=True)
    img_url = Column(String(255), index=True)
    add_date = Column(String(255), index=True)
    manufacture = Column(String(255), index=True)
    capacity_2 = Column(String(255), index=True)

def db_table():
    if not inspect(db_engine).has_table(User.__tablename__):
        Base.metadata.create_all(db_engine)
    else:
        with db_engine.begin() as connection:
            setting_stme = 'set sql_safe_updates=0;'
            delete_stmt = delete(Base.metadata.tables[User.__tablename__])
            connection.execute(text(setting_stme))
            connection.execute(delete_stmt)

#csv to map
def to_map(csv_file_path):
    data_list = []
    try:
        csvfile = pd.read_csv(csv_file)
        
        for row in csvfile.itertuples():
            print(row)
            data_list.append(row)

        return data_list
    except Exception as e:
        print(e)

def import_data(data_list):

    with db_engine.connect() as connection:
        stmt = insert(User).values(data_list)
        connection.execute(stmt)

    print("Finish Import.")

db_table()
data_list = to_map(csv_file)
import_data(data_list)
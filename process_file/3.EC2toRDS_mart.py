import csv, datetime
from sqlalchemy import create_engine, insert, delete, Column, text
from sqlalchemy.types import Float, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.inspection import inspect

current_date = datetime.datetime.now().strftime('%Y-%m-%d')

# MySQL 접속 정보
db_host = 'mysqltest.c5aznfazxhi5.ap-northeast-2.rds.amazonaws.com'
db_user = 'admin'
db_password = 'qwer1234'
db_database = 'jpatest'
table_name = 'mart'

# CSV 파일을 데이터프레임으로 읽어오기
csv_file = f'/home/ubuntu/csvfile/mart_list_with_x_y_{current_date}.csv'
# df_csv = pd.read_csv(csv_file, encoding='utf-8', index_col=0, header= None)

# MySQL 데이터베이스에 연결
db_engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_database}')

# table 형태 미리 정의
Base = declarative_base()
class User(Base):
    __tablename__ = table_name
    mart_id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), index=True)
    address = Column(String(255), index=True)
    latitude = Column(Float, index=True)
    longitude = Column(Float, index=True)
    phone = Column(String(255), index=True)

def db_table():
    if not inspect(db_engine).has_table(User.__tablename__):
        Base.metadata.create_all(db_engine)
    else:
        with db_engine.begin() as connection:
            setting_stme = 'set sql_safe_updates=0;'
            delete_stmt = delete(Base.metadata.tables[User.__tablename__])
            # print(delete_stmt)
            connection.execute(text(setting_stme))
            connection.execute(delete_stmt)
            # connection.commit()

#csv to map
def to_map(csv_file_path):
    data_list = []
    try:
        with open(csv_file_path, 'r') as csvfile:
            csvreader = csv.DictReader(csvfile)
            
            for row in csvreader:
                data_list.append(row)

        return data_list
    except Exception as e:
        print(e)

def import_data(data_list):

    with db_engine.connect() as connection:
        stmt = insert(User).values(data_list)
        # print(stmt)
        connection.execute(stmt)
        # connection.commit()

    print("Finish Import.")

db_table()
data_list = to_map(csv_file)
import_data(data_list)
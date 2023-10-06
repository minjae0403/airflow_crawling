from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime as dt

current_date = dt.datetime.now().strftime('%Y-%m-%d')

default_args={
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 5, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2023, 10, 24, 0, 0),
}

dag = DAG(
    dag_id='except_seoul_process',
    default_args=default_args,
    description='Crawling and pretreatment process DAG',
    schedule= '0 16 * * *'
)

t1 = BashOperator(
    task_id='URL_Crawling',
    bash_command='python3 /home/ubuntu/crawling_pythonfile/7.RowMarketURLCrawling_except_seoul.py',
    dag=dag
)

t2 = BashOperator(
    task_id='Address_Changing',
    bash_command='python3 /home/ubuntu/crawling_pythonfile/8.AddressChange_except_seoul.py',
    dag=dag
)

t3 = BashOperator(
    task_id='S3_save_mart_csv',
    bash_command=f'aws s3 cp /home/ubuntu/csvfile/mart_list_except_seoul_with_x_y_{current_date}.csv s3://scvfile/backup_mart_list/',
    dag=dag
)

t1 >> t2 >> t3
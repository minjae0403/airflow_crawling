from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime as dt

current_date = dt.datetime.now().strftime('%Y-%m-%d')

default_args={
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 25, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2023, 10, 24, 0, 0),
}

dag = DAG(
    dag_id='Main_process',
    default_args=default_args,
    description='Crawling and pretreatment process DAG',
    schedule= '0 16 * * *'
)

t1 = BashOperator(
    task_id='URL_Crawling',
    bash_command='python3 /home/ubuntu/crawling_pythonfile/1.RowMarketURLCrawling.py',
    dag=dag
)

t2 = BashOperator(
    task_id='Address_Changing',
    bash_command='python3 /home/ubuntu/crawling_pythonfile/2.AddressChange.py',
    dag=dag
)

t3 = BashOperator(
     task_id='EC2_to_RDS',
     bash_command='python3 /home/ubuntu/crawling_pythonfile/3.EC2toRDS_mart.py',
     dag=dag
)

t4 = BashOperator(
    task_id='Products_Crawling',
    bash_command='python3 /home/ubuntu/crawling_pythonfile/4.RowMarketProductsCrawling.py',
    dag=dag
)

t5 = BashOperator(
    task_id='Products_preprocessing',
    bash_command='spark-submit /home/ubuntu/crawling_pythonfile/6.Productspreprocessing.py',
    dag=dag
)


t6 = BashOperator(
     task_id='EC2_to_RDS_products',
     bash_command='python3 /home/ubuntu/crawling_pythonfile/5.EC2toRDS_products.py',
     dag=dag
)

t7 = BashOperator(
    task_id='S3_save_mart_csv',
    bash_command=f'aws s3 cp /home/ubuntu/csvfile/mart_list_with_x_y_{current_date}.csv s3://scvfile/backup_mart_list/',
    dag=dag
)

t8 = BashOperator(
    task_id='S3_save_products_csv',
    bash_command=f'aws s3 cp /home/ubuntu/csvfile/modified_products_list_{current_date}.csv s3://scvfile/backup_product_list/',
    dag=dag
)

t9 = BashOperator(
    task_id='S3_save_mart_with_location_csv',
    bash_command=f'aws s3 cp /home/ubuntu/csvfile/mart_list_with_location_{current_date}.csv s3://scvfile/backup_mart_list/',
    dag=dag
)



t1 >> [t2,t4]
t2 >> [t9, t7, t3]
t4 >> t5
t5 >> [t8, t6]

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime as dt

current_date = dt.datetime.now().strftime('%Y-%m-%d')

default_args={
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 6, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2023, 10, 24, 0, 0),
}

dag = DAG(
    dag_id='MySQLtoS3_process',
    default_args=default_args,
    description='MySQL DataBase Table move to S3 process DAG',
    schedule= '30 15 * * *'
)

t1 = BashOperator(
    task_id='MovetoS3',
    bash_command='python3 /home/ubuntu/MySQLtoS3_pythonfile/MySQLtoS3.py',
    dag=dag
)

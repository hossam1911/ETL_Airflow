from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
import pandas as pd 
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

def uncompress_tar_file(file_path, extract_path):
    with tarfile.open(file_path, 'r') as tar:
        tar.extractall(path=extract_path)
def extract(file_path,loc_path):
    df =pd.read_csv(file_path)
    selected_columns = df.loc[:, ['Rowid', 'Timestamp', 'Vehicle number','Vehicle type']]
    selected_columns.to_csv(loc_path)

def extract2(file_path,loc_path):
    df = pd.read_csv(file_path, sep='\t') 
    selected_columns = df.loc[:, ['Number of axles', 'Tollplaza id', 'Tollplaza code']]
    selected_columns.to_csv(loc_path) 

def extract3(file_path,loc_path):
    df = pd.read_csv(file_path) 
    selected_columns = df.loc[:, ['Type of Payment code', 'Vehicle Code']]
    selected_columns.to_csv(loc_path) 

def Transform(file_path,loc_path):
    df= pd.read_csv(file_path)
    df['vehicle_type']=df['vehicle_type'].str.upper()
    df.to_csv(loc_path)

default_args = {
    'owner': 'Hossam Metwally',
    'start_date': days_ago(0),
    'email': ['Hossam12@yahoo.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

unzip_data = PythonOperator(
    task_id='unzip_data',
    python_callable=uncompress_tar_file,
    op_kwargs={'file_path': 'tolldata.tgz', 'extract_path': '/home/project'},
    dag=dag,
)

extract_data_from_csv = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract,
    op_kwargs={'file_path': 'vehicle-data.csv', 'loc_path': 'csv_data.csv'},
    dag=dag,
)

extract_data_from_tsv = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract2,
    op_kwargs={'file_path': 'tollplaza-data.tsv', 'loc_path': 'tsv_data.csv'},
    dag=dag,
)


extract_data_from_fixed_width = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract3,
    op_kwargs={'file_path': 'payment-data.txt', 'loc_path': 'fixed_width_data.csv'},
    dag=dag,
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command = 'paste csv_data.csv tsv_data.csv fixed_width_data.csv > final.csv'
    ,dag=dag,
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=Transform,
    op_kwargs={'file_path': 'final.csv', 'loc_path': 'transformed_data.csv'},
    dag=dag,
)

unzip_data > extract_data_from_csv > extract_data_from_tsv > extract_data_from_fixed_width > consolidate_data > transform_data
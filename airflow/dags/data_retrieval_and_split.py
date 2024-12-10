from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sklearn.model_selection import train_test_split
import gspread
from oauth2client.service_account import ServiceAccountCredentials

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_retrieval_and_split',
    default_args=default_args,
    description='Pobranie i podział danych',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 12, 1),
    catchup=False,
)


def download_data():
    data = pd.read_csv('/opt/airflow/include/raw_data.csv')
    data.to_csv('/opt/airflow/downloaded_data.csv', index=False)


def split_data():
    data = pd.read_csv('/opt/airflow/downloaded_data.csv')
    train, test = train_test_split(data, test_size=0.3, random_state=42)
    train.to_csv('/opt/airflow/train_data.csv', index=False)
    test.to_csv('/opt/airflow/test_data.csv', index=False)


def upload_to_gsheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('/opt/airflow/include/credentials.json', scope)
    client = gspread.authorize(creds)

    train = pd.read_csv('/opt/airflow/train_data.csv')
    test = pd.read_csv('/opt/airflow/test_data.csv')

    sheet_train = client.open('DataSplit').worksheet('TrainData')
    sheet_test = client.open('DataSplit').worksheet('TestData')

    sheet_train.update([train.columns.values.tolist()] + train.values.tolist())
    sheet_test.update([test.columns.values.tolist()] + test.values.tolist())


download_task = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    dag=dag,
)

split_task = PythonOperator(
    task_id='split_data',
    python_callable=split_data,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_gsheets',
    python_callable=upload_to_gsheets,
    dag=dag,
)

download_task >> split_task >> upload_task

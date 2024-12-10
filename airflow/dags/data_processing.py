from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.preprocessing import LabelEncoder
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
    'data_processing',
    default_args=default_args,
    description='Przetwarzanie danych',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 12, 1),
    catchup=False,
)


def download_from_gsheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('/opt/airflow/include/credentials.json', scope)
    client = gspread.authorize(creds)

    sheet = client.open('DataSplit').worksheet('TrainData')
    data = pd.DataFrame(sheet.get_all_records())

    data.to_csv('/opt/airflow/raw_data.csv', index=False)


def clean_data():
    data = pd.read_csv('/opt/airflow/raw_data.csv')

    # Drop rows with missing values and duplicates
    data = data.dropna()
    data = data.drop_duplicates()

    # Handle categorical data (Label Encoding for 'yes'/'no' and other categorical columns)
    le = LabelEncoder()
    categorical_columns = ['mainroad', 'guestroom', 'basement', 'hotwaterheating', 'airconditioning', 'prefarea',
                           'furnishingstatus']

    for col in categorical_columns:
        data[col] = le.fit_transform(data[col])

    data.to_csv('/opt/airflow/clean_data.csv', index=False)


def scale_data():
    data = pd.read_csv('/opt/airflow/clean_data.csv')

    # Separate numerical and categorical columns
    numerical_columns = data.select_dtypes(include=['float64', 'int64']).columns

    # Scaling numerical columns
    scaler = StandardScaler()
    data[numerical_columns] = scaler.fit_transform(data[numerical_columns])

    normalizer = MinMaxScaler()
    data[numerical_columns] = normalizer.fit_transform(data[numerical_columns])

    data.to_csv('/opt/airflow/processed_data.csv', index=False)


def upload_processed_data_to_gsheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('/opt/airflow/include/credentials.json', scope)
    client = gspread.authorize(creds)

    data = pd.read_csv('/opt/airflow/processed_data.csv')

    sheet = client.open('ProcessedData').worksheet('ProcessedDataSheet')
    sheet.update([data.columns.values.tolist()] + data.values.tolist())


download_data_task = PythonOperator(
    task_id='download_from_gsheets',
    python_callable=download_from_gsheets,
    dag=dag,
)

clean_data_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag,
)

scale_data_task = PythonOperator(
    task_id='scale_data',
    python_callable=scale_data,
    dag=dag,
)

upload_data_task = PythonOperator(
    task_id='upload_processed_data_to_gsheets',
    python_callable=upload_processed_data_to_gsheets,
    dag=dag,
)

download_data_task >> clean_data_task >> scale_data_task >> upload_data_task

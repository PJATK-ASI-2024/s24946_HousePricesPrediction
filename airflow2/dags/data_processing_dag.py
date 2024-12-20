from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

def process_data():

    file_path = '/opt/airflow/dags/Housing.csv'
    df = pd.read_csv(file_path)

    df.drop_duplicates(inplace=True)

    for column in ['bedrooms', 'bathrooms', 'stories', 'parking']:
        df[column].fillna(df[column].median(), inplace=True)

    numeric_columns = ['area', 'bedrooms', 'bathrooms', 'stories', 'parking']
    df[numeric_columns] = (df[numeric_columns] - df[numeric_columns].mean()) / df[numeric_columns].std()

    categorical_columns = ['mainroad', 'guestroom', 'basement', 'hotwaterheating', 'airconditioning', 'prefarea', 'furnishingstatus']
    df = pd.get_dummies(df, columns=categorical_columns, drop_first=True)

    os.makedirs('/opt/airflow/processed_data', exist_ok=True)

    df.to_csv('/opt/airflow/processed_data/processed_data.csv', index=False)

with DAG(
    dag_id='data_processing_dag',
    start_date=datetime(2024, 12, 1),
    schedule_interval=None
) as dag:
    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data
    )

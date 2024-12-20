from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import pickle
import os

def train_model():

    df = pd.read_csv('/opt/airflow/processed_data/processed_data.csv')

    X = df.drop('price', axis=1)
    y = df['price']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    model = RandomForestRegressor(random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)

    os.makedirs('/opt/airflow/models', exist_ok=True)
    os.makedirs('/opt/airflow/reports', exist_ok=True)

    with open('/opt/airflow/models/model.pkl', 'wb') as f:
        pickle.dump(model, f)

    with open('/opt/airflow/reports/evaluation_report.txt', 'w') as f:
        f.write(f'MAE: {mae}\n')

with DAG(
    dag_id='model_training_dag',
    start_date=datetime(2024, 12, 1),
    schedule_interval=None
) as dag:
    train_model_task = PythonOperator(
        task_id='train_model',
        python_callable=train_model
    )

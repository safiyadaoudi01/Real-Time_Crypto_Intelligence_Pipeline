from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

def run_fred_task():
    subprocess.run(["python", "/opt/airflow/dags/fred_test.py"], check=True)

with DAG(
    "fred_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=False
) as dag:

    fred_task = PythonOperator(
        task_id="fetch_and_produce_fred",
        python_callable=run_fred_task
    )
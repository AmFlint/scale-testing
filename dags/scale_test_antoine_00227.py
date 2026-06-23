from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="scale_test_antoine_00227",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["scale-test"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    workers = [EmptyOperator(task_id=f"task_{i}") for i in range(98)]
    start >> workers >> end

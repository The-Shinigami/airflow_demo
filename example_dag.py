from airflow import DAG
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator

with DAG("example",
    start_date=datetime(2023,2,10),
    schedule_interval="*/1 * * * *",
    catchup=False,
    max_active_runs=1) as dag: 
    #   Python Tasks
    a = DummyOperator(
        task_id="a"
    )
    b = DummyOperator(
        task_id="b"
    )
    c = DummyOperator(
        task_id="c"
    )
    a >> b >> c
from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

from airflow.models import Variable
import pandas as pd
from datetime import datetime
import shutil
import os

def _check_csv_files_in_directory():
    file_path = Variable.get("file_path")
    files = os.listdir(file_path+'/in')
    csv_files = [f for f in files if f.endswith('.csv')]
    print(csv_files)
    if len(csv_files) > 0:
        return "read_file"
# -------------------------------------------------
def _read_file():
    file_path = Variable.get("file_path")
    data_frame = pd.read_csv(file_path+'/in/data.csv')
    return data_frame.to_json(orient="split")
# -------------------------------------------------
def _insert_data_db(task_instance):
    data_json = task_instance.xcom_pull(task_ids="read_file")
    data_frame = pd.read_json(data_json, orient="split")
     # Create a PostgresHook to connect to the database.
    pg_hook = PostgresHook(postgres_conn_id='task_1_db')

     # Iterate over each row of the DataFrame and insert the data into the database.
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    for _, row in data_frame.iterrows():
        insert_sql = """
            INSERT INTO task (id, email, date, role)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """
        values = (row['id'], row['email'], row['date'], row['role'])
        cursor.execute(insert_sql, values)
    conn.commit()
    cursor.close()
    conn.close()
# -------------------------------------------------

def _move_file():
    file_path = Variable.get("file_path")
    shutil.move(file_path+'/in/data.csv', file_path+'/out/data.csv')
# -------------------------------------------------

with DAG("task_1_0",
    start_date=datetime(2023,2,10),
    schedule_interval="*/1 * * * *",
    catchup=False) as dag:
#   Python Tasks
    insert_data_db = PythonOperator(
        task_id="insert_data",
        python_callable = _insert_data_db
    )
    read_file = PythonOperator(
        task_id="read_file",
        python_callable=_read_file
    )
    move_file = PythonOperator(
        task_id="move_file",
        python_callable=_move_file
    )
#   Postgres Tasks 
    create_task_table = PostgresOperator(
        task_id="create_task_table",
        postgres_conn_id="task_1_db",
        sql="""
            CREATE TABLE IF NOT EXISTS task (
            id SERIAL PRIMARY KEY,
            email VARCHAR NOT NULL,
            role VARCHAR NOT NULL,
            date DATE NOT NULL);
          """,
    )
#   Branch
    check_csv_files_in_directory = BranchPythonOperator(
        task_id="check_csv_files_in_directory",
        python_callable = _check_csv_files_in_directory
    )

    check_csv_files_in_directory >> read_file >> create_task_table >> insert_data_db >> move_file
    
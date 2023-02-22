from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator, get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import pandas as pd
from datetime import datetime
import shutil
import os


# def _check_csv_files_in_directory(task_instance):
#     file_path = Variable.get("file_path")
#     files = os.listdir(file_path + '/in')
#     csv_files = [f for f in files if f.endswith('.csv')]
#     task_instance.xcom_push(key="files",value=csv_files)
#     if len(csv_files) > 0:
#         return "process_files"

    
# def _read_file(file_name):
#     file_path = Variable.get("file_path")
#     data_frame = pd.read_csv(f"{file_path}/in/{file_name}")
#     return data_frame.to_json(orient="split")


# def _insert_data_db(task_instance, data_json):
#     # data_frame = pd.read_json(data_json, orient="split")
#     data_json = task_instance.xcom_pull(task_ids="read_file")
#     data_frame = pd.read_json(data_json, orient="split")
#     pg_hook = PostgresHook(postgres_conn_id='task_1_db')
#     conn = pg_hook.get_conn()
#     cursor = conn.cursor()
#     for _, row in data_frame.iterrows():
#         insert_sql = """
#             INSERT INTO task (id, email, date, role)
#             VALUES (%s, %s, %s, %s)
#             ON CONFLICT (id) DO NOTHING
#         """
#         values = (row['id'], row['email'], row['date'], row['role'])
#         cursor.execute(insert_sql, values)
#     conn.commit()
#     cursor.close()
#     conn.close()


# def _move_file(file_name):
#     file_path = Variable.get("file_path")
#     shutil.move(f"{file_path}/in/{file_name}", f"{file_path}/out/{file_name}")


# with DAG("task_1_dag",
#          start_date=datetime(2023, 2, 10),
#          schedule_interval="*/1 * * * *",
#          catchup=False) as dag:
#     check_csv_files_in_directory = BranchPythonOperator(
#             task_id="check_csv_files_in_directory",
#             python_callable=_check_csv_files_in_directory,
#             provide_context=True
#         )
#     with TaskGroup(group_id='process_files') as process_files:
#         context = get_current_context()
#         files = context['ti'].xcom_pull(
#             task_ids='check_csv_files_in_directory', key='files')
#         for file in files :

#             read_file = PythonOperator(
#                 task_id="read_file",
#                 python_callable=_read_file,
#                 op_kwargs={"file_name": file}
#             ) 
            
#             create_task_table = PostgresOperator(
#                 task_id="create_task_table",
#                 postgres_conn_id="task_1_db",
#                 sql="""
#                     CREATE TABLE IF NOT EXISTS task (
#                         id SERIAL PRIMARY KEY,
#                         email VARCHAR NOT NULL,
#                         role VARCHAR NOT NULL,
#                         date DATE NOT NULL
#                     );
#                 """,
#             )

#             insert_data_db = PythonOperator(
#                 task_id="insert_data",
#                 python_callable=_insert_data_db,
#                 # op_kwargs={"data_json": "{{ task_instance.xcom_pull(task_ids='read_file') }}"},
#             )

#             move_file = PythonOperator(
#                 task_id="move_file",
#                 python_callable=_move_file,
#                 op_kwargs={"file_name": file},
#                 # op_kwargs={"file_name": "{{ task_instance.xcom_pull(task_ids='check_csv_files_in_directory') }}"},
#             )
#             read_file >> create_task_table >> insert_data_db >> move_file

       
#         check_csv_files_in_directory >> process_files


def _check_csv_files_in_directory():
    file_path = Variable.get("file_path")
    files = os.listdir(file_path + '/in')
    csv_files = [f for f in files if f.endswith('.csv')]
    return csv_files

def _read_file(file_name):
    file_path = Variable.get("file_path")
    data_frame = pd.read_csv(f"{file_path}/in/{file_name}")
    return data_frame.to_json(orient="split")


def _insert_data_db(task_instance, file_name):
    # data_frame = pd.read_json(data_json, orient="split")
    data_json = task_instance.xcom_pull(task_ids=f"process_files.read_file_{file_name}")
    data_frame = pd.read_json(data_json, orient="split")
    pg_hook = PostgresHook(postgres_conn_id='task_1_db')
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


def _move_file(file_name):
    file_path = Variable.get("file_path")
    shutil.move(f"{file_path}/in/{file_name}", f"{file_path}/out/{file_name}")


with DAG("task_1", start_date=datetime(2023, 2, 10), schedule_interval="*/1 * * * *", catchup=False) as dag:

    csv_files = _check_csv_files_in_directory()

    with TaskGroup(group_id='process_files') as process_files:
        for file in csv_files:
            read_file = PythonOperator(
                task_id=f"read_file_{file[:-4]}",
                python_callable=_read_file,
                op_kwargs={"file_name": file},
                provide_context=True
            ) 
            create_task_table = PostgresOperator(
                task_id=f"create_task_table_{file[:-4]}",
                postgres_conn_id="task_1_db",
                sql="""
                    CREATE TABLE IF NOT EXISTS task (
                        id SERIAL PRIMARY KEY,
                        email VARCHAR NOT NULL,
                        role VARCHAR NOT NULL,
                        date DATE NOT NULL
                    );
                """,
            )
            insert_data_db = PythonOperator(
                task_id=f"insert_data_{file[:-4]}",
                python_callable=_insert_data_db,
                op_kwargs={"file_name": file[:-4]},
                provide_context=True
            )
            move_file = PythonOperator(
                task_id=f"move_file_{file[:-4]}",
                python_callable=_move_file,
                op_kwargs={"file_name": file},
            )
            read_file >> create_task_table >> insert_data_db >> move_file

    check_csv_files_in_directory = PythonOperator(
        task_id="check_csv_files_in_directory",
        python_callable=_check_csv_files_in_directory,
    )

    check_csv_files_in_directory >> process_files

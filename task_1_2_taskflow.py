from airflow.decorators import dag,task
from airflow.models import Variable
from airflow.sensors.filesystem import FileSensor
import pandas as pd
from datetime import datetime
import shutil
import os
from airflow.operators.python import get_current_context

from airflow.hooks.postgres_hook import PostgresHook
from airflow.sensors.base import PokeReturnValue
from airflow.exceptions import AirflowFailException
from airflow.operators.python import BranchPythonOperator

file_path_in = Variable.get("file_path") + "/in"
file_path_out = Variable.get("file_path") + "/out"
file_path_error = Variable.get("file_path") + "/error"
# ---------------------------------------------------

@dag("task_1_2_taskflow",
    start_date=datetime(2023,2,10),
    schedule_interval="*/2 * * * *",
    catchup=False,
    max_active_runs=1)
def createDag():
    #   File Sensor
    file_path = file_path_in + '/*'
    wait_for_file = FileSensor(
    task_id='wait_for_file',
    filepath=file_path,
    poke_interval=10,
    fs_conn_id = "task_1_file",
    timeout=30,
    soft_fail=True
)
    # ---------------------------------------------------
    @task
    def get_file():
        files = os.listdir(file_path_in)
        print(files)
        if len(files) > 0:
            return files     
    # ---------------------------------------------------
    @task
    def validate_file(files):
        errors = []
        has_spaces = lambda my_string: any(c.isspace() for c in my_string)
        file = files[0]
        if(not file.endswith('.csv')) :
            errors.append("This file type is not valid,not a csv")     
        elif not file.startswith("users") and not has_spaces(file):
            errors.append("This file name is invalid,doesn't starts with users or there is a space")
        context = get_current_context()
        task_instance = context["ti"]
        task_instance.xcom_push(key='file',value=file)
        
        if(len(errors) > 0):
            for error in errors:
                raise AirflowFailException(error)

        
    
    # -------------------------------------------------
    @task(trigger_rule = "all_failed")
    def handle_error():
        context = get_current_context()
        task_instance = context["ti"]
        file = task_instance.xcom_pull(task_ids="validate_file",key="file")
        shutil.move(file_path_in+'/'+file, file_path_error+'/'+file)
    # -------------------------------------------------
    @task(trigger_rule = "all_success")
    def read_file():
        context = get_current_context()
        task_instance = context["ti"]
        file = task_instance.xcom_pull(task_ids="validate_file",key="file")
        data_frame = pd.read_csv(file_path_in + '/'+ file)
        return data_frame.to_json(orient="split")
    # -------------------------------------------------
    @task
    def insert_data_db():  
        context = get_current_context()
        task_instance = context["ti"]
        data_json = task_instance.xcom_pull(task_ids="read_file")
        data_frame = pd.read_json(data_json, orient="split")
        # Create a PostgresHook to connect to the database.
        pg_hook = PostgresHook(postgres_conn_id='task_1_db')

        # Iterate over each row of the DataFrame and insert the data into the database.
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
    
        all_users_sql ="SELECT * FROM users;"
        cursor.execute(all_users_sql)
        all_users = cursor.fetchall()

        # Create dictionary where keys are IDs and values are tuples
        all_users_dict = {}
        for user in all_users:
            all_users_dict [user[0]] = user
            
        for _, row in data_frame.iterrows():
            user_existed = all_users_dict.get(row['id']) != None
            if user_existed :
                update_sql = """
                UPDATE users
                SET email = %s, date = %s, role = %s
                WHERE id = %s
                """
                values = (row['email'], row['date'], row['role'],row['id'])
                cursor.execute(update_sql, values)
            else :
                insert_sql = """
                    INSERT INTO users (id, email, date, role)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """
                values = (row['id'],row['email'], row['date'], row['role'])
                cursor.execute(insert_sql, values)
            
        conn.commit()
        cursor.close()
        conn.close()
    # -------------------------------------------------
    @task
    def move_file(): 
        context = get_current_context()
        task_instance = context["ti"]
        file = task_instance.xcom_pull(task_ids="validate_file",key="file")
        shutil.move(file_path_in+'/'+file, file_path_out+'/'+file)

    wait_for_file_instance = wait_for_file
    get_file_instance = get_file()
    validate_file_instance = validate_file(get_file_instance)
    handle_error_instance = handle_error()
    read_file_instance = read_file()
    insert_data_db_instance = insert_data_db()
    move_file_instance = move_file()

    wait_for_file_instance >> get_file_instance >> validate_file_instance >> [handle_error_instance,read_file_instance]
    read_file_instance >> insert_data_db_instance >> move_file_instance

dag_instance = createDag()
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

from psycopg2.extensions import connection as _connection
import json

file_path_in = Variable.get("file_path") + "/in"
file_path_out = Variable.get("file_path") + "/out"
file_path_error = Variable.get("file_path") + "/error"

def serialize_connection(conn: _connection) -> dict:
    return {
        "conn" : conn
    }
# ---------------------------------------------------

@dag("task_1_3",
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
    def insert_users_db():
        context = get_current_context()
        task_instance = context["ti"]
        pg_hook = PostgresHook(postgres_conn_id='task_2_db')
        conn = pg_hook.get_conn()
        data_json = task_instance.xcom_pull(task_ids="split_data",key="data_user")
        data_frame = pd.read_json(data_json, orient="split")
        cursor = conn.cursor()
    
        all_users_sql ="SELECT * FROM user;"
        cursor.execute(all_users_sql)
        all_users = cursor.fetchall()

        # Create dictionary where keys are IDs and values are tuples
        all_users_dict = {}
        for user in all_users:
            all_users_dict [user[0]] = user
            
        for _, row in data_frame.iterrows():
            user_existed = all_users_dict.get(row['id_personne']) != None
            if user_existed :
                update_sql = """
                UPDATE users
                SET  nom = %s, prenom = %s,email = %s
                WHERE id = %s
                """
                values = (row['nom'], row['prenom'], row['email'],row['id_personne'])
                cursor.execute(update_sql, values)
            else :
                insert_sql = """
                    INSERT INTO users (id_personne,nom,prenom,email)
                    VALUES (%s, %s, %s, %s)
                """
                values = (row['id_personne'], row['nom'], row['prenom'], row['email'])
                cursor.execute(insert_sql, values)
            
        conn.commit()
        cursor.close()
    @task
    def insert_entite_juridique_db():
        context = get_current_context()
        task_instance = context["ti"]
        pg_hook = PostgresHook(postgres_conn_id='task_2_db')
        conn = pg_hook.get_conn()
        data_json = task_instance.xcom_pull(task_ids="split_data",key="data_entite_juridique")
        data_frame = pd.read_json(data_json, orient="split")
        cursor = conn.cursor()
    
        all_users_sql ="SELECT * FROM entite_juridique;"
        cursor.execute(all_users_sql)
        all_users = cursor.fetchall()

        # Create dictionary where keys are IDs and values are tuples
        all_users_dict = {}
        for user in all_users:
            all_users_dict [user[0]] = user
            
        for _, row in data_frame.iterrows():
            user_existed = all_users_dict.get(row['code_ej']) != None
            if user_existed :
                update_sql = """
                UPDATE entite_juridique
                SET lib_ej = %s
                WHERE code_ej = %s
                """
                values = (row['lib_ej'],row['code_ej'])
                cursor.execute(update_sql, values)
            else :
                insert_sql = """
                    INSERT INTO entite_juridique (code_ej,lib_ej)
                    VALUES (%s, %s)
                """
                values = (row['code_ej'], row['lib_ej'])
                cursor.execute(insert_sql, values)
            
        conn.commit()
        cursor.close()
    @task
    def insert_bannette_db():
        context = get_current_context()
        task_instance = context["ti"]
        pg_hook = PostgresHook(postgres_conn_id='task_2_db')
        conn = pg_hook.get_conn()
        data_json = task_instance.xcom_pull(task_ids="split_data",key="data_bannette")
        data_frame = pd.read_json(data_json, orient="split")
        cursor = conn.cursor()
    
        all_users_sql ="SELECT * FROM bannette;"
        cursor.execute(all_users_sql)
        all_users = cursor.fetchall()

        # Create dictionary where keys are IDs and values are tuples
        all_users_dict = {}
        for user in all_users:
            all_users_dict [user[0]] = user
            
        for _, row in data_frame.iterrows():
            user_existed = all_users_dict.get(row['code_bannette']) != None
            if user_existed :
                update_sql = """
                UPDATE bannette
                SET lib_bannette = %s
                WHERE code_bannette = %s
                """
                values = (row['lib_bannette'],row['code_bannette'])
                cursor.execute(update_sql, values)
            else :
                insert_sql = """
                    INSERT INTO bannette (code_bannette,lib_bannette)
                    VALUES (%s, %s)
                """
                values = (row['code_bannette'], row['lib_bannette'])
                cursor.execute(insert_sql, values)
            
        conn.commit()
        cursor.close()
    @task
    def split_data():  
        context = get_current_context()
        task_instance = context["ti"]
        data_json = task_instance.xcom_pull(task_ids="read_file")
        data_frame = pd.read_json(data_json, orient="split")

        data_user = data_frame[['id_personne','nom','prenom','email']]
        data_entite_juridique = data_frame[['code_ej','lib_ej']]
        data_bannette = data_frame[['code_bannette','lib_bannette']]

        task_instance.xcom_push(key='data_user',value=data_user.to_json(orient="split"))
        task_instance.xcom_push(key='data_entite_juridique',value=data_entite_juridique.to_json(orient="split"))
        task_instance.xcom_push(key='data_bannette',value=data_bannette.to_json(orient="split"))

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
    split_data_instance = split_data()
    insert_users_db_instance = insert_users_db()
    insert_entite_juridique_db_instance = insert_entite_juridique_db()
    insert_bannette_db_instance = insert_bannette_db()
    move_file_instance = move_file()

    wait_for_file_instance >> get_file_instance >> validate_file_instance >> [handle_error_instance,read_file_instance]
    read_file_instance >> split_data_instance
    split_data_instance >> insert_users_db_instance  >> move_file_instance
    split_data_instance >> insert_entite_juridique_db_instance >> move_file_instance
    split_data_instance >> insert_bannette_db_instance >> move_file_instance
     
dag_instance = createDag()
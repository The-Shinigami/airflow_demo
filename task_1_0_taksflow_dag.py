from airflow.decorators import dag,task
from datetime import datetime
import os
from airflow.models import Variable
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
file_path_in = Variable.get("file_path") + "/in"

@dag("task_1_0_taskflow",
    start_date=datetime(2023,2,10),
    schedule_interval="*/2 * * * *",
    catchup=False)
def createDag():
    @task
    def get_file(): 
        files = os.listdir(file_path_in)
        if len(files) > 0:
            return files

    @task
    def validate_file(files):
        errors = []
        has_spaces = lambda my_string: any(c.isspace() for c in my_string)
        file = files[0]
        if(not file.endswith('.csv')) :
            errors.append("This file type is not valid,not a csv")     
        elif not file.startswith("users") and not has_spaces(file):
            errors.append("This file name is invalid,doesn't starts with users or there is a space")
        
        task_instance.xcom_push(key="file",value=file)

        if(len(errors) > 0):
            for error in errors:
                raise AirflowFailException(error)
            return "handle_error"
        else:
            return "read_file"   
    
    @task
    def read_file(csv_file):
        data_frame = pd.read_csv(file_path_in+'/'+csv_file)
        return data_frame.to_json(orient="split")

    @task
    def insert_data_db(data_json):
        data_frame = pd.read_json(data_json, orient="split")
        # Create a PostgresHook to connect to the database.
        pg_hook = PostgresHook(postgres_conn_id='task_1_db')

        # Iterate over each row of the DataFrame and insert the data into the database.
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
    
        all_users_sql ="SELECT * FROM task;"
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
                UPDATE task
                SET email = %s, date = %s, role = %s
                WHERE id = %s
                """
                values = (row['email'], row['date'], row['role'],row['id'])
                cursor.execute(update_sql, values)
            else :
                insert_sql = """
                    INSERT INTO task (id, email, date, role)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """
                values = (row['id'],row['email'], row['date'], row['role'])
                cursor.execute(insert_sql, values)
            
        conn.commit()
        cursor.close()
        conn.close()


    get_file_instance = get_file() 
    read_file_instance = read_file(get_file_instance)
    insert_data_db_instance = insert_data_db(read_file_instance)

    read_file_instance >> insert_data_db_instance

my_dag = createDag()
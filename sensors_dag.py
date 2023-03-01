from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'wait_for_file',
    default_args=default_args,
    description='Example DAG to wait for a file',
    schedule_interval=None
)

file_path = Variable.get("file_path")+'/in/*'

wait_for_file = FileSensor(
    task_id='wait_for_file',
    filepath=file_path,
    poke_interval=30,
    fs_conn_id = "task_1_file",
    dag=dag
)

process_file = BashOperator(
    task_id='process_file',
    bash_command='echo "Processing file..."',
    dag=dag
)

# Define a Python callable to get the list of files
def get_files(ti):
    files = ti.xcom_pull(task_ids='wait_for_file')
    return files

# Define a Python callable to print the list of files
def print_files(files):
    print("Files found:", files)

# Define a Python callable to store the list of files in an XCom variable
def store_files(ti):
    files = ti.xcom_pull(task_ids='wait_for_file')
    ti.xcom_push(key='files', value=files)

# Use the set_downstream method to specify the order of tasks
wait_for_file >> process_file

# Use the set_upstream method to specify the dependencies of the PythonOperator tasks
store_files_task = PythonOperator(
    task_id='store_files',
    python_callable=store_files,
    provide_context=True,
    dag=dag
)

get_files_task = PythonOperator(
    task_id='get_files',
    python_callable=get_files,
    provide_context=True,
    dag=dag
)

print_files_task = PythonOperator(
    task_id='print_files',
    python_callable=print_files,
    op_args=[get_files_task.output],
    dag=dag
)

wait_for_file >> store_files_task >> get_files_task >> print_files_task


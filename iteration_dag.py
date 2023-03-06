
from airflow.models import Variable
from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task

@task
def read_csv_chunk(file_path, start_index, chunk_size):
    df = pd.read_csv(file_path, skiprows=start_index, nrows=chunk_size)
    return df.values.tolist()

@task
def print_chunk(chunk):
    for row in chunk:
        print(row)

def create_taskflow(file_path, chunk_size):
    df =pd.DataFrame()
    try:
        df = pd.read_csv(file_path, nrows=1)  # Read the first row to get the column names
        column_names = df.columns.tolist()

        num_rows = sum(1 for _ in open(file_path)) - 1  # Count the number of rows (excluding the header row)
        num_chunks = (num_rows + chunk_size - 1) // chunk_size  # Calculate the number of chunks

        with TaskGroup("read_csv_task_group") as read_csv_task_group:
            for i in range(0, num_rows, chunk_size):
                chunk = read_csv_chunk(file_path, i, chunk_size)
                print_chunk(chunk)

        return read_csv_task_group
    except:
        print("error")
    

with DAG(
    'read_csv_and_print_rows_taskflow',
    description='DAG to read a CSV file and print each row to the console using TaskFlow API',
    schedule_interval=None,
    start_date=datetime(2023, 3, 2),
) :

    file_path_in = Variable.get("file_path") + "/in"
    read_csv_taskflow = create_taskflow(file_path_in + '/users_data.csv', 2)

    read_csv_taskflow

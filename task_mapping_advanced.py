from airflow.decorators import dag,task
from datetime import datetime

@task
def make_list():
    return [1,2,3,4]


@task
def consumer(arg):    
    print(arg)

@task
def add_one(arg):
    if(arg == 3):
       raise ValueError("the value is 3 ")
    return arg+1

@task
def sub_two(arg):
    return arg-2

@task(trigger_rule="one_failed")
def its_failed(arg):
    print("this task with arg %s is failed",arg)


@dag(dag_id="dynamic-task-map", 
     start_date=datetime(2022, 4, 2),
     catchup=False)
def create_dag():
    make_list_instance = make_list()
    add_one_to_list = add_one.expand(arg=make_list_instance)
    sub_two_to_list = sub_two.expand(arg=add_one_to_list)
    its_failed.expand(arg=sub_two_to_list)
    consumer.expand(arg=sub_two_to_list)

dag_instance = create_dag()   

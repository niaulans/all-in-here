from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello World! My name is {first_name} {last_name}, and I'm {age} years old.")
    
def get_name(ti):
    ti.xcom_push(key='first_name', value='Nia Ulan')
    ti.xcom_push(key='last_name', value='Sari')
    
def get_age(ti):
    ti.xcom_push(key='age', value=25)
    
default_args = {
    'owner' : 'niaulans',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

with DAG (
    dag_id='our_dag_with_python_operator_v06',
    default_args=default_args,
    description='Our first dag using python operator',
    start_date=datetime(2025, 3, 12),
    schedule_interval=None
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        # op_kwargs={'age': 25}
    )
    
    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )
    
    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )
    
    [task2, task3] >> task1




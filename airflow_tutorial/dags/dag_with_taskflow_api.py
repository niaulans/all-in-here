from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    'owner' : 'niaulans',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

@dag(dag_id='dag_with_taskflow_api_v02', 
     default_args=default_args, 
     start_date=datetime(2025, 3, 13),
     schedule_interval=None
     )

def hello_world_etl():
    
    # @task()
    # def get_name():
    #     return 'Nia'
    
    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'Nia',
            'last_name' : 'Ulan Sari'
        }
    
    @task()
    def get_age():
        return 25

    @task()
    def greet(first_name, last_name, age):
        print(f"Hello world! My name is {first_name} {last_name}, and I am {age} years old")

    # name = get_name()
    name_dict= get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'], 
          last_name=name_dict['last_name'], 
          age=age)
    
greet_dag = hello_world_etl()
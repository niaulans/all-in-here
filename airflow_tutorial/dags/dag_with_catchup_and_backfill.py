from datetime import datetime, timedelta
# from pytz import timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

# local_tz = timezone("Asia/Jakarta") 
# start_date = local_tz.localize(datetime(2025, 3, 10, 14, 0)).astimezone(timezone("UTC"))

default_args = {
    'owner': 'niaulans',
    'retries': 5,
    'retry_delay': timedelta(minutes=5) 
}

with DAG(
    dag_id='dag_with_catchup_and_backfill_v02',
    default_args=default_args,
    start_date=datetime(2025, 3, 10),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo This is a simple bash command'
    )






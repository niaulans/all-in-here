from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    'owner': 'niaulans',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG (
    dag_id='dag_with_aws_v01',
    default_args=default_args,
    start_date=datetime(2025, 3, 17),
    schedule_interval='@daily'
) as dag:
    task1 = S3KeySensor(
        task_id='sensor_aws_s3_v01',
        bucket_key='data.csv',
        bucket_name='learn-airflowww',  
        aws_conn_id='aws_s3_conn',
        mode='poke',
        poke_interval=5,
        timeout=30
    )


from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

schedule = timedelta(minutes=5)
args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
}
s3dag = DAG(
    dag_id='s3_sensor',
    schedule_interval=schedule,
    default_args=args,
    concurrency=1,
    max_active_runs=1
)


def new_file_detection(**kwargs):
    print("A new file has arrived in s3 bucket")


file_sensor = S3KeySensor(
    task_id='s3_key_sensor_task',
    poke_interval=10,  # (seconds); checking file every half an hour
    timeout=60 * 5,  # timeout in 12 hours
    bucket_key="s3://gpongracz/hello1.csv",
    bucket_name=None,
    wildcard_match=False,
    dag=s3dag)

print_message = PythonOperator(task_id='print_message',
                               provide_context=True,
                               python_callable=new_file_detection,
                               dag=s3dag)

file_sensor >> print_message
from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from src.kafka_consumer_to_s3 import consumer_to_s3

default_args = {
    'owner': 'fora22',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    # 'start_date': datetime(2022,4,26),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

dagName = 'consumer-dag'
with DAG(
    dagName,
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='*/15 * * * *',
    tags=['fora'],
    ) as dag:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = PythonOperator(
        task_id='consumer',
        python_callable=consumer_to_s3,
        dag=dag
    )

    complete = BashOperator(
        task_id='complete',
        depends_on_past=False,
        bash_command='echo "consumer complete~!"',
        trigger_rule=TriggerRule.NONE_FAILED,
        dag=dag
    )


    t1 >> complete
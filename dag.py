import datetime
import pendulum

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

import sys
sys.path.insert(0, 'path_to_email_report.py')
from email_report import EmailReport

def send_report(**context):
    report = EmailReport(context=context)
    report.send_email_report()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    dag_id='dummy_test_dag',
    schedule_interval=None,
    start_date=pendulum.datetime(2023, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=360000),
    default_args=default_args,
) as dag:

    start_task = DummyOperator(
        task_id='start_test',
        dag=dag
    )

    test_task = DummyOperator(
        task_id='dummy',
        dag=dag,
    )

    email_report = PythonOperator(
        task_id='email_report',
        python_callable=send_report,
        provide_context=True,
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Task dependencies
    start_task >> dummy >> email_report

if __name__ == "__main__":
    dag.cli()

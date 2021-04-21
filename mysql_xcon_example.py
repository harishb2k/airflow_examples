from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
import logging
from airflow.operators.mysql_operator import MySqlOperator as BaseMySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from custom.mysql_operators import ReturningMySqlOperator
import os

default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'MySQL_XCon_Example_Extract_Data_From_Table',
    default_args=default_args,
    start_date=days_ago(2),
    tags=['mysql', 'xcom'],
    schedule_interval="* * * * *"
)

t0 = DummyOperator(task_id='start')


mysql_task = ReturningMySqlOperator(
    task_id='mysql_select',
    mysql_conn_id='mysql_harish',
    sql='select * from users',
    dag=dag,
    params=default_args
)


def get_records(**kwargs):
    ti = kwargs['ti']
    xcom = ti.xcom_pull(task_ids='mysql_select')
    string_to_print = 'Data extractd from ReturningMySqlOperator: {}'.format(xcom)
    logging.info(string_to_print)
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }} and {{ string_to_print }}" >> ~/a.txt ',

t2 = PythonOperator(
    task_id='print_mysql_result',
    provide_context=True,
    python_callable=get_records,
    dag=dag)


def write_some_file():
    try:
        with open("/tmp/{{ job_id }}.txt", "wt") as fout:
            fout.write('test1\n')        
    except Exception as e:
        log.error(e)
        raise AirflowException(e)

write_file_task = PythonOperator(
    task_id='write_some_file',
    python_callable=write_some_file,
    dag=dag
) 

t0 >> mysql_task >> t2 >> write_file_task


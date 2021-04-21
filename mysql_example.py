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
from op.my import ReturningMySqlOperator


default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'example_mysql',
    default_args=default_args,
    start_date=days_ago(2),
    tags=['example'],
    schedule_interval="* * * * *"
)

t0 = DummyOperator(
        task_id='start'
    )




mysql_task = ReturningMySqlOperator(
    task_id='select',
    mysql_conn_id='mysql_harish',
    sql='select * from users',
    dag=dag,
)


def get_records(**kwargs):
    ti = kwargs['ti']
    xcom = ti.xcom_pull(task_ids='select')
    string_to_print = 'Value in xcom is: {}'.format(xcom)
    logging.info(string_to_print)
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }} and {{ string_to_print }}" >> ~/a.txt ',

t2 = PythonOperator(
    task_id='records',
    provide_context=True,
    python_callable=get_records,
    dag=dag)


t0 >> mysql_task >> t2


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
from airflow.utils.decorators import apply_defaults

class ReturningMySqlOperator(BaseMySqlOperator):

	@apply_defaults
	def __init__(
		self, 
    	params: dict, 
    	**kwargs) -> None:
		super().__init__(**kwargs)
		self.params = params

	def execute(self, context):
	    self.log.info('Executing: %s', self.sql)
	    hook = MySqlHook(mysql_conn_id=self.mysql_conn_id, schema=self.database)
	    result = hook.get_first(self.sql)
	    self.params['r'] = result
	    return result
    
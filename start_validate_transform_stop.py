"""
### Example HTTP and Python
"""
from airflow import models
from airflow import DAG
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.operators import SimpleHttpOperator
import datetime
import json

default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2020, 1, 1),
    'retries': 0
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
       'start_validate_transform_stop',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    def start():
        import logging
        logging.info('STAGE-START:Start of a great data pipeline')

    def stop(**kwargs):
        import logging
        logging.info('STAGE-STOP:End of a great data pipelin')
        ti = kwargs['ti']
        returnval = ti.xcom_pull(task_ids='transform')
        logging.info(returnval)


    def transform(**kwargs):
        import logging
        ti = kwargs['ti']
        logging.info('STAGE-TRANSFORM:'+ti.xcom_pull(task_ids='validate'))

    start_python = python_operator.PythonOperator(
        task_id='start',
        python_callable=start)

    stop_python = python_operator.PythonOperator(
        task_id='stop',
        provide_context=True,
        python_callable=stop)

    validate_http = SimpleHttpOperator(
        task_id='validate',
        method='POST',
        http_conn_id='gcp_gcf',
        endpoint='',
        data=json.dumps({"tablename": "customers"}),
        xcom_push=True,
        provide_context=True,
        headers={"Content-Type": "application/json"})

    transform_python = python_operator.PythonOperator(
        task_id='transform',
        provide_context=True,
        python_callable=transform)

    # DAG execution sequence
    start_python >> validate_http >> transform_python >> stop_python

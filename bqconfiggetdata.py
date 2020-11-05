"""
### Example HTTP and Python
"""
from airflow import models
from airflow import DAG
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.operators import SimpleHttpOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
    BigQueryGetDataOperator
)

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

DATA_SAMPLE_GCS_BUCKET_NAME="dataengpa"
DATA_SAMPLE_GCS_OBJECT_NAME="deniro.csv"
DATASET_NAME='pa'
SOURCE_BUCKET='dataengpa'
SRC1='deniro.csv'
CONFIG_TABLE_NAME='config'

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
    'read_config_frombq_a',
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args) as dag:
    import logging


    ##############################################################
    # Use this to fetch the data from BQ config data             #
    ##############################################################
    get_data_bq = BigQueryGetDataOperator(
        task_id="get_config",
        dataset_id=DATASET_NAME,
        table_id=CONFIG_TABLE_NAME,
        max_results=1,
        selected_fields="schema_nm, table_nm, file_name, path, data_topic_nm"
    )


    ##############################################################
    # The code below demonstrates how to use  configdata returned#
    # in the getdata                                             #
    ##############################################################

    def formatConfig(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='get_config')
        configdict = {}
        configdict['CVS_CAREMARK_MEMBER'] = {}
        for row in data:
            configdict['CVS_CAREMARK_MEMBER']['schema_nm'] = row[0]
            configdict['CVS_CAREMARK_MEMBER']['table_nm'] = row[1]
            configdict['CVS_CAREMARK_MEMBER']['file_name'] = row[2]
            configdict['CVS_CAREMARK_MEMBER']['path'] = row[3]
            configdict['CVS_CAREMARK_MEMBER']['data_topic_nm'] = row[4]

        ti.xcom_push(key="val1", value=configdict)

    format_python = python_operator.PythonOperator(
        task_id='format',
        provide_context=True,
        python_callable=formatConfig)

   
    ##############################################################
    # The code below demonstrates fetching of config             #
    # You use this code in any task to access the config you have#
    # This line                                                  #
    # data = ti.xcom_pull(key=,value=)                           #   
    # Can be used by downstream tasks or to be kept variable     # 
    ##############################################################
    def fetchConfig(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(key="val1", task_ids='format')
        print("YOYOYOYO")
        print(data)
        ti.xcom_push(key="val2", value=data)


    fetch_python = python_operator.PythonOperator(
        task_id='fetch_config',
        provide_context=True,
        python_callable=fetchConfig)

    get_data_bq >> format_python >> fetch_python
    

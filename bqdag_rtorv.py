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
    BigQueryValueCheckOperator
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
TABLE_NAME='deniro_tmp_jagged'

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
    'deniro_raw_to_rawvieable',
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args) as dag:
    ##############################################################
    # Create temporary table with the file loaded into gcs as it #
    ##############################################################

        loadGcsToBq = GCSToBigQueryOperator(
          task_id='gcstobq_jagged',
          bucket=SOURCE_BUCKET,
          source_objects=[SRC1],
          destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
          create_disposition='CREATE_IF_NEEDED',
          source_format='CSV',
          write_disposition='WRITE_TRUNCATE',
          allow_jagged_rows=True,
          skip_leading_rows=1,
          schema_fields=[
	  {
	    "mode": "NULLABLE",
	    "name": "year",
	    "type": "INTEGER"
	  },
	  {
	    "mode": "NULLABLE",
	    "name": "score",
	    "type": "INTEGER"
	  },
	  {
	    "mode": "NULLABLE",
	    "name": "title",
	    "type": "STRING"
	  },
	  {
	    "mode": "NULLABLE",
	    "name": "tbd1",
	    "type": "STRING"
          }, 
	  {
	    "mode": "NULLABLE",
	    "name": "tbd2",
	    "type": "STRING"
          }, 
          ]
        )

    ##############################################################
    # Check mandatory columns for null values                    #
    # This function sets up a check to see if the numbber of null#
    # values allowed in the title column to be 1                 # 
    ##############################################################
        validateTitleNull = BigQueryValueCheckOperator(
          task_id="validateNullTitle",
          sql=f"SELECT COUNT(*) FROM {DATASET_NAME}.{TABLE_NAME} WHERE title IS NULL",
          pass_value=1,
          use_legacy_sql=False
        )


    ##############################################################
    # Update row to set tbd1 with uppercase value of title       #
    ##############################################################
        UPDATE_STATEMENT = f"""UPDATE `{DATASET_NAME}.{TABLE_NAME}` SET tbd1 = LOWER(title) WHERE TRUE""" 
        
        updateTableColumns = BigQueryInsertJobOperator(
          task_id="select_query_job",
          configuration={
            "query": {
              "query": UPDATE_STATEMENT,
              "useLegacySql": False,
            }
          }
        )

        loadGcsToBq >> validateTitleNull >> updateTableColumns

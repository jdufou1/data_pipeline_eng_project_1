from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

from utils import *

from google.cloud import storage
import snowflake.connector
import pendulum
import os
import pandas as pd
import json
import numpy as np
import uuid
import time

default_args = {
    "owner": "jdufou1",
    "retries": 5,
    "retry_delai": timedelta(minutes=2)
}

start_date = datetime.now()

with DAG(
    dag_id="dag_pipeline_v1",
    default_args=default_args,
    description="send flashscore data from GCS to Snowflake",
    start_date= pendulum.datetime(2023, 1, 1, tz="UTC"), # pendulum.datetime(2023, 11, 18, tz="UTC"), # start_date,
    catchup=False,
    schedule_interval="0 */8 * * *" # 
) as dag :
    
    starting_task = BashOperator(
        task_id="starting_task",
        bash_command="echo Starting task"
    )

    @task()
    def task_daily_file_process():
        files_published_today = get_files_published_today(BUCKET_NAME)
        ETL_pipeline(files_published_today)

    ending_task = BashOperator(
        task_id="ending_task",
        bash_command="echo Ending task"
    )

    starting_task >> task_daily_file_process() >> ending_task
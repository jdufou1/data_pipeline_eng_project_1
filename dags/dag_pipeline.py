from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

from etl_functions import ETL_pipeline
from google.cloud import storage

import yaml

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
    start_date=start_date,
    schedule_interval="50 23 * * *"
) as dag :
    
    starting_task = BashOperator(
        task_id="starting_task",
        bash_command="echo Starting task"
    )

    @task()
    def task_daily_file_process():
        
        

        PATH_CREDENTIALS_GLOBAL = "./dags/credentials/credentials.yml"
        PATH_CREDENTIALS_GCLOUD = "./dags/credentials/credentials-google-cloud.json"

        with open(PATH_CREDENTIALS_GLOBAL, "r") as fichier:
            credentials = yaml.safe_load(fichier)

        start_date = datetime.now()

        BUCKET_NAME = credentials["source_bucket_name"]


        def get_files_published_today(bucket_name, credentials_path):
            client = storage.Client.from_service_account_json(credentials_path)

            bucket = client.get_bucket(bucket_name)

            today = datetime.now().date()
            yesterday = today - timedelta(days=1)

            blobs_today_and_yesterday = [blob for blob in bucket.list_blobs()
                                        if blob.updated.date() == today or blob.updated.date() == yesterday]

            file_names_today_and_yesterday = [blob.name for blob in blobs_today_and_yesterday]

            return file_names_today_and_yesterday


        files_published_today = get_files_published_today(BUCKET_NAME, PATH_CREDENTIALS_GCLOUD)
        ETL_pipeline(files_published_today)



    ending_task = BashOperator(
        task_id="ending_task",
        bash_command="echo Ending task"
    )

    starting_task >> task_daily_file_process() >> ending_task

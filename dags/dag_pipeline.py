from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

from etl_functions import ETL_pipeline
from google.cloud import storage

import yaml

import os

import requests


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
        
        # Obtenez le chemin du répertoire courant
        current_directory = os.path.dirname(os.path.abspath(__file__))
        

        # PATH_CREDENTIALS_GLOBAL = os.path.join(current_directory, "credentials", "credentials.yml")
        # PATH_CREDENTIALS_GCLOUD = os.path.join(current_directory, "credentials", "credentials-google-cloud.json") # "./dags/credentials/credentials-google-cloud.json"

        response = requests.get("https://storage.cloud.google.com/europe-west6-airflow-data-e-f3099903-bucket/dags/credentials/credentials.yml")

        if response.status_code == 200:
            # Chargez les informations depuis le contenu téléchargé
            credentials = yaml.safe_load(response.text)
        else:
            print(f"Erreur lors du téléchargement du fichier credentials depuis l'URL. Code d'état : {response.status_code}")

        start_date = datetime.now()

        BUCKET_NAME = credentials["source_bucket_name"]


        def get_files_published_today(bucket_name):
            response = requests.get("https://storage.cloud.google.com/europe-west6-airflow-data-e-f3099903-bucket/dags/credentials/credentials-google-cloud.json")
            if response.status_code == 200:
                # Initialisez le client de stockage avec les informations téléchargées
                credentials = response.json()
                client = storage.Client.from_service_account_info(credentials)

                # Maintenant, vous pouvez utiliser le client pour effectuer des opérations sur le stockage
                bucket = client.get_bucket('nom_du_seau')
                blobs = bucket.list_blobs()

                for blob in blobs:
                    print(blob.name)
            else:
                print(f"Erreur lors du téléchargement du fichier d'informations d'identification depuis l'URL. Code d'état : {response.status_code}")

            bucket = client.get_bucket(bucket_name)

            today = datetime.now().date()
            yesterday = today - timedelta(days=1)

            blobs_today_and_yesterday = [blob for blob in bucket.list_blobs()
                                        if blob.updated.date() == today or blob.updated.date() == yesterday]

            file_names_today_and_yesterday = [blob.name for blob in blobs_today_and_yesterday]

            return file_names_today_and_yesterday


        files_published_today = get_files_published_today(BUCKET_NAME)
        ETL_pipeline(files_published_today)



    ending_task = BashOperator(
        task_id="ending_task",
        bash_command="echo Ending task"
    )

    starting_task >> task_daily_file_process() >> ending_task

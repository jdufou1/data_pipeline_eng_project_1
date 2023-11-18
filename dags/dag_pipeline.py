from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

from google.cloud import storage
import snowflake.connector
import pendulum
import os
import pandas as pd
import json

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
    schedule_interval="0 * * * *"# "50 23 * * *"
) as dag :
    
    starting_task = BashOperator(
        task_id="starting_task",
        bash_command="echo Starting task"
    )

    @task()
    def task_daily_file_process():

        
        BUCKET_NAME = "data-flashscore"
        # ID_PROJECT = "primeval-argon-358717"
        # SnowFlake

        ORGNAME = "QXGHBSB"
        ACCOUNT_NAME = "EF08275"

        ACCOUNT_IDENTIFIER = f"{ORGNAME}-{ACCOUNT_NAME}"
        PASSWORD = "Jyde-7819020!"
        USER ="JDUFOU1"

        WH = "COMPUTE_WH"
        DB = "FLASH_SCORE_DB"

        NAME_TABLE = "MATCHES_FINISHED"

        def extract(file):
            return read_json_from_gcs(BUCKET_NAME, file)

        def transform_pandas(json_obj):
            # Convert JSON object to a string
            json_data = json.dumps(json_obj)

            # Load JSON data into a pandas DataFrame
            df = pd.read_json(json_data, orient='records')

            # Filter rows where 'current_status' is 'Finished'
            df = df[df['current_status'] == 'Finished']

            # Drop columns
            columns_to_drop = ['match_hour', 'current_status']
            df = df.drop(columns=columns_to_drop)

            # Drop rows with missing values
            df = df.dropna()

            # Convert specific columns to integer type
            df['participant_away_current_score'] = pd.to_numeric(df['participant_away_current_score'], errors='coerce')
            df['participant_home_current_score'] = pd.to_numeric(df['participant_home_current_score'], errors='coerce')

            # Reorder columns
            desired_column_order = ["current_country", "current_tournament", "participant_home", "participant_home_current_score", "participant_away", "participant_away_current_score"]
            df = df[desired_column_order]

            # Rename columns
            column_mapping = {"current_country": "COUNTRY_MATCH", "current_tournament": "TOURNAMENT", "participant_home": "NAME_TEAM_HOME",
                            "participant_home_current_score": "SCORE_TEAM_HOME", "participant_away": "NAME_TEAM_AWAY",
                            "participant_away_current_score": "SCORE_TEAM_AWAY"}

            df = df.rename(columns=column_mapping)

            return df

        def load(df):

            # Créer une connexion à Snowflake
            conn=snowflake.connector.connect(
                account=ACCOUNT_IDENTIFIER,
                user=USER,
                password=PASSWORD,
                warehouse = WH,
                database = DB,
                
            )
            # Créer un curseur
            cur = conn.cursor()

            # Requête d'insertion
            insert_query = f"INSERT INTO {NAME_TABLE} (COUNTRY_MATCH, TOURNAMENT, NAME_TEAM_HOME, SCORE_TEAM_HOME, NAME_TEAM_AWAY, SCORE_TEAM_AWAY) VALUES (%s, %s, %s, %s, %s, %s)"


            data_to_insert = [tuple(row) for row in df.to_numpy()]# .toPandas().to_dict(orient='records')

            # Exécuter la requête d'insertion pour chaque ligne de données
            cur.executemany(insert_query, data_to_insert) # [row for row in data_to_insert])

            # Valider les changements
            conn.commit()

            # Fermer le curseur et la connexion
            cur.close()
            conn.close()
            print("fermeture")
            
        def ETL_pipeline(list_files):
            bucket_source = "data-flashscore"
            bucket_destination = "data-flashscore-used"
            for file in list_files:
                # Appel de la fonction
                json_obj = extract(file)
                move_file(bucket_source, file, bucket_destination, file)
                df = transform_pandas(json_obj)
                load(df)

        def get_files_published_today(bucket_name):
            credentials_json_str = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
            # Vérifiez si la variable d'environnement est définie
            if credentials_json_str is None:
                raise ValueError("La variable d'environnement GOOGLE_APPLICATION_CREDENTIALS_JSON n'est pas définie.")

            # Chargez la clé JSON depuis la chaîne JSON
            credentials = json.loads(credentials_json_str)

            client = storage.Client.from_service_account_info(credentials)

            bucket = client.get_bucket(bucket_name)

            today = datetime.now().date()
            yesterday = today - timedelta(days=1)

            # blobs_today_and_yesterday = [blob for blob in bucket.list_blobs()
            #                             if blob.updated.date() == today or blob.updated.date() == yesterday]

            file_names_today_and_yesterday = [blob.name for blob in bucket.list_blobs() ]# blobs_today_and_yesterday]

            return file_names_today_and_yesterday


        files_published_today = get_files_published_today(BUCKET_NAME)
        ETL_pipeline(files_published_today)

    ending_task = BashOperator(
        task_id="ending_task",
        bash_command="echo Ending task"
    )

    starting_task >> task_daily_file_process() >> ending_task





def read_json_from_gcs(bucket_name, file_path):
    credentials_json_str = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    # Vérifiez si la variable d'environnement est définie
    if credentials_json_str is None:
        raise ValueError("La variable d'environnement GOOGLE_APPLICATION_CREDENTIALS_JSON n'est pas définie.")

    # Chargez la clé JSON depuis la chaîne JSON
    credentials = json.loads(credentials_json_str)

    client = storage.Client.from_service_account_info(credentials)

    # Obtenez le seau
    bucket = client.get_bucket(bucket_name)

    # Obtenez l'objet (fichier) dans le seau
    blob = bucket.blob(file_path)

    # Téléchargez le contenu du fichier JSON
    json_content = blob.download_as_text()

    # Analysez le contenu JSON
    data = json.loads(json_content)

    return data


def move_file(source_bucket_name, source_object_name, destination_bucket_name, destination_object_name):
    credentials_json_str = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    # Vérifiez si la variable d'environnement est définie
    if credentials_json_str is None:
        raise ValueError("La variable d'environnement GOOGLE_APPLICATION_CREDENTIALS_JSON n'est pas définie.")

    # Chargez la clé JSON depuis la chaîne JSON
    credentials = json.loads(credentials_json_str)

    client = storage.Client.from_service_account_info(credentials)

    # Récupérer le seau source
    source_bucket = client.bucket(source_bucket_name)

    # Récupérer l'objet source
    source_blob = source_bucket.blob(source_object_name)

    # Récupérer le seau de destination
    destination_bucket = client.bucket(destination_bucket_name)

    # Copier l'objet vers le seau de destination
    source_bucket.copy_blob(
        source_blob, destination_bucket, destination_object_name
    )

    # Supprimer l'objet source après avoir été copié
    source_blob.delete()

    print(f"Fichier déplacé avec succès de {source_bucket_name}/{source_object_name} vers {destination_bucket_name}/{destination_object_name}")
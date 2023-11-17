from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

# from etl_functions import ETL_pipeline
from google.cloud import storage

import yaml

import requests

import pandas as pd

from google.cloud import storage
import json
import snowflake.connector
import yaml
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
        
        # PATH_CREDENTIALS_GLOBAL = os.path.join(current_directory, "credentials", "credentials.yml")
        # PATH_CREDENTIALS_GCLOUD = os.path.join(current_directory, "credentials", "credentials-google-cloud.json") # "./dags/credentials/credentials-google-cloud.json"

        response = requests.get("https://storage.cloud.google.com/europe-west6-airflow-data-e-f3099903-bucket/credentials/credentials.yml")

        if response.status_code == 200:
            # Chargez les informations depuis le contenu téléchargé
            credentials = yaml.safe_load(response.text)
        else:
            print(f"Erreur lors du téléchargement du fichier credentials depuis l'URL. Code d'état : {response.status_code}")


        
        BUCKET_NAME = credentials["source_bucket_name"]
        ID_PROJECT = credentials["id_project"]
        # SnowFlake

        ORGNAME = credentials["name_organisation"]
        ACCOUNT_NAME = credentials["name_account"]

        ACCOUNT_IDENTIFIER = f"{ORGNAME}-{ACCOUNT_NAME}"
        PASSWORD = credentials["password_user"]
        USER = credentials["id_user"]

        WH = credentials["name_warehouse"]
        DB = credentials["name_database"]

        NAME_TABLE = credentials["name_table"]










        def extract(file):
            return read_json_from_gcs(BUCKET_NAME, file)


        # def transform_spark(spark, json_obj):
        #     json_data = json.dumps(json_obj)

        #     # Charger le JSON dans un RDD
        #     rdd = spark.sparkContext.parallelize([json_data])

        #     # Convertir le RDD en DataFrame
        #     df = spark.read.json(rdd)


        #     # drop columns

        #     df = df.filter(df["current_status"] == "Finished")

        #     df_without_columns = df.drop("match_hour", "current_status")

        #     # drop lines with no values
        #     df_2 = df_without_columns.na.drop()

        #     df_2 = df_2.withColumn("participant_away_current_score", col("participant_away_current_score").cast("int"))
        #     df_2 = df_2.withColumn("participant_home_current_score", col("participant_home_current_score").cast("int"))

        #     desired_column_order = ["current_country", "current_tournament", "participant_home", "participant_home_current_score", "participant_away", "participant_away_current_score"]
        #     desired_column_name = ["COUNTRY_MATCH", "TOURNAMENT", "NAME_TEAM_HOME", "SCORE_TEAM_HOME", "NAME_TEAM_AWAY", "SCORE_TEAM_AWAY"]

        #     # Reorder the columns using the select method
        #     df_3 = df_2.select(*desired_column_order)

        #     column_mapping = dict(zip(desired_column_order, desired_column_name))

        #     # Rename all columns in the DataFrame
        #     df_renamed = df_3
        #     for old_col, new_col in column_mapping.items():
        #         df_renamed = df_renamed.withColumnRenamed(old_col, new_col)


        #     return df_renamed


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
            for file in list_files:
                # Appel de la fonction
                json_obj = extract(file)
                df = transform_pandas(json_obj)
                load(df)



















        def get_files_published_today(bucket_name):
            response = requests.get("https://storage.cloud.google.com/europe-west6-airflow-data-e-f3099903-bucket/credentials/credentials-google-cloud.json")
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





def read_json_from_gcs(bucket_name, file_path):
    # Créez une instance du client GCS avec les informations d'identification
    # Téléchargez le contenu du fichier depuis l'URL
    response = requests.get("https://storage.cloud.google.com/europe-west6-airflow-data-e-f3099903-bucket/credentials/credentials-google-cloud.json")

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

    # Obtenez le seau
    bucket = client.get_bucket(bucket_name)

    # Obtenez l'objet (fichier) dans le seau
    blob = bucket.blob(file_path)

    # Téléchargez le contenu du fichier JSON
    json_content = blob.download_as_text()

    # Analysez le contenu JSON
    data = json.loads(json_content)

    return data



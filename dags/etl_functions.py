import pandas as pd

from google.cloud import storage
import json
import snowflake.connector
import yaml
import os

# PATH_CREDENTIALS_GLOBAL = "./dags/credentials/credentials.yml"
# PATH_CREDENTIALS_GCLOUD = "./dags/credentials/credentials-google-cloud.json"
current_directory = os.path.dirname(os.path.abspath(__file__))
PATH_CREDENTIALS_GLOBAL = os.path.join(current_directory, "credentials", "credentials.yml")
PATH_CREDENTIALS_GCLOUD = os.path.join(current_directory, "credentials", "credentials-google-cloud.json")

with open(PATH_CREDENTIALS_GLOBAL, "r") as fichier:
    credentials = yaml.safe_load(fichier)

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



def read_json_from_gcs(bucket_name, file_path, credentials_path):
    # Créez une instance du client GCS avec les informations d'identification
    client = storage.Client.from_service_account_json(credentials_path)

    # Obtenez le seau
    bucket = client.get_bucket(bucket_name)

    # Obtenez l'objet (fichier) dans le seau
    blob = bucket.blob(file_path)

    # Téléchargez le contenu du fichier JSON
    json_content = blob.download_as_text()

    # Analysez le contenu JSON
    data = json.loads(json_content)

    return data


def extract(file):
    return read_json_from_gcs(BUCKET_NAME, file, PATH_CREDENTIALS_GCLOUD)


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


# from datetime import datetime, timedelta

# from google.cloud import storage

# start_date = datetime.now()

# ID_PROJECT = "primeval-argon-358717"
# PATH_CREDENTIALS = "../credentials/credentials-google-cloud.json"
# JSON_FILE_PATH = "08_11_2023-23_00_19.json"
# BUCKET_NAME = "data-flashscore"

# def get_files_published_today(bucket_name, credentials_path):
#     # Créez une instance du client GCS avec les informations d'identification
#     client = storage.Client.from_service_account_json(credentials_path)

#     # Obtenez le seau
#     bucket = client.get_bucket(bucket_name)

#     # Obtenez la date d'aujourd'hui et celle d'hier
#     today = datetime.now().date()
#     yesterday = today - timedelta(days=1)

#     # Liste des objets dans le seau filtrée pour n'inclure que ceux modifiés aujourd'hui et hier
#     blobs_today_and_yesterday = [blob for blob in bucket.list_blobs()
#                                  if blob.updated.date() == today or blob.updated.date() == yesterday]

#     # Récupérer les noms des fichiers publiés aujourd'hui et hier
#     file_names_today_and_yesterday = [blob.name for blob in blobs_today_and_yesterday]

#     return file_names_today_and_yesterday

# def daily_file_process():
#     files_published_today = get_files_published_today(BUCKET_NAME, PATH_CREDENTIALS)
#     ETL_pipeline(files_published_today)


if __name__ == "__main__":
    pass
    # files_published_today = get_files_published_today(BUCKET_NAME, PATH_CREDENTIALS)
    # ETL_pipeline(files_published_today)

    
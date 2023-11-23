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
import numpy as np
import uuid


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

        NAME_TABLE_MATCH_FINISHED = "FINISHED_MATCHES"
        NAME_TABLE_HOME_STARTING_LINEUP = "TEAM_HOME_STARTING_LINEUP_FINISHED_MATCHES"
        NAME_TABLE_AWAY_STARTING_LINEUP = "TEAM_AWAY_STARTING_LINEUP_FINISHED_MATCHES"
        NAME_TABLE_ODDS = "ODDS_FINISHED_MATCHES"

        def extract(file):
            return read_json_from_gcs(BUCKET_NAME, file)

        def transform_pandas(json_obj):
            json_data = json.dumps(json_obj)
            print(json_data)
            # Load JSON data into a pandas DataFrame
            original_df = pd.read_json(json_data, orient='records')
            print("original_df : ",original_df.columns)
            # Filter rows where 'current_status' is 'Finished'
            df = original_df[original_df['current_status'] == "Termin\u00e9"]

            # Drop columns
            # columns_to_drop = ['match_hour', 'current_status']

            # match_minute = 0 # default value
            # match_hour = 0 # default value

            # df["match_minute"] = match_minute
            # df["match_hour"] = match_hour


            columns_to_keep = [
                'current_country', 
                'current_tournament',
                'participant_home',
                'participant_away',
                'participant_home_current_score',
                'participant_away_current_score',
                'year',
                'month',
                'day',
                'hour',
                'minute'
            ]

            print("df : ",df.columns)
            print(df.shape)
            
            df = df.loc[:, columns_to_keep].copy()
            # df = df.drop(columns=columns_to_drop)

            # Drop rows with missing values
            df = df.dropna()

            # Convert specific columns to integer type
            df['participant_away_current_score'] = pd.to_numeric(df['participant_away_current_score'], errors='coerce')
            df['participant_home_current_score'] = pd.to_numeric(df['participant_home_current_score'], errors='coerce')

            print("before : ",df.shape)

            df['ID_MATCH'] = [str(uuid.uuid4()) for _ in range(df.shape[0])]

            print("after : ",df.shape)
            print(df.head())

            print(df.columns)

            # Reorder columns
            desired_column_order = [
                'ID_MATCH',
                'year',
                'month',
                'day',
                'hour',
                'minute',
                "current_country", 
                "current_tournament", 
                "participant_home", 
                "participant_home_current_score", 
                "participant_away", 
                "participant_away_current_score"
            ]

            df = df[desired_column_order]

            # Rename columns
            column_mapping = {
                'year' : "YEAR_MATCH",
                'month' : "MONTH_MATCH",
                'day' : "DAY_MATCH",
                'hour' : "HOUR_MATCH",
                'minute' : "MINUTE_MATCH",
                "current_country": "COUNTRY_MATCH", 
                "current_tournament": "TOURNAMENT", 
                "participant_home": "NAME_TEAM_HOME",
                "participant_home_current_score": "SCORE_TEAM_HOME", 
                "participant_away": "NAME_TEAM_AWAY",
                "participant_away_current_score": "SCORE_TEAM_AWAY"
            }

            print("df shape ! ",df.shape)

            df = df.rename(columns=column_mapping)

            return df

        def transform_home_starting_lineup(json_obj):
            json_data = json.dumps(json_obj)

            # Load JSON data into a pandas DataFrame
            original_df = pd.read_json(json_data, orient='records')

            df = original_df[original_df['current_status'] == "Termin\u00e9"]

            columns_to_keep = ["lineups_data"]

            df = df.loc[:, columns_to_keep]

            lineups_data_home = df['lineups_data'].apply(lambda json_obj: json_obj["Team1"])
            lineups_data_away = df['lineups_data'].apply(lambda json_obj: json_obj["Team2"])

            lineups_data_home = lineups_data_home.apply(lambda json_list: [json_list_val["name"] for json_list_val in json_list])
            lineups_data_away = lineups_data_away.apply(lambda json_list: [json_list_val["name"] for json_list_val in json_list])
            
            print("test TTTTT")
            print("1 : ",lineups_data_home.shape, len(lineups_data_home))
            print("2 : ",lineups_data_away.shape)

            # verification qu'il y a exactement 11 joueurs par equipe
            # for i in range (len(lineups_data_home)):
            #     print(len(lineups_data_home[i]))
                
            lineups_data_home = np.array([team if len(team) == 11 else [None] * 11 for team in lineups_data_home ])
            lineups_data_away = np.array([team if len(team) == 11 else [None] * 11 for team in lineups_data_away ])

            

            return lineups_data_home, lineups_data_away



        def transform_odds(json_obj):
            
                
            json_data = json.dumps(json_obj)

            # Load JSON data into a pandas DataFrame
            original_df = pd.read_json(json_data, orient='records')

            df = original_df[original_df['current_status'] == 'Termin\u00e9']

            columns_to_keep = ["bookmakers_data"]

            df = df.loc[:, columns_to_keep]

            

            return df.reset_index(drop=True)





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
            insert_query = f"""INSERT INTO {NAME_TABLE_MATCH_FINISHED} (
                ID_MATCH,
                DATE_LOAD,
                YEAR_MATCH,
                MONTH_MATCH,
                DAY_MATCH,
                HOUR_MATCH,
                MINUTE_MATCH,
                COUNTRY_MATCH,
                TOURNAMENT,
                NAME_TEAM_HOME,
                SCORE_TEAM_HOME,
                NAME_TEAM_AWAY,
                SCORE_TEAM_AWAY
            ) VALUES (%s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            # Initialiser une liste pour stocker les IDs
            inserted_ids = []

            # Exécuter la requête d'insertion pour chaque ligne de données
            for row in df.itertuples(index=False):
                print("row : ",row)
                time.sleep(2)
                cur.execute(insert_query, row)
                # Récupérer l'ID de la ligne insérée
                cur.execute(f"SELECT ID_MATCH FROM {NAME_TABLE_MATCH_FINISHED} ORDER BY DATE_LOAD DESC LIMIT 1") # recuperer les ids des lignes load
                last_inserted_id = cur.fetchone()[0]
                print("last_inserted_id : ",last_inserted_id)
                inserted_ids.append(last_inserted_id)


            # Valider les changements
            conn.commit()

            # Fermer le curseur et la connexion
            cur.close()
            conn.close()
            # print(inserted_ids ,len(inserted_ids))
            return inserted_ids


        def get_odd_value(name_odd, row_odd):
            
            print("row_odd: ",row_odd)
            for odd in row_odd:

                if odd["bookmaker"] == name_odd:
                    if len(odd["odds"]) != 3:
                        return [None, None, None]
                    else:
                        return [float(o) for o in odd["odds"]]
            return [None, None, None]

        def process_row_odd(row_odd):
            print(row_odd)
            data = list()
            
            data = get_odd_value("Betclic.fr", row_odd)
            data += get_odd_value("Unibet.fr", row_odd)
            data += get_odd_value("bwin.fr", row_odd)
            data += get_odd_value("France Pari", row_odd)
            data += get_odd_value("NetBet.fr", row_odd)
            data += get_odd_value("Winamax", row_odd)
            data += get_odd_value("bet365", row_odd)
            data += get_odd_value("1xBet", row_odd)

            return data


        def load_odds(df_odds, inserted_ids):
            print("PASSAGE odds")
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
            insert_query = f"""INSERT INTO {NAME_TABLE_ODDS} (
                        ID_MATCH,
                        
                        BETCLIC_HOME_ODD,
                        BETCLIC_NIL_ODD,
                        BETCLIC_AWAY_ODD,

                        UNIBET_HOME_ODD,
                        UNIBET_NIL_ODD,
                        UNIBET_AWAY_ODD,

                        BWIN_HOME_ODD,
                        BWIN_NIL_ODD,
                        BWIN_AWAY_ODD,

                        FRANCE_PARIS_HOME_ODD,
                        FRANCE_PARIS_NIL_ODD,
                        FRANCE_PARIS_AWAY_ODD,

                        NETBET_HOME_ODD,
                        NETBET_NIL_ODD,
                        NETBET_AWAY_ODD,

                        WINAMAX_HOME_ODD,
                        WINAMAX_NIL_ODD,
                        WINAMAX_AWAY_ODD,

                        BET365_HOME_ODD,
                        BET365_NIL_ODD,
                        BET365_AWAY_ODD,

                        ONEBET_HOME_ODD,
                        ONEBET_NIL_ODD,
                        ONEBET_AWAY_ODD

                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""


            print('df_odds["bookmakers_data"] : ',df_odds["bookmakers_data"])

            print("len(inserted_ids) : ", len(inserted_ids))
            print('len(df_odds["bookmakers_data"]) : ',len(df_odds["bookmakers_data"]))
            # Exécuter la requête d'insertion pour chaque ligne de données
            for i_row_odds,id in zip(range(len(df_odds["bookmakers_data"])), inserted_ids):
                print("eter, ",df_odds["bookmakers_data"][i_row_odds])
                row_odds_processed = process_row_odd(df_odds["bookmakers_data"][i_row_odds])
                print("eter, ",id, row_odds_processed)
                row = [id] + row_odds_processed

                cur.execute(insert_query, row)
                # Récupérer l'ID de la ligne insérée

            # Valider les changements
            conn.commit()

            # Fermer le curseur et la connexion
            cur.close()
            conn.close()

            return inserted_ids








        def load_player(arr_player, inserted_ids, name_table):

            print("load_player")
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
            insert_query = f"""INSERT INTO {name_table} (
                ID_MATCH,
                NAME_PLAYER_1,
                NAME_PLAYER_2,
                NAME_PLAYER_3,
                NAME_PLAYER_4,
                NAME_PLAYER_5,
                NAME_PLAYER_6,
                NAME_PLAYER_7,
                NAME_PLAYER_8,
                NAME_PLAYER_9,
                NAME_PLAYER_10,
                NAME_PLAYER_11
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            print("inserted_ids : ",inserted_ids)
            print("arr_player : ",arr_player)

            for id, team_player in zip(inserted_ids, arr_player):
                print(team_player,id)
                tp = team_player.tolist()
                if tp != [None]*11:
                    row = [id] + tp
                    cur.execute(insert_query, row)

            # Valider les changements
            conn.commit()

            # Fermer le curseur et la connexion
            cur.close()
            conn.close()


            
        def ETL_pipeline(list_files):
            bucket_source = "data-flashscore"
            bucket_destination = "data-flashscore-used"
            for file in list_files:
                # extract data
                json_obj = extract(file)

                # transform functions
                df = transform_pandas(json_obj)
                arr_home, arr_away = transform_home_starting_lineup(json_obj)
                df_odds = transform_odds(json_obj)

                inserted_ids = load(df)

                print("inserted_ids : ",inserted_ids)

                load_player(arr_home, inserted_ids, NAME_TABLE_HOME_STARTING_LINEUP)
                load_player(arr_away, inserted_ids, NAME_TABLE_AWAY_STARTING_LINEUP)
                load_odds(df_odds, inserted_ids)
                move_file(bucket_source, file, bucket_destination, file)



                

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
            # yesterday = today - timedelta(days=1)

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
from google.cloud import storage
import snowflake.connector
import pendulum
import os
import pandas as pd
import json
import numpy as np
import uuid
import time
import os

BUCKET_NAME = os.getenv('BUCKET_NAME')
ORGNAME = os.getenv('ORGNAME')
ACCOUNT_NAME = os.getenv('ACCOUNT_NAME')

ACCOUNT_IDENTIFIER = f"{ORGNAME}-{ACCOUNT_NAME}"
PASSWORD = os.getenv("PASSWORD")
USER = os.getenv("USER")

WH = os.getenv("WH")
DB = os.getenv("DB")

NAME_TABLE_MATCH_FINISHED = os.getenv("NAME_TABLE_MATCH_FINISHED")
NAME_TABLE_HOME_STARTING_LINEUP = os.getenv("NAME_TABLE_HOME_STARTING_LINEUP")
NAME_TABLE_AWAY_STARTING_LINEUP = os.getenv("NAME_TABLE_AWAY_STARTING_LINEUP")
NAME_TABLE_ODDS = os.getenv("NAME_TABLE_ODDS")




def move_file(source_bucket_name, source_object_name, destination_bucket_name, destination_object_name):
    credentials_json_str = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if credentials_json_str is None:
        raise ValueError("La variable d'environnement GOOGLE_APPLICATION_CREDENTIALS_JSON n'est pas définie.")

    credentials = json.loads(credentials_json_str)

    client = storage.Client.from_service_account_info(credentials)

    source_bucket = client.bucket(source_bucket_name)

    source_blob = source_bucket.blob(source_object_name)

    destination_bucket = client.bucket(destination_bucket_name)

    source_bucket.copy_blob(
        source_blob, destination_bucket, destination_object_name
    )

    source_blob.delete()

    print(f"Fichier déplacé avec succès de {source_bucket_name}/{source_object_name} vers {destination_bucket_name}/{destination_object_name}")

def read_json_from_gcs(bucket_name, file_path):
    credentials_json_str = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if credentials_json_str is None:
        raise ValueError("La variable d'environnement GOOGLE_APPLICATION_CREDENTIALS_JSON n'est pas définie.")

    credentials = json.loads(credentials_json_str)

    client = storage.Client.from_service_account_info(credentials)

    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob(file_path)

    json_content = blob.download_as_text()

    data = json.loads(json_content)

    return data


def extract(file):
    return read_json_from_gcs(BUCKET_NAME, file)

def transform_pandas(json_obj):
    
    json_data = json.dumps(json_obj)

    original_df = pd.read_json(json_data, orient='records')

    df = original_df[original_df['current_status'] == "Finished"]

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

    df = df.loc[:, columns_to_keep].copy()

    df = df.dropna()

    # Convert specific columns to integer type
    df['participant_away_current_score'] = pd.to_numeric(df['participant_away_current_score'], errors='coerce')
    df['participant_home_current_score'] = pd.to_numeric(df['participant_home_current_score'], errors='coerce')

    df['ID_MATCH'] = [str(uuid.uuid4()) for _ in range(df.shape[0])]

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

    df = df.rename(columns=column_mapping)

    return df

def transform_home_starting_lineup(json_obj):
    json_data = json.dumps(json_obj)

    # Load JSON data into a pandas DataFrame
    original_df = pd.read_json(json_data, orient='records')

    df = original_df[original_df['current_status'] == "Finished"]

    columns_to_keep = ["lineups_data"]

    df = df.loc[:, columns_to_keep]

    lineups_data_home = df['lineups_data'].apply(lambda json_obj: json_obj["Team1"])
    lineups_data_away = df['lineups_data'].apply(lambda json_obj: json_obj["Team2"])

    lineups_data_home = lineups_data_home.apply(lambda json_list: [json_list_val["name"] for json_list_val in json_list])
    lineups_data_away = lineups_data_away.apply(lambda json_list: [json_list_val["name"] for json_list_val in json_list])
        
    lineups_data_home = np.array([team if len(team) == 11 else [None] * 11 for team in lineups_data_home ])
    lineups_data_away = np.array([team if len(team) == 11 else [None] * 11 for team in lineups_data_away ])

    return lineups_data_home, lineups_data_away

def transform_odds(json_obj):
        
    json_data = json.dumps(json_obj)

    # Load JSON data into a pandas DataFrame
    original_df = pd.read_json(json_data, orient='records')

    df = original_df[original_df['current_status'] == 'Finished']

    columns_to_keep = ["bookmakers_data"]

    df = df.loc[:, columns_to_keep]

    return df.reset_index(drop=True)

def load(df):
    conn=snowflake.connector.connect(
        account=ACCOUNT_IDENTIFIER,
        user=USER,
        password=PASSWORD,
        warehouse = WH,
        database = DB,
    )
    cur = conn.cursor()

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

    inserted_ids = []

    for row in df.itertuples(index=False):
        time.sleep(2)
        cur.execute(insert_query, row)
        cur.execute(f"SELECT ID_MATCH FROM {NAME_TABLE_MATCH_FINISHED} ORDER BY DATE_LOAD DESC LIMIT 1") # recuperer les ids des lignes load
        last_inserted_id = cur.fetchone()[0]
        inserted_ids.append(last_inserted_id)

    conn.commit()

    cur.close()
    conn.close()
    # print(inserted_ids ,len(inserted_ids))
    return inserted_ids

def get_odd_value(name_odd, row_odd):
    
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
    conn=snowflake.connector.connect(
        account=ACCOUNT_IDENTIFIER,
        user=USER,
        password=PASSWORD,
        warehouse = WH,
        database = DB,
        
    )
    cur = conn.cursor()

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

    for i_row_odds,id in zip(range(len(df_odds["bookmakers_data"])), inserted_ids):
        row_odds_processed = process_row_odd(df_odds["bookmakers_data"][i_row_odds])
        row = [id] + row_odds_processed
        cur.execute(insert_query, row)

    conn.commit()

    cur.close()
    conn.close()

    return inserted_ids

def load_player(arr_player, inserted_ids, name_table):

    conn=snowflake.connector.connect(
        account=ACCOUNT_IDENTIFIER,
        user=USER,
        password=PASSWORD,
        warehouse = WH,
        database = DB,
        
    )
    cur = conn.cursor()

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

    for id, team_player in zip(inserted_ids, arr_player):
        tp = team_player.tolist()
        if tp != [None]*11:
            row = [id] + tp
            cur.execute(insert_query, row)

    conn.commit()

    cur.close()
    conn.close()

def ETL_pipeline(list_files):
    bucket_source = "data-flashscore"
    bucket_destination = "data-flashscore-used"
    for file in list_files:
        json_obj = extract(file)

        df = transform_pandas(json_obj)
        arr_home, arr_away = transform_home_starting_lineup(json_obj)
        df_odds = transform_odds(json_obj)

        inserted_ids = load(df)

        load_player(arr_home, inserted_ids, NAME_TABLE_HOME_STARTING_LINEUP)
        load_player(arr_away, inserted_ids, NAME_TABLE_AWAY_STARTING_LINEUP)
        load_odds(df_odds, inserted_ids)
        move_file(bucket_source, file, bucket_destination, file)

def get_files_published_today(bucket_name):
    credentials_json_str = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')

    if credentials_json_str is None:
        raise ValueError("La variable d'environnement GOOGLE_APPLICATION_CREDENTIALS_JSON n'est pas définie.")

    credentials = json.loads(credentials_json_str)

    client = storage.Client.from_service_account_info(credentials)

    bucket = client.get_bucket(bucket_name)

    today = datetime.now().date()

    file_names_today_and_yesterday = [blob.name for blob in bucket.list_blobs() ]# blobs_today_and_yesterday]

    return file_names_today_and_yesterday
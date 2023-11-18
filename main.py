import requests
import yaml
from google.cloud import storage
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import json
# url = "https://storage.cloud.google.com/europe-west6-airflow-data-e-f3099903-bucket/credentials/credentials.yml"

# try:
#     response = requests.get(url, allow_redirects=True)

#     if response.status_code == 200:
#         # Chargez les informations depuis le contenu téléchargé
#         content = response.text
#         # Load the yaml
#         credentials = yaml.safe_load(content)
#         print("Informations d'identification chargées avec succès.")
#         print(credentials)
#     else:
#         raise ValueError(f"Erreur lors du téléchargement du fichier credentials depuis l'URL. Code d'état : {response.status_code}")

# except Exception as e:
#     print(f"Une erreur s'est produite : {e}")
#     # Ajoutez ici le code de gestion des erreurs spécifiques si nécessaire


# import urllib
# txt =  urllib.request.urlopen(url).read()
# print(txt)

load_dotenv()

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

    blobs_today_and_yesterday = [blob for blob in bucket.list_blobs()
                                if blob.updated.date() == today or blob.updated.date() == yesterday]

    file_names_today_and_yesterday = [blob.name for blob in blobs_today_and_yesterday]

    return file_names_today_and_yesterday

print(get_files_published_today("data-flashscore"))



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
    new_blob = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_object_name
    )

    # Supprimer l'objet source après avoir été copié
    source_blob.delete()

    print(f"Fichier déplacé avec succès de {source_bucket_name}/{source_object_name} vers {destination_bucket_name}/{destination_object_name}")



bucket_source = "data-flashscore"
bucket_destination = "data-flashscore-used"
file = "04_11_2023-18_38_54.json"

# Exemple d'utilisation
move_file(bucket_source, file, bucket_destination, file)
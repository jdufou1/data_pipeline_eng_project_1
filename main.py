import requests
import yaml


url = "https://storage.cloud.google.com/europe-west6-airflow-data-e-f3099903-bucket/credentials/credentials.yml"

try:
    response = requests.get(url, allow_redirects=True)

    if response.status_code == 200:
        # Chargez les informations depuis le contenu téléchargé
        content = response.text
        # Load the yaml
        credentials = yaml.safe_load(content)
        print("Informations d'identification chargées avec succès.")
        print(credentials)
    else:
        raise ValueError(f"Erreur lors du téléchargement du fichier credentials depuis l'URL. Code d'état : {response.status_code}")

except Exception as e:
    print(f"Une erreur s'est produite : {e}")
    # Ajoutez ici le code de gestion des erreurs spécifiques si nécessaire


import urllib
txt =  urllib.request.urlopen(url).read()
print(txt)
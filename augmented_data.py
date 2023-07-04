import concurrent.futures
import datetime
import json
import logging
import os
import re
import urllib.request
from collections import defaultdict
from zipfile import ZipFile

import pandas as pd
import requests

# Définition des constantes
CSV_FILENAME = 'SUP_ANTENNE.csv'
JSON_DIR = 'local_antenna_data'
AUGMENTED_JSON_DIR = 'local_antenna_data_augmented'
JSON_EXT = '.json'
BASE_URL = 'https://www.data.gouv.fr/api/1/'
PATH = 'datasets/551d4ff3c751df55da0cd89f'

# Configuration des logs
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger()

# Fonction pour créer un répertoire s'il n'existe pas déjà
def create_directory(dirname: str):
    if not os.path.exists(dirname):
        os.makedirs(dirname)
        logger.info(f"Répertoire '{dirname}' créé.")
    else:
        logger.info(f"Répertoire '{dirname}' existe déjà.")

# Fonction pour supprimer le fichier ZIP après extraction
def delete_zip_file(zip_path):
    try:
        # Suppression du fichier ZIP
        os.remove(zip_path)
        # Retour de succès si la suppression s'est bien passée
        return True
    except Exception as e:
        # En cas d'erreur, affichage d'un message d'erreur et retour de l'échec
        logging.error("Erreur lors de la suppression du fichier ZIP : ", e)
        return False

# Fonction pour supprimer un fichier CSV local
def delete_csv_file(file_name):
    try:
        # Suppression du fichier
        os.remove(file_name)
        # Retour de succès si la suppression s'est bien passée
        return True
    except Exception as e:
        # En cas d'erreur, affichage d'un message d'erreur et retour de l'échec
        logging.error("Erreur lors de la suppression du fichier local : ", e)
        return False

# Fonction pour renommer un fichier .txt en .csv
def rename_txt_file_to_csv_file(old_name, new_name):
    try:
        # Renommage du fichier
        os.rename(old_name, new_name)
        # Retour de succès si le renommage s'est bien passé
        return True
    except Exception as e:
        # En cas d'erreur, affichage d'un message d'erreur et retour de l'échec
        logging.error("Erreur lors du renommage du fichier : ", e)
        return False

# Fonction pour vérifier si le fichier local est à jour par rapport à la version en ligne
def is_local_file_up_to_date(online_date):
    logger.info("Vérification si le fichier local est à jour...")
    # Vérification de l'existence du fichier local
    if os.path.exists(CSV_FILENAME):
        # Récupération de la date de dernière modification du fichier local
        local_file_time = os.path.getmtime(CSV_FILENAME)
        local_date = datetime.datetime.fromtimestamp(local_file_time)
        # Si la date du fichier local est plus récente que la date en ligne
        if local_date > online_date:
            logging.info("La version locale de SUP_ANTENNE.csv est à jour.")
            # Le fichier local est à jour, pas besoin de télécharger le fichier en ligne
            return False
        else:
            logging.info("Mise à jour de SUP_ANTENNE.csv...")
            # Suppression du fichier local avant de télécharger la version en ligne
            delete_csv_file(CSV_FILENAME)
            return True
    else:
        # Si le fichier local n'existe pas, il faut télécharger le fichier en ligne
        return True

# Fonction pour vérifier si un fichier JSON est obsolète par rapport à un fichier CSV
def is_json_outdated_compared_to_csv(filename, csv_filename):
    # Si le fichier n'existe pas, il est considéré comme obsolète
    if not os.path.exists(filename):
        return True
    # Obtenir le timestamp du fichier
    file_time = os.path.getmtime(filename)
    # Convertir le timestamp en date
    file_date = datetime.datetime.fromtimestamp(file_time)
    # Obtenir le timestamp du fichier CSV
    csv_file_time = os.path.getmtime(csv_filename)
    # Convertir le timestamp du fichier CSV en date
    csv_file_date = datetime.datetime.fromtimestamp(csv_file_time)
    # Le fichier est obsolète si sa date est antérieure à celle du fichier CSV
    return file_date < csv_file_date

# Fonction pour récupérer les données JSON depuis l'API de data.gouv.fr
def get_data():
    logger.info(f"Envoi de la requête GET à {BASE_URL + PATH}...")

    # Essai d'envoi de la requête GET
    with requests.get(BASE_URL + PATH) as response:
        # Si le statut de la réponse n'est pas 200 (succès), log une erreur et retourne None
        if response.status_code != 200:
            logger.error(f'Requête GET a échoué avec le statut: {response.status_code}')
            return None
        # Essai de décodage du JSON de la réponse
        try:
            data = response.json()
            logger.info('Données JSON reçues avec succès.')
            return data
        except json.JSONDecodeError:
            logger.error("Erreur lors du décodage du JSON.")
            return None

# Fonction pour trouver l'URL des données à partir de la réponse de l'API
def find_data_url(data):
    logger.info("Recherche de l'URL des dernières tables supports antennes emetteurs bandes...")
    # Parcours des ressources dans la réponse
    for resource in data['resources']:
        # Si le titre de la ressource contient "Tables supports antennes emetteurs bandes"
        if 'Tables supports antennes emetteurs bandes' in resource['title']:
            # Récupération de l'URL de la ressource
            url = resource['url']
            logger.info(f'URL trouvé: {url}')
            # Retour de l'URL trouvé
            return url
    # Si aucun URL n'est trouvé, retour de None
    return None

# Fonction pour extraire l'horodatage à partir de l'URL
def get_timestamp_from_url(url):
    logger.info("Extraction de l'horodatage de l'URL...")
    # Utilisation d'une expression régulière pour trouver la date dans l'URL
    match = re.search(r'\d{8}-\d{6}', url)
    # Si une date est trouvée
    if match:
        # Conversion de la date en objet datetime
        date_str = match.group()
        date = datetime.datetime.strptime(date_str, "%Y%m%d-%H%M%S")
        logger.info(f'Date extraite de l\'URL: {date}')
        # Retour de la date
        return date
    # Si aucune date n'est trouvée, retour de None
    return None

def get_antenna_data_last_modified_date():
    logger.info("Obtention de la date de dernière mise à jour des orientations des antennes...")
    url = BASE_URL + PATH
    response = requests.get(url)
    data = response.json()
    data_url = find_data_url(data)
    if data_url is not None:
        last_modified_date = get_timestamp_from_url(data_url)
        if last_modified_date is not None:
            return last_modified_date.strftime("%d-%m-%Y %H:%M:%S")
        else:
            logger.info("Impossible d'extraire l'horodatage de l'URL des données.")
            return None
    else:
        logger.info("Impossible de trouver l'URL des données des antennes.")
        return None

# Fonction pour télécharger le fichier ZIP à partir de l'URL
def download_zip_file(file_url):
    logger.info(f"Téléchargement du fichier ZIP depuis {file_url}...")
    try:
        # Téléchargement du fichier
        zip_path, _ = urllib.request.urlretrieve(file_url)
        logger.info(f'Fichier téléchargé avec succès à l\'emplacement: {zip_path}')
        # Retour du chemin du fichier téléchargé
        return zip_path
    except Exception as e:
        # En cas d'erreur, affichage d'un message d'erreur et retour de None
        logger.error(f"Erreur lors du téléchargement du fichier ZIP : {e}")
        return None

# Fonction pour extraire le fichier CSV du ZIP
def extract_csv_from_zip(zip_path, file_name):
    logger.info(f"Extraction de {file_name} du fichier ZIP...")
    try:
        # Ouverture du fichier ZIP
        with ZipFile(zip_path, 'r') as zip_ref:
            # Extraction du fichier CSV
            zip_ref.extract(file_name, '.')
        logger.info(f'Fichier {file_name} extrait avec succès.')
        # Retour de succès si l'extraction s'est bien passée
        return True
    except Exception as e:
        # En cas d'erreur, affichage d'un message d'erreur et retour de l'échec
        logger.error(f"Erreur lors de l'extraction du fichier ZIP : {e}")
        return False

# Fonction pour mettre à jour le fichier CSV
def update_csv_file(data):
    logger.info("Mise à jour du fichier SUP_ANTENNE.csv...")

    # Trouver l'URL du fichier de données dans la réponse de l'API
    file_url = find_data_url(data)
    # Extraire la date de l'URL du fichier de données
    online_date = get_timestamp_from_url(file_url)
    # Si le fichier local n'est pas à jour
    if is_local_file_up_to_date(online_date):
        # Télécharger le fichier ZIP à l'URL trouvée
        zip_path = download_zip_file(file_url)
        # Si le téléchargement a réussi et que l'extraction du fichier ZIP et le renommage du fichier ont réussi
        if zip_path is not None and extract_csv_from_zip(zip_path, 'SUP_ANTENNE.txt') and rename_txt_file_to_csv_file('SUP_ANTENNE.txt', CSV_FILENAME):
            # Supprimer le fichier ZIP téléchargé
            delete_zip_file(zip_path)
            logger.info("Mise à jour du fichier SUP_ANTENNE.csv réussie.")
        else:
            logger.error("Erreur lors de la mise à jour.")

# Fonction pour charger un DataFrame à partir d'un fichier CSV
def load_dataframe_from_csv(filename: str) -> pd.DataFrame:
    logger.info(f"Chargement du DataFrame à partir de {filename}...")

    # Vérification de l'existence du fichier
    if not os.path.isfile(filename):
        logger.error(f"Le fichier '{filename}' n'existe pas.")
        exit()

    # Liste des colonnes à utiliser dans le DataFrame
    usecols = ['STA_NM_ANFR', 'AER_ID', 'AER_NB_AZIMUT', 'AER_NB_ALT_BAS']

    # Chargement du fichier CSV dans un DataFrame
    df = pd.read_csv(filename, delimiter=';', usecols=usecols)
    
    # Retour du DataFrame
    return df

# Fonction pour fusionner les enregistrements du DataFrame dans le fichier JSON
def merge_dataframe_records_into_json(record: dict, dict_df: defaultdict) -> list:
    new_data = []
    # Vérifie si les clés 'fields' et 'sta_nm_anfr' existent dans l'enregistrement
    if 'fields' in record and 'sta_nm_anfr' in record['fields']:
        sta_nm_anfr = record['fields']['sta_nm_anfr']

        # Si 'sta_nm_anfr' est dans le dictionnaire DataFrame
        if sta_nm_anfr in dict_df:
            # Pour chaque information dans le groupe 'sta_nm_anfr' du dictionnaire DataFrame
            for info in dict_df[sta_nm_anfr]:
                # Crée une copie de l'enregistrement
                new_record = record.copy()
                # Ajoute de nouvelles données à 'fields' dans l'enregistrement
                new_record['fields']['aer_id'] = info['AER_ID']
                new_record['fields']['aer_nb_azimut'] = info['AER_NB_AZIMUT']
                new_record['fields']['aer_nb_alt_bas'] = info['AER_NB_ALT_BAS']
                # Ajoute le nouvel enregistrement à la liste des nouvelles données
                new_data.append(new_record)
    return new_data

# Fonction pour fusionner les données du fichier JSON avec celles du DataFrame
def merge_json_with_dataframe(filename: str, dict_df: defaultdict):
    logger.info(f"Début de l'augmentation du fichier {filename}...")
    # Essai d'ouvrir et de charger le fichier JSON
    try:
        with open(filename, 'r') as f:
            data = json.load(f)
    except json.JSONDecodeError:
        logger.error(f"Erreur : le fichier {filename} n'est pas un JSON valide.")
        return

    # Liste pour stocker les nouvelles données
    new_data = []
    # Pour chaque enregistrement dans les données
    for record in data:
        # Fusionne les enregistrements du DataFrame dans l'enregistrement JSON et ajoute les nouveaux enregistrements à la liste des nouvelles données
        new_data.extend(merge_dataframe_records_into_json(record, dict_df))

    # Retourne les nouvelles données
    return new_data

# Fonction pour fusionner un fichier JSON avec un DataFrame et sauvegarder le résultat
def merge_json_with_dataframe_and_save(filename, dict_df):
    # Nom complet du fichier JSON original
    old_file = os.path.join(JSON_DIR, filename)
    # Nom complet du nouveau fichier JSON
    new_file = os.path.join(AUGMENTED_JSON_DIR, filename)

    # Si le nouveau fichier JSON est obsolète par rapport au fichier CSV
    if is_json_outdated_compared_to_csv(new_file, CSV_FILENAME):
        # Fusionne le fichier JSON avec le DataFrame
        new_data = merge_json_with_dataframe(old_file, dict_df)
        # Écrit les nouvelles données dans le nouveau fichier JSON
        write_list_to_json_file(new_file, new_data)
    else:
        logger.info(f"Le fichier '{new_file}' est à jour.")

# Fonction pour écrire une liste de données dans un fichier JSON
def write_list_to_json_file(filename: str, data: list):
    with open(filename, 'w') as f:
        json.dump(data, f)

# Fonction pour traiter les fichiers JSON
def process_json_files():
    logger.info("Traitement des fichiers JSON...")

    # Si le répertoire des fichiers JSON n'existe pas, log une erreur et retourne
    if not os.path.exists(JSON_DIR):
        logger.error(f"Erreur : le répertoire '{JSON_DIR}' n'existe pas.")
        return
    # Liste des fichiers JSON dans le répertoire
    json_files = [filename for filename in os.listdir(JSON_DIR) if filename.endswith(JSON_EXT)]
    # Vérifie si les fichiers JSON sont obsolètes par rapport au fichier CSV
    json_files_are_outdated = any([is_json_outdated_compared_to_csv(os.path.join(AUGMENTED_JSON_DIR, filename), CSV_FILENAME) for filename in json_files])
    # Si au moins un fichier JSON est obsolète
    if json_files_are_outdated:
        # Charge le DataFrame à partir du fichier CSV
        df = load_dataframe_from_csv(CSV_FILENAME)
        # Convertit le DataFrame en un dictionnaire
        dict_df = convert_dataframe_to_dict(df)
        # Utilise un ThreadPoolExecutor pour fusionner chaque fichier JSON avec le DataFrame et sauvegarder le résultat en parallèle
        with concurrent.futures.ThreadPoolExecutor() as executor:
            jobs = [executor.submit(merge_json_with_dataframe_and_save, filename, dict_df) for filename in json_files]
            # Attends que tous les travaux soient terminés
            for job in concurrent.futures.as_completed(jobs):
                pass
    else:
        # Si tous les fichiers JSON sont à jour, log cette information
        logger.info("Tous les fichiers JSON sont à jour.")

# Fonction pour convertir un DataFrame en un dictionnaire
def convert_dataframe_to_dict(df: pd.DataFrame) -> defaultdict:
    logger.info("Conversion du DataFrame en dictionnaire...")

    # Groupement du DataFrame par 'STA_NM_ANFR' et conversion des enregistrements dans chaque groupe en une liste de dictionnaires
    df_grouped = df.groupby('STA_NM_ANFR')[['AER_ID', 'AER_NB_AZIMUT', 'AER_NB_ALT_BAS']].apply(lambda x: x.to_dict('records')).to_dict()
    return df_grouped


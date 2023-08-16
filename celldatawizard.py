import concurrent.futures
import datetime
import json
import logging
import math
import os
import re
import urllib.request
from collections import defaultdict
from zipfile import ZipFile

import folium
import numpy as np
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from geopy.distance import distance
from requests.exceptions import RequestException
from shapely.geometry import Point
from threading import Thread
import tkinter as tk
from tkinter import ttk

# Définition des constantes
CSV_FILENAME = 'SUP_ANTENNE.csv'
JSON_DIR = 'local_antenna_data'
AUGMENTED_JSON_DIR = 'local_antenna_data_augmented'
JSON_EXT = '.json'
BASE_URL = 'https://www.data.gouv.fr/api/1/'
PATH = 'datasets/551d4ff3c751df55da0cd89f'
URL_LAST_MODIFIED = "https://data.anfr.fr/anfr/visualisation/information/?id=dd11fac6-4531-4a27-9c8c-a3a9e4ec2107&refine.statut=En+service&refine.statut=Techniquement+op%C3%A9rationnel"
LOCAL_DATA_DIR = "local_antenna_data"
AUGMENTED_DATA_DIR = 'local_antenna_data_augmented'
URL_BASE = "https://data.anfr.fr/api/records/2.0/downloadfile/format=json&refine.statut=En+service&refine.statut=Techniquement+op%C3%A9rationnel&resource_id=88ef0887-6b0f-4d3f-8545-6d64c8f597da"


OPERATORS = [
    "ORANGE",
    "BOUYGUES TELECOM",
    "SFR",
    "FREE MOBILE",
    "DIGICEL",
    "FREE CARAIBES",
    "SFR CARAIBES",
]

GENERATIONS = ["2G", "3G", "4G", "5G"]

OPERATOR_NAME_MAPPING = {
    "SFR CARAIBES": "OUTREMER TELECOM",
}

# Configuration des logs
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger()

# Configuration de la journalisation
logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# Fonction pour obtenir la date de dernière modification des données ANFR
def get_anfr_data_last_modified_date():
    logging.info("Début de la fonction get_anfr_data_last_modified_date.")

    logging.info("Récupération de la date de dernière modification des données de l'ANFR.")
    
    url = "https://data.anfr.fr/anfr/visualisation/information/?id=dd11fac6-4531-4a27-9c8c-a3a9e4ec2107&refine.adm_lb_nom=BOUYGUES+TELECOM&refine.adm_lb_nom=DIGICEL&refine.adm_lb_nom=FREE+CARAIBES&refine.adm_lb_nom=FREE+MOBILE&refine.adm_lb_nom=ORANGE&refine.adm_lb_nom=OUTREMER+TELECOM&refine.adm_lb_nom=SFR"
    
    try:
        logging.info("Envoi de la requête GET.")
        response = requests.get(url)
        response.raise_for_status()
        
        logging.info("Extraction de la date à partir de la réponse.")
        last_modified_text = response.text.split('language&quot;:&quot;fr&quot;,&quot;modified&quot;:&quot;', 1)[1]
        last_modified_date = last_modified_text.split('&quot;', 1)[0]
        datetime_object = datetime.strptime(last_modified_date, "%Y-%m-%dT%H:%M:%S.%f")
        formatted_date = datetime_object.strftime("%d-%m-%Y %H:%M:%S")
        
        logging.info(f"Date extraite avec succès : {formatted_date}")
        return formatted_date

    except RequestException as e:
        logging.error(f"Une erreur est survenue lors de la tentative de récupération des données de l'ANFR : {e}")
        return None
    except ValueError as e:
        logging.error(f"Une erreur est survenue lors de l'analyse de l'en-tête 'Last-Modified' : {e}")
        return None
    except IndexError as e:
        logging.error(f"Une erreur est survenue lors du fractionnement de la réponse : {e}")
        return None
    finally:
        logging.info("Fin de la fonction get_anfr_data_last_modified_date.")

# Fonction pour récupérer ou mettre à jour les données de l'antenne
def retrieve_or_update_antenna_data(operator, generation, local_data_dir, anfr_last_modified_date):
    # Log de l'information
    logging.info(f"Récupération ou mise à jour des données de l'antenne pour {operator} {generation}.")
    
    # Construire le chemin du fichier
    filepath = os.path.join(local_data_dir, f"{operator}_{generation}.json")
    
    # Si le fichier n'existe pas ou si les données locales sont périmées, télécharger et rafraîchir les données
    if not os.path.exists(filepath) or is_local_data_outdated(filepath, anfr_last_modified_date):
        return download_and_refresh_local_data(operator, generation, local_data_dir)
    
    # Lire les données de l'antenne et les retourner
    return read_antenna_data(operator, generation, local_data_dir)

# Fonction pour télécharger les données de l'antenne
def download_antenna_data(operator, generation, local_data_dir):
    # Log de l'information
    logging.info(f"Téléchargement des données de l'antenne pour {operator} {generation}.")
    
    try:
        # Obtenir le nom de l'opérateur dans l'API
        operator_name_in_api = OPERATOR_NAME_MAPPING.get(operator, operator)
        
        # Construire l'URL de l'API
        url = f"{URL_BASE}&refine.adm_lb_nom={operator_name_in_api}&refine.generation={generation}"
        
        # Faire une requête GET à l'API et convertir la réponse en JSON
        data = requests.get(url).json()

        # Créer le chemin vers le fichier local
        filepath = os.path.join(local_data_dir, f"{operator}_{generation}.json")

        # Ouvrir le fichier en mode écriture
        with open(filepath, "w") as f:
            # Ecrire les données dans le fichier en format JSON
            json.dump(data, f)

        # Si tout s'est bien passé, retourner True
        return True

    # En cas d'exception
    except Exception as e:
        # Enregistrer l'erreur dans le fichier de log
        logging.error(f"Erreur lors du téléchargement des données pour {operator} {generation} : {e}")
        # Retourner False
        return False

# Fonction pour télécharger et rafraîchir les données locales
def download_and_refresh_local_data(operator, generation, local_data_dir):
    # Log de l'information
    logging.info(f"Téléchargement et actualisation des données pour {operator} {generation}.")
    
    # Télécharger les données de l'antenne
    download_success = download_antenna_data(operator, generation, local_data_dir)
    # Si le téléchargement échoue, retourner None
    if not download_success:  
        return None
    # Lire les données de l'antenne et les retourner
    return read_antenna_data(operator, generation, local_data_dir)

# Fonction pour lire les données de l'antenne
def read_antenna_data(operator, generation, local_data_dir):
    # Log de l'information
    logging.info(f"Lecture des données de l'antenne pour {operator} {generation}.")

    # Créer le chemin vers le fichier local
    filepath = os.path.join(local_data_dir, f"{operator}_{generation}.json")
    
    # Ouvrir le fichier en mode lecture
    with open(filepath, "r") as f:
        # Charger les données JSON et les retourner
        data = json.load(f)
    return data

# Fonction pour vérifier si les données locales sont périmées
def is_local_data_outdated(filepath, anfr_last_modified_date):
    # Log de l'information
    logging.info(f"Vérification si les données locales à {filepath} sont périmées.")
    
    # Vérifier si le fichier local existe
    if os.path.exists(filepath):
        # Obtenir la date de dernière modification du fichier local
        local_last_modified_date = datetime.fromtimestamp(os.path.getmtime(filepath)).strftime("%d-%m-%Y %H:%M:%S")
        
        # Convertir les dates en objets datetime pour la comparaison
        local_last_modified_date = datetime.strptime(local_last_modified_date, "%d-%m-%Y %H:%M:%S")
        anfr_last_modified_date = datetime.strptime(anfr_last_modified_date, "%d-%m-%Y %H:%M:%S")
        
        # Si les données locales sont plus anciennes que les données ANFR, retourner True
        if local_last_modified_date < anfr_last_modified_date:
            return True
    # Si le fichier local n'existe pas, retourner False
    return False



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
        local_date = datetime.fromtimestamp(local_file_time)
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
    file_date = datetime.fromtimestamp(file_time)
    # Obtenir le timestamp du fichier CSV
    csv_file_time = os.path.getmtime(csv_filename)
    # Convertir le timestamp du fichier CSV en date
    csv_file_date = datetime.fromtimestamp(csv_file_time)
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
        date = datetime.strptime(date_str, "%Y%m%d-%H%M%S")
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

# Fonction pour convertir un DataFrame en un dictionnaire
def convert_dataframe_to_dict(df: pd.DataFrame) -> defaultdict:
    logger.info("Conversion du DataFrame en dictionnaire...")

    # Groupement du DataFrame par 'STA_NM_ANFR' et conversion des enregistrements dans chaque groupe en une liste de dictionnaires
    df_grouped = df.groupby('STA_NM_ANFR')[['AER_ID', 'AER_NB_AZIMUT', 'AER_NB_ALT_BAS']].apply(lambda x: x.to_dict('records')).to_dict()
    return df_grouped

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



def get_geolocation_info():
    try:
        response = requests.get('http://ip-api.com/json/')
        geodata = response.json()
        return geodata
    except requests.exceptions.RequestException as e:
        logging.error("RequestException in get_geolocation_info: %s", e)
        return None
        
def create_df_from_antenna_data(all_data):
    df = pd.DataFrame({"record": all_data})
    df["latitude"] = df["record"].apply(lambda x: x["fields"]["coordonnees"][1])
    df["longitude"] = df["record"].apply(lambda x: x["fields"]["coordonnees"][0])
    df["generation"] = df["record"].apply(lambda x: x["fields"]["generation"])
    df["operator"] = df["record"].apply(lambda x: x["fields"]["adm_lb_nom"])
    return df

def haversine(lat1, lon1, lat2, lon2):
    # Convert coordinates to radians
    lat1, lon1 = np.radians([lat1, lon1])
    lat2, lon2 = np.radians([lat2, lon2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    
    # Radius of earth in kilometers
    r = 6371.0
    
    return c * r

def add_geometry_and_distance_to_df(df, lat, lon):
    df["geometry"] = df.apply(lambda x: Point(x["longitude"], x["latitude"]), axis=1)
    df["distance"] = haversine(lat, lon, df["latitude"].values, df["longitude"].values)   
    return df

def filter_df_within_radius(df, radius):
    df_within_radius = df[df["distance"] <= radius]
    return df_within_radius

def filter_antennas_by_radius(all_data, lat, lon, radius):
    df = create_df_from_antenna_data(all_data)
    df = add_geometry_and_distance_to_df(df, lat, lon)
    df_within_radius = filter_df_within_radius(df, radius)
    return df_within_radius

def create_data_dir_if_not_exists(local_data_dir):
    if not os.path.exists(local_data_dir):
        os.makedirs(local_data_dir)

def calculate_total_steps(operators, generations):
    return len(operators) * len(generations)

def download_all_antenna_data(operators, generations, local_data_dir, anfr_last_modified_date, update_progress_callback):
    total_steps = calculate_total_steps(operators, generations)
    current_step = 0
    download_failures = []

    # Créez un gestionnaire de contexte avec ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=12) as executor:  # max_workers définit le nombre maximum de threads utilisés simultanément
        # Créez une liste de tous les jobs à exécuter. Chaque job est une exécution de la fonction download_antenna_data avec des paramètres spécifiques
        jobs = [executor.submit(download_antenna_data, operator, generation, local_data_dir) for operator in operators for generation in generations]
        
        for job in concurrent.futures.as_completed(jobs):
            success = job.result()  # récupère le résultat de download_antenna_data, qui est un booléen
            
            if not success:
                download_failures.append((operator, generation))  # Vous pouvez également modifier votre fonction download_antenna_data pour qu'elle renvoie le couple (operator, generation) en cas d'échec
            
            current_step += 1
            progress = (current_step / total_steps) * 100
            update_progress_callback(progress)
    
    return download_failures

def retrieve_all_antenna_data(operators, generations, local_data_dir, anfr_last_modified_date, update_progress_callback):
    all_data = []
    total_steps = calculate_total_steps(operators, generations)
    current_step = 0
    for operator in operators:
        for generation in generations:
            data = retrieve_or_update_antenna_data(operator, generation, local_data_dir, anfr_last_modified_date)
            if data is not None:
                all_data.extend(data)
            current_step += 1
            progress = (current_step / total_steps) * 100
            update_progress_callback(progress)
    return all_data

def calculate_antenna_densities(operators, generations, df_within_radius, area):
    densities = {gen: {} for gen in generations}
    for generation in generations:
        for operator in operators:
            filtered_df = df_within_radius.query("`generation` == @generation and `operator` == @operator")
            densities[generation][operator] = len(filtered_df) / area
    return densities

def count_antennas(operators, generations, df_within_radius):
    antenna_counts = {gen: {} for gen in generations}
    for generation in generations:
        for operator in operators:
            filtered_df = df_within_radius.query("`generation` == @generation and `operator` == @operator")
            antenna_counts[generation][operator] = len(filtered_df)
    return antenna_counts

def get_antenna_azimuth(antenna_id, operator, generation):
    filepath = os.path.join(AUGMENTED_DATA_DIR, f"{operator}_{generation}.json")
    with open(filepath, "r") as file:
        data = json.load(file)
    for record in data:
        if record['fields']['id'] == antenna_id:
            azimuth = record['fields'].get('aer_nb_azimut', None)
            if azimuth is not None:
                azimuth = azimuth.replace(',', '.')  # Remplace la virgule par un point
                return float(azimuth)  # Convertit en float avant de renvoyer
    return None

def calculate_oriented_antennas(operators, generations, df_within_radius, lat, lon):
    oriented_antennas = {gen: {} for gen in generations}
    for generation in generations:
        for operator in operators:
            df_operator_gen = df_within_radius.query("`generation` == @generation and `operator` == @operator")
            for index, row in df_operator_gen.iterrows():
                azimuth = get_antenna_azimuth(row['record']['fields']['id'], operator, generation)
                if azimuth is not None:
                    if is_oriented_towards_point(row['latitude'], row['longitude'], azimuth, lat, lon):
                        if operator in oriented_antennas[generation]:
                            oriented_antennas[generation][operator] += 1
                        else:
                            oriented_antennas[generation][operator] = 1
    return oriented_antennas

def is_oriented_towards_point(antenna_lat, antenna_lon, antenna_azimuth, point_lat, point_lon):
    angle_to_point = calculate_bearing(antenna_lat, antenna_lon, point_lat, point_lon)
    if abs(angle_to_point - antenna_azimuth) <= 70:  # 140° de vision, donc 70° de chaque côté de l'azimut
        return True
    return False

def calculate_bearing(lat1, lon1, lat2, lon2):
    lat1 = math.radians(lat1)
    lat2 = math.radians(lat2)
    diffLong = math.radians(lon2 - lon1)
    x = math.sin(diffLong) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - (math.sin(lat1) * math.cos(lat2) * math.cos(diffLong))
    bearing = math.degrees(math.atan2(x, y))
    return (bearing + 360) % 360  # Normalisation à 0-360

def calculate_antenna_density_and_counts(operators, generations, all_data, lat, lon, radius):
    area = math.pi * radius * radius
    df_within_radius = filter_antennas_by_radius(all_data, lat, lon, radius)
    densities = calculate_antenna_densities(operators, generations, df_within_radius, area)
    antenna_counts = count_antennas(operators, generations, df_within_radius)
    oriented_antennas = calculate_oriented_antennas(operators, generations, df_within_radius, lat, lon)
    return densities, antenna_counts, oriented_antennas

def calculate_density(operators, generations, lat, lon, radius, anfr_last_modified_date, update_progress_callback):
    create_data_dir_if_not_exists(LOCAL_DATA_DIR)
    
    all_data = retrieve_all_antenna_data(operators, generations, LOCAL_DATA_DIR, anfr_last_modified_date, update_progress_callback)

    if not all_data:
        return f"Erreur lors du téléchargement ou de la récupération des données d'antenne."

    create_data_dir_if_not_exists(AUGMENTED_DATA_DIR)

    data = get_data()
    if data is not None:
        update_csv_file(data)
        process_json_files()

    # Pas besoin de récupérer à nouveau les données
    densities, antenna_counts, oriented_antennas = calculate_antenna_density_and_counts(operators, generations, all_data, lat, lon, radius)
    return densities, antenna_counts, oriented_antennas

def validate_inputs(lat, lon, radius):
    if not lat or not lon or not radius:
        return "Tous les champs doivent être remplis."

    try:
        lat = float(lat)
        lon = float(lon)
        radius = float(radius)

        if not (-90 <= lat <= 90):
            raise ValueError("La latitude doit être comprise entre -90 et 90.")

        if not (-180 <= lon <= 180):
            raise ValueError("La longitude doit être comprise entre -180 et 180.")

        if not (0 <= radius <= 10000):
            raise ValueError("Le rayon doit être compris entre 0 et 10 000 km.")

        return lat, lon, radius
    except ValueError as e:
        return str(e)

def update_progressbar(progress):
    progressbar["value"] = progress
    root.update_idletasks()

def display_antenna_density_results(operators, generations, densities, antenna_counts, oriented_antennas):
    text_result.delete(1.0, tk.END)
    for generation in generations:
        text_result.insert(tk.END, f"---- {generation} ----\n")
        for operator in operators:
            if operator in densities[generation]:
                oriented_antennas_gen_operator = oriented_antennas[generation].get(operator, 0)
                text_result.insert(tk.END, f"{operator} : {densities[generation][operator]:.2f} antennes / km² (total : {antenna_counts[generation][operator]} antennes, {oriented_antennas_gen_operator} orientées vers le point demandé)\n")
        text_result.insert(tk.END, "\n")
    text_result.insert(tk.END, f"Date de dernière mise à jour des données : {anfr_last_modified_date}\n")
    
def create_map(densities, lat, lon, radius):
    m = folium.Map(location=[lat, lon], zoom_start=12)

    # Ajoute un cercle pour représenter le rayon de recherche
    folium.Circle(
        radius=radius*1000,  # folium prend le rayon en mètres
        location=[poi_lat, poi_lon],
        color="red",
        fill=False,
    ).add_to(m)

    # Ajoute un marqueur pour le point d'intérêt
    folium.Marker(
        location=[lat, lon],
    ).add_to(m)

    # Parcoure les antennes dans le dataframe
    for index, row in df.iterrows():
        # Calcule l'orientation de l'antenne
        azimuth = get_antenna_azimuth(row['record']['fields']['id'], row['operator'], row['generation'])

        # Si l'antenne est orientée vers le point d'intérêt, elle est ajoutée sur la carte
        if azimuth is not None and is_oriented_towards_point(row['latitude'], row['longitude'], azimuth, poi_lat, poi_lon):
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=f"Antenna ID: {row['record']['fields']['id']}<br>Operator: {row['operator']}<br>Generation: {row['generation']}<br>Azimuth: {azimuth}",
                icon=folium.Icon(color="green" if row['operator'] == 'ORANGE' else "blue"),  # change la couleur en fonction de l'opérateur
            ).add_to(m)

            # Ajoute une ligne pour représenter l'azimut de l'antenne
            folium.PolyLine(
                locations=[
                    [row['latitude'], row['longitude']],
                    [row['latitude'] + 0.01 * math.sin(math.radians(azimuth)), row['longitude'] + 0.01 * math.cos(math.radians(azimuth))]],
                color="black",
                weight=2.5,
            ).add_to(m)
    
    return m

def compute_and_show_antenna_density(lat, lon, radius, operators):
    
    generations = ["2G", "3G", "4G", "5G"]
    result = calculate_density(operators, generations, lat, lon, radius, anfr_last_modified_date, update_progressbar)
    
    if isinstance(result, str):  # Si result est une chaîne de caractères, cela signifie qu'une erreur s'est produite
        print(result)  # Ou affichez l'erreur de la manière qui convient le mieux à votre application
    else:
        densities, antenna_counts, oriented_antennas = result
        display_antenna_density_results(operators, generations, densities, antenna_counts, oriented_antennas)  # Ajoutez oriented_antennas ici

        # Après avoir calculé densities, antenna_counts, oriented_antennas
        #map_ = create_map(densities, lat, lon, radius)
        #map_.save("map.html")  # Sauvegarde la carte en HTML


worker_thread = None




def start_density_calculation():
    global worker_thread

    if worker_thread is not None and worker_thread.is_alive():
        text_result.delete(1.0, tk.END)
        text_result.insert(tk.END, "Un calcul est déjà en cours. Veuillez attendre la fin du calcul en cours.")
        return

    lat = lat_entry.get()
    lon = lon_entry.get()
    radius = radius_entry.get()
    operators = [operator_listbox.get(i) for i in operator_listbox.curselection()]

    validation_result = validate_inputs(lat, lon, radius)
    if isinstance(validation_result, str):
        text_result.delete(1.0, tk.END)
        text_result.insert(tk.END, validation_result)
        return
    else:
        lat, lon, radius = validation_result

    worker_thread = Thread(target=lambda: compute_and_show_antenna_density(lat, lon, radius, operators))
    worker_thread.start()

if __name__ == "__main__":
    logging.basicConfig(filename='app.log', filemode='w', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    geodata = get_geolocation_info()
    if geodata:

        root = tk.Tk()

        root.title("Calcul de densité d'antennes")

        label_last_modified_date = tk.Label(root)
        anfr_last_modified_date = get_anfr_data_last_modified_date()
        label_last_modified_date.config(text=f"Dernière mise à jour des données ANFR : {anfr_last_modified_date}")
        label_last_modified_date.pack()
        
        antenna_last_modified_date = get_antenna_data_last_modified_date()
        label_antenna_last_modified_date = tk.Label(root)
        label_antenna_last_modified_date.config(text=f"Dernière mise à jour des orientations des antennes : {antenna_last_modified_date}")
        label_antenna_last_modified_date.pack()


        lat_label = tk.Label(root, text="Latitude (en degrés) :")
        lat_label.pack()
        lat_entry = tk.Entry(root)
        lat_entry.insert(0, str(geodata['lat']))  # Pré-remplir avec la latitude
        lat_entry.pack()

        lon_label = tk.Label(root, text="Longitude (en degrés) :")
        lon_label.pack()
        lon_entry = tk.Entry(root)
        lon_entry.insert(0, str(geodata['lon']))  # Pré-remplir avec la longitude
        lon_entry.pack()

        radius_label = tk.Label(root, text="Rayon (en kilomètres) :")
        radius_label.pack()
        radius_entry = tk.Entry(root)
        radius_entry.insert (0, 1)   # Pré-remplir avec 1
        radius_entry.pack()

        operator_listbox = tk.Listbox(root, selectmode=tk.MULTIPLE)
        for operator in OPERATORS:
            operator_listbox.insert(tk.END, operator)
        operator_listbox.pack()

        button_calculate_density = tk.Button(root, text="Calculer la densité", command=start_density_calculation)
        button_calculate_density.pack()

        progressbar = ttk.Progressbar(root, length=100, mode='determinate')
        progressbar.pack()

        text_result = tk.Text(root)
        text_result.pack()

        root.mainloop()

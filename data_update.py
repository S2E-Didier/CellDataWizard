import json
import logging
import os
from datetime import datetime

import requests
from requests.exceptions import RequestException

# Constantes
URL_BASE = "https://data.anfr.fr/api/records/2.0/downloadfile/format=json&refine.statut=En+service&refine.statut=Techniquement+op%C3%A9rationnel&resource_id=88ef0887-6b0f-4d3f-8545-6d64c8f597da"

# Configuration de la journalisation
logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# Liste des opérateurs de téléphonie mobile
OPERATORS = [
    "ORANGE",
    "BOUYGUES TELECOM",
    "SFR",
    "FREE MOBILE",
    "DIGICEL",
    "FREE CARAIBES",
    "SFR CARAIBES",
]

# Liste des générations de technologies mobiles
GENERATIONS = ["2G", "3G", "4G", "5G"]

# Cartographie des noms d'opérateurs
OPERATOR_NAME_MAPPING = {
    "SFR CARAIBES": "OUTREMER TELECOM",
}

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

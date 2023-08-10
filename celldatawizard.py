import json
import logging
import math
import os
from datetime import datetime
from threading import Thread

from concurrent.futures import ThreadPoolExecutor
import folium


import numpy as np
import pandas as pd
import requests
from geopy.distance import distance
from requests.exceptions import RequestException
from shapely.geometry import Point
import tkinter as tk
from tkinter import ttk

import augmented_data
from augmented_data import get_antenna_data_last_modified_date
from data_update import (download_antenna_data, get_anfr_data_last_modified_date, 
                         read_antenna_data, retrieve_or_update_antenna_data)

URL_LAST_MODIFIED = "https://data.anfr.fr/anfr/visualisation/information/?id=dd11fac6-4531-4a27-9c8c-a3a9e4ec2107&refine.statut=En+service&refine.statut=Techniquement+op%C3%A9rationnel"
LOCAL_DATA_DIR = "local_antenna_data"
AUGMENTED_DATA_DIR = 'local_antenna_data_augmented'

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

    data = augmented_data.get_data()
    if data is not None:
        augmented_data.update_csv_file(data)
        augmented_data.process_json_files()

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

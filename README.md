
## Description:

L'application "Densité d'antennes" est conçue pour calculer et afficher la densité et le nombre d'antennes mobiles pour différents opérateurs et générations d'antennes (2G, 3G, 4G, 5G) dans un rayon spécifié autour d'un point d'intérêt défini par ses coordonnées (latitude et longitude). L'application utilise l'API ANFR (Agence nationale des fréquences) pour récupérer les données des stations mobiles en France. L'orientation des antennes de ces stations mobiles est récupérée sur le site data.gouv.fr dans le jeu de données [sur les installations radioélectriques de plus de 5 watts](https://www.data.gouv.fr/fr/datasets/donnees-sur-les-installations-radioelectriques-de-plus-de-5-watts-1/#/resources)

L'application utilise un système de mise en cache local pour éviter de télécharger à nouveau les mêmes données d'antennes à chaque fois que le calcul est effectué si les données locales sont plus récentes que les données en ligne.

L'application préremplie les coordonnées géographiques en se basant sur l'IP.

## Manuel d'installation:

1. Assurez-vous d'avoir installé Python 3.6 ou une version ultérieure sur votre ordinateur. Vous pouvez le télécharger et l'installer à partir du site officiel: https://www.python.org/downloads/

2. Installez les bibliothèques nécessaires en ouvrant un terminal et en exécutant la commande suivante:
```
pip install numpy pandas requests geopy shapely tkinter
```

3. Téléchargez et copiez les fichiers `densite_antennes.py`, `data_update.py` et `augmented_data.py` dans le même répertoire

4. Pour exécuter l'application, ouvrez un terminal, accédez au répertoire où se trouve le fichier `densite_antennes.py`, puis exécutez la commande suivante:
```
python densite_antennes.py
```

## Manuel d'utilisation:

1.  **Lancement du script :** Lancez le script en utilisant la commande `python densite_antennes.py` dans votre terminal ou invite de commande. Cela ouvrira une interface utilisateur graphique.
    
2.  **Entrée des coordonnées :** Dans les champs de texte appropriés, entrez la latitude (entre -90 et 90) et la longitude (entre -180 et 180) de l'emplacement pour lequel vous souhaitez calculer la densité d'antennes.
    
3.  **Définition du rayon :** Entrez le rayon (en kilomètres) autour de l'emplacement pour lequel vous souhaitez calculer la densité d'antennes. Le rayon doit être compris entre 0 et 10 000 km.
    
4.  **Sélection des opérateurs :** Dans la liste des opérateurs, sélectionnez les opérateurs pour lesquels vous souhaitez calculer la densité d'antennes. Vous pouvez sélectionner plusieurs opérateurs en maintenant la touche Ctrl (ou Cmd sur Mac) enfoncée tout en cliquant sur les noms des opérateurs.
    
5.  **Calcul de la densité :** Cliquez sur le bouton "Calculer la densité" pour commencer le calcul. Le script commencera à télécharger les données des antennes des opérateurs sélectionnés et à calculer la densité d'antennes. Pendant ce temps, la barre de progression vous indiquera l'avancement du processus.
    
6.  **Visualisation des résultats :** Une fois le calcul terminé, les résultats s'afficheront dans la zone de texte située en bas de l'application. Pour chaque opérateur et chaque génération de réseau (2G, 3G, 4G, 5G), le script affiche la densité d'antennes (en nombre d'antennes par km²) et le nombre total d'antennes dans la zone spécifiée.
    
7.  **Nouveau calcul :** Pour effectuer un nouveau calcul, il suffit de modifier les coordonnées, le rayon et/ou les opérateurs sélectionnés, puis de cliquer à nouveau sur le bouton "Calculer la densité".
    

Note :

 - Les données d'antennes sont téléchargées à partir de l'API ANFR et  	du site data.gouv.fr et sont mises à jour régulièrement. La date et    l'heure de la dernière mise à jour des données sont affichées en haut   de l'application. Si vous lancez un nouveau calcul peu de temps après   un précédent, le script utilisera les données déjà téléchargées, à condition qu'elles soient toujours à jour.
 - **L'application nécessite une connexion internet**

# API ANFR

L'API ANFR (Agence nationale des fréquences) est une interface de programmation d'application fournie par l'Agence nationale des fréquences française. L'ANFR est un établissement public responsable de la régulation et de la planification des fréquences radioélectriques en France. L'API ANFR permet d'accéder aux données relatives aux sites d'antennes-relais de téléphonie mobile en France.

L'API ANFR est basée sur la plateforme OpenDataSoft, qui facilite l'accès, le partage et l'utilisation de données ouvertes. Vous pouvez accéder aux données de l'API ANFR en effectuant des requêtes HTTP GET et en spécifiant les paramètres appropriés dans l'URL. Les données sont généralement retournées au format JSON, mais d'autres formats sont également disponibles.

Dans le code que nous avons travaillé précédemment, l'URL de base pour accéder aux données des antennes-relais est la suivante :

```python

url_base = "https://data.anfr.fr/api/records/2.0/downloadfile/format=json&refine.statut=En+service&refine.statut=Techniquement+op%C3%A9rationnel&resource_id=88ef0887-6b0f-4d3f-8545-6d64c8f597da"

```

Les paramètres spécifiés dans cette URL sont :

- `format=json` : Indique que les données doivent être retournées au format JSON.

- `refine.statut=En+service` et `refine.statut=Techniquement+op%C3%A9rationnel` : Permet de filtrer les antennes-relais dont le statut est "En service" ou "Techniquement opérationnel".

- `resource_id=88ef0887-6b0f-4d3f-8545-6d64c8f597da` : Identifiant unique de la ressource (ensemble de données) des sites d'antennes-relais sur la plateforme OpenDataSoft.


Pour filtrer les données en fonction de l'opérateur et de la génération de l'antenne, nous utilisons les paramètres `refine.adm_lb_nom` et `refine.generation` :

```python

url = f"{url_base}&refine.adm_lb_nom={operator}&refine.generation={generation}"

```

Ensuite, nous utilisons la bibliothèque `requests` pour effectuer une requête GET et récupérer les données :


```python

data = requests.get(url).json()

```

Les données retournées par l'API ANFR sont ensuite utilisées pour calculer la densité d'antennes et le nombre d'antennes dans un rayon spécifique pour chaque opérateur et chaque génération d'antennes.


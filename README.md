# README - Projet de Gestion de Données

Ce projet utilise Python pour extraire, manipuler et visualiser des données à partir de fichiers CSV. Les fichiers Python fournissent des fonctionnalités variées, notamment l'importation de données (initfiles.py), la création de graphiques (01graph.py, 02graph.py et 03graph.py), et la fusion de données( 01.py, 02.py, 03.py). Suivez les instructions ci-dessous pour configurer et exécuter les fichiers correctement.

## Prérequis

Avant de commencer, assurez-vous d'avoir installé les éléments suivants sur votre machine :

1. **Python 3.12** - Téléchargez et installez Python à partir de [python.org](https://www.python.org/downloads/).
2. **Bibliothèques Python** - Installez les bibliothèques nécessaires en exécutant :
   ```bash
   pip install pandas sqlalchemy unidecode configparser xlsxwriter numpy mysql-connector-python requests
Serveur MySQL - Vous devez disposer d'un serveur MySQL en cours d'exécution. Vous pouvez utiliser MAMP ou XAMPP par exemple.

Assurez-vous que le serveur MySQL est démarré.

Configuration
Fichier de configuration (config.ini) :

Modifiez si necessaire le fichier nommé config.ini à la racine de votre projet:

ini
[database]
host = localhost
user = root
password = 
database = statsdb5
port = 3306
host : L'adresse du serveur MySQL. Utilisez localhost si le serveur s'exécute sur votre machine.
user : Le nom d'utilisateur de la base de données. Le nom d'utilisateur par défaut est généralement root.
password : Le mot de passe pour le nom d'utilisateur de la base de données. Si vous n'avez pas défini de mot de passe, laissez-le vide.
database : Le nom de la base de données que vous allez utiliser (par exemple, statsdb5).
port : Le port MySQL (par défaut, c'est 3306).

Création de la base de données :

Après avoir renseigné le fichier config.ini pour coller à votre configuration, executez le fichier initfiles.py.

Exécution des fichiers Python
Les fichiers Python doivent être exécutés dans un ordre spécifique pour garantir que les données sont traitées correctement. Voici l'ordre d'exécution :

Assurez vous d'avoir bien éxécuté le fichier initfiles.py, sans quoi les données ne seront pas présente dans votre base mysql.

# Étapes d'enrichissement des données

## 1. Exécution des scripts d'enrichissement initial

- **Exécutez le fichier** `01.py`. 
  - Ce script va récupérer les codes géographiques (codegeo) pour chaque client et les enregistrer dans une nouvelle table appelée `01eg_insee_iris`.
  
- **Ensuite, exécutez le fichier** `01graph.py`.
  - Ce script va extraire les informations contenues dans `01eg_insee_iris` et construire un fichier Excel nommé `01enriched_clients_with_charts.xlsx`, qui contiendra des graphiques basés sur ces données.

## 2. Exécution des scripts d'enrichissement par l'âge

- **Exécutez le fichier** `02.py`.
  - Ce script va récupérer les codes géographiques pour chaque client et les enregistrer dans une nouvelle table appelée `02eg_insee_iris`.

- **Ensuite, exécutez le fichier** `02graph.py`.
  - Ce script va extraire les informations contenues dans `02eg_insee_iris` et construire un fichier Excel nommé `02enriched_clients_with_charts.xlsx`, qui contiendra des graphiques basés sur ces données.

## 3. Exécution des scripts d'enrichissement géomarketing

- **Exécutez le fichier** `03.py`.
  - Ce script va également récupérer les codes géographiques pour chaque client, mais cette fois-ci, les informations seront enregistrées dans une nouvelle table appelée `03eg_insee_iris`.

- **Enfin, exécutez le fichier** `03graph.py`.
  - Ce dernier script va utiliser les données de `03eg_insee_iris` pour créer un fichier Excel nommé `03enriched_clients_with_charts.xlsx`, incluant des graphiques pertinents.


Résultats
Après l'exécution des scripts, des fichiers Excels seront créés (par exemple, 03enriched_clients_with_charts.xlsx) contenant des graphiques et des analyses basés sur les données de votre base de données.
Aussi, des tables contenant les résultats de la fonction seront créées dans votre base de données.

Dépannage
Problèmes de connexion à la base de données : Vérifiez que votre serveur MySQL est en cours d'exécution et que les détails dans config.ini sont corrects.
Erreurs lors de l'exécution des scripts : Assurez-vous que toutes les bibliothèques nécessaires sont installées et que les tables sont bien présentes dans la base de données. Assurez-vous également que les noms des colonnes sont correctes.
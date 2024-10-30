import os
import pandas as pd
import json
import mysql.connector
import numpy as np
import re
import requests
import configparser

# Load configuration
config = configparser.ConfigParser()
config.read("config.ini")

DB_CONFIG = {
    "host": config["database"]["host"],
    "user": config["database"]["user"],
    "password": config["database"]["password"],
    "database": config["database"]["database"],
    "port": config["database"].getint("port", fallback=3306),
}

# Function to normalize column names
def normalize_column_name(col_name):
    col_name = col_name.lower()
    col_name = re.sub(r"[^a-z0-9]", "_", col_name)
    return col_name

# Function to infer SQL data types based on pandas column data types
def infer_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "DECIMAL(10, 2)"
    else:
        return "VARCHAR(255)"

# Function to generate CREATE TABLE SQL queries
def generate_sql_create_table(file_name, df):
    table_name = os.path.splitext(file_name)[0].lower()
    sql_columns = []

    for col in df.columns:
        sql_type = infer_sql_type(df[col].dtype)
        column_definition = f"{col} {sql_type}"
        sql_columns.append(column_definition)

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        {', '.join(sql_columns)}
    );
    """
    return create_table_query

# Function to download file if missing
def download_if_missing(file_path, url):
    if not os.path.exists(file_path):
        print(f"Téléchargement de {os.path.basename(file_path)} depuis {url}...")
        response = requests.get(url, verify=False)
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f"{os.path.basename(file_path)} téléchargé avec succès.")
    else:
        print(f"{os.path.basename(file_path)} est déjà présent dans le dossier.")

# Dictionary to store SQL queries
def create_json_queries(directory):
    sql_queries = {}
    for file_name in os.listdir(directory):
        if file_name.endswith(".csv"):
            file_path = os.path.join(directory, file_name)
            try:
                df = pd.read_csv(file_path, low_memory=False)
                df.columns = [normalize_column_name(col) for col in df.columns]
                create_table_query = generate_sql_create_table(file_name, df)
                sql_queries[file_name] = create_table_query
            except Exception as e:
                print(f"Erreur lors de la lecture de {file_name} : {e}")

    with open("tables_script.json", "w") as f:
        json.dump(sql_queries, f, indent=4)

    print("Les requêtes SQL ont été générées et sauvegardées dans 'tables_script.json'.")

def create_database_if_not_exists(connection, db_name):
    cursor = connection.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    cursor.close()
    connection.database = db_name

# Database connection
def connect_to_db():
    return mysql.connector.connect(
        host=DB_CONFIG["host"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        port=DB_CONFIG["port"],
    )

# Create tables from JSON file
def create_tables_from_json(json_file, connection):
    with open(json_file, "r") as f:
        sql_queries = json.load(f)

    cursor = connection.cursor()
    for table, create_query in sql_queries.items():
        try:
            cursor.execute(create_query)
            print(f"Table '{table}' créée avec succès.")
        except mysql.connector.Error as err:
            print(f"Erreur lors de la création de la table '{table}': {err}")
    connection.commit()

# Insert data from CSV file
def insert_data_from_csv(file_name, df, table_name, connection, batch_size=1000):
    df.columns = [normalize_column_name(col) for col in df.columns]
    df = df.replace({np.nan: None})

    cursor = connection.cursor()
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    data = df.values.tolist()

    for i in range(0, len(data), batch_size):
        batch_data = data[i : i + batch_size]
        try:
            cursor.executemany(insert_query, batch_data)
        except mysql.connector.Error as err:
            print(f"Erreur lors de l'insertion dans la table '{table_name}': {err}")
            for row in batch_data:
                print(f"Ligne problématique : {row}")
                break
            raise Exception(f"Programme stoppé à cause d'une erreur dans la table '{table_name}'")

    connection.commit()

# Directory path for CSV files
directory = os.path.join(os.getcwd(), "output_data")
os.makedirs(directory, exist_ok=True)

# Check and download required CSV files
download_if_missing(
    os.path.join(directory, "refCP.csv"),
    "https://raw.githubusercontent.com/AurelienLELEU/GeoEnrichissmentStats/a16b696e86907f812d563515b05e15751330a1b3/refCP.csv",
)
download_if_missing(
    os.path.join(directory, "Ref_IRIS_geo2024.csv"),
    "https://raw.githubusercontent.com/AurelienLELEU/GeoEnrichissmentStats/a16b696e86907f812d563515b05e15751330a1b3/Ref_IRIS_geo2024.csv",
)

# Generate SQL queries and create JSON file
create_json_queries(directory)

# Connect to the database and process data
connection = connect_to_db()
create_database_if_not_exists(connection, DB_CONFIG["database"])
create_tables_from_json("tables_script.json", connection)

# Insert data into respective tables
for file_name in os.listdir(directory):
    if file_name.endswith(".csv"):
        file_path = os.path.join(directory, file_name)
        table_name = os.path.splitext(file_name)[0].lower()
        try:
            df = pd.read_csv(file_path, low_memory=False)
            df.columns = [normalize_column_name(col) for col in df.columns]
            insert_data_from_csv(file_name, df, table_name, connection)
        except Exception as e:
            print(f"Erreur lors du traitement de {file_name} : {e}")

connection.close()
print("Processus terminé.")

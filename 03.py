# Import the necessary libraries
import pandas as pd
from sqlalchemy import create_engine
import configparser

# Load configuration from config.ini
config = configparser.ConfigParser()
config.read("./config.ini")

# Extract database credentials
DB_CONFIG = {
    "host": config["database"]["host"],
    "user": config["database"]["user"],
    "password": config["database"]["password"],
    "database": config["database"]["database"],
    "port": config["database"].get("port", "3306"),
}

# Construct the database URL for SQLAlchemy
db_url = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

# Create a connection to the MySQL database using SQLAlchemy
engine = create_engine(db_url)

# Load the '01eg_insee_iris' table from the database into a Pandas DataFrame
enriched_clients = pd.read_sql_table("01eg_insee_iris", con=engine)

# Load the 'maj_2014_references' table from the database into another DataFrame
maj_reference = pd.read_sql_table("maj_2014_references", con=engine)

# Merge the two DataFrames on the 'codgeo' column with a left join
merged_df = pd.merge(enriched_clients, maj_reference, on="codgeo", how="left")

# Define the name for the new table to be created in the MySQL database
new_table_name = "03enriched_clients_with_references"

# Save the merged DataFrame as a new table in the database
# Replace the table if it already exists
merged_df.to_sql(new_table_name, con=engine, index=False, if_exists="replace")

# Print a confirmation message indicating that the table was created successfully
print(f"Table '{new_table_name}' créée avec succès dans la base de données.")

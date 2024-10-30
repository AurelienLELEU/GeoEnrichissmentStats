import re
import pandas as pd
from sqlalchemy import create_engine, text
from unidecode import unidecode
import configparser

# Load configuration
config = configparser.ConfigParser()
config.read("config.ini")

DB_CONFIG = {
    "host": config["database"]["host"],
    "user": config["database"]["user"],
    "password": config["database"]["password"],
    "database": config["database"]["database"],
    "port": config["database"].get("port", "3306"),
}

# Construct the database URL for SQLAlchemy
db_url = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"


def EG_Insee_Iris(
    table_entree,
    top_tnp,
    civilite=None,
    prenom=None,
    nom=None,
    complement_nom=None,
    adresse=None,
    complement_adrs=None,
    lieu_dit=None,
    cp=None,
    ville=None,
    id_client=None,
    pays=None,
    email=None,
    tel=None,
):
    """Enrichit un DataFrame avec des données INSEE et IRIS.

    Args:
        table_entree (pd.DataFrame): DataFrame d'entrée.
        top_tnp (int): Indicateur pour déterminer la logique d'analyse des noms.
        civilite, prenom, nom, ... (str, optional): Noms de colonnes pour divers champs. Par défaut à None.

    Returns:
        pd.DataFrame: DataFrame enrichi.
    """
    # Connexion à la base de données
    engine = create_engine(db_url)
    REFCPDF = pd.read_sql_table("refcp", con=engine)
    REFIRISGEO2024DF = pd.read_sql_table("ref_iris_geo2024", con=engine)

    def normalize_column_name(col_name):
        if col_name is None:
            return None
        col_name = col_name.lower()
        col_name = re.sub(r"[^a-z0-9]", "_", col_name)
        return col_name

    enriched_df = table_entree.copy()

    if top_tnp == 1:
        split_names = enriched_df[nom].str.split(expand=True)
        enriched_df["gender"] = split_names[0]
        enriched_df["prenom"] = split_names[1]
        enriched_df["nom"] = split_names[2]
    else:
        columns_to_select = [
            col
            for col in [
                civilite,
                prenom,
                nom,
                complement_nom,
                adresse,
                complement_adrs,
                lieu_dit,
                cp,
                ville,
                id_client,
                pays,
                email,
                tel,
            ]
            if col is not None
        ]
        normalized_columns = [normalize_column_name(col) for col in columns_to_select]
        enriched_df = enriched_df[normalized_columns]

    enriched_df.columns = [
        normalize_column_name(col) for col in enriched_df.columns if col is not None
    ]

    enriched_df["ville_normalized"] = enriched_df["ville"].apply(
        lambda x: unidecode(str(x).lower().replace("-", " "))
    )
    REFCPDF["nom_de_la_commune_normalized"] = REFCPDF["nom_de_la_commune"].apply(
        lambda x: unidecode(str(x).lower().replace("-", " "))
    )

    enriched_df["lieu_dit_normalized"] = enriched_df["lieu_dit"].apply(
        lambda x: unidecode(str(x).lower().replace("-", " ")) if pd.notna(x) else ""
    )
    REFIRISGEO2024DF["lib_iris_normalized"] = REFIRISGEO2024DF["lib_iris"].apply(
        lambda x: unidecode(str(x).lower().replace("-", " ")) if pd.notna(x) else ""
    )

    enriched_df = enriched_df.merge(
        REFCPDF[
            [
                "code_postal",
                "code_commune_insee",
                "nom_de_la_commune",
                "nom_de_la_commune_normalized",
            ]
        ],
        how="left",
        left_on=["cp", "ville_normalized"],
        right_on=["code_postal", "nom_de_la_commune_normalized"],
    )
    enriched_df["c_insee"] = enriched_df["code_commune_insee"]

    enriched_df["c_insee"] = enriched_df["c_insee"].apply(
        lambda x: f"0{x}" if pd.notna(x) and len(str(x)) == 4 else x
    )

    enriched_df = enriched_df.merge(
        REFIRISGEO2024DF[["depcom", "lib_iris", "lib_iris_normalized", "code_iris"]],
        how="left",
        left_on=["c_insee", "lieu_dit_normalized"],
        right_on=["depcom", "lib_iris_normalized"],
    )

    enriched_df["c_iris"] = enriched_df["code_iris"].str[-4:].fillna("0000")

    enriched_df["c_qualite_iris"] = enriched_df.apply(
        lambda row: (
            1
            if pd.notna(row["code_iris"]) and pd.notna(row["c_insee"])
            else (2 if pd.notna(row["c_insee"]) else 8)
        ),
        axis=1,
    )

    enriched_df["codgeo"] = enriched_df["c_insee"].fillna("") + enriched_df["c_iris"]

    original_columns = enriched_df.columns.tolist()
    enriched_df = enriched_df[original_columns].drop(
        columns=[
            "ville_normalized",
            "lieu_dit_normalized",
            "nom_de_la_commune_normalized",
            "lib_iris_normalized",
        ]
    )

    # Enregistrer dans la base de données, en supprimant la table si elle existe
    table_name = "01eg_insee_iris"

    with engine.connect() as connection:
        with connection.begin():
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

        enriched_df.to_sql(table_name, con=engine, index=False, if_exists="replace")

    return enriched_df


# Exemple d'utilisation

# Connexion à la base de données
engine = create_engine(db_url)

enriched_table = EG_Insee_Iris(
    table_entree=pd.read_sql_table("true_table_entree", con=engine),
    top_tnp=0,
    cp="cp",
    ville="ville",
    id_client="id_client",
    lieu_dit="lieu_dit",
    civilite="civilit_",
    nom="nom",
    prenom="prenom",
)

print(enriched_table)

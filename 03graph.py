# Import necessary libraries
import pandas as pd
import xlsxwriter
from sqlalchemy import create_engine
import configparser
import random

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

# Load the 'enriched_clients_with_references' table into a DataFrame
df = pd.read_sql_table("03enriched_clients_with_references", con=engine)

# Define the output file name for the Excel file
output_file = "03enriched_clients_with_charts.xlsx"

# Create an Excel writer with xlsxwriter
writer = pd.ExcelWriter(output_file, engine="xlsxwriter")
workbook = writer.book


# Function to add sheets with Excel charts
def add_sheet_with_excel_chart(
    sheet_name, columns, graph_data, x_col, y_col, chart_type="column"
):
    # Filter required columns for the sheet
    filtered_df = df[columns]
    # Write filtered data to the Excel sheet
    filtered_df.to_excel(
        writer, sheet_name=sheet_name, index=False, startrow=0, startcol=0
    )
    # Write chart data below the filtered data
    graph_data.to_excel(
        writer,
        sheet_name=sheet_name,
        index=False,
        startrow=len(filtered_df) + 2,
        startcol=0,
    )
    worksheet = writer.sheets[sheet_name]

    # Create the chart in Excel
    chart = workbook.add_chart({"type": chart_type})
    chart.add_series(
        {
            "name": y_col,
            "categories": [
                sheet_name,
                len(filtered_df) + 3,
                0,
                len(filtered_df) + 3 + len(graph_data) - 1,
                0,
            ],
            "values": [
                sheet_name,
                len(filtered_df) + 3,
                1,
                len(filtered_df) + 3 + len(graph_data) - 1,
                1,
            ],
            "fill": {"color": random.choice(["blue", "pink", "black", "red", "green"])},
        }
    )

    # Set chart titles and axis labels
    chart.set_title({"name": sheet_name + " Graph"})
    chart.set_x_axis({"name": x_col})
    chart.set_y_axis({"name": y_col})

    # Insert the chart into the Excel sheet
    worksheet.insert_chart("D2", chart)


# Creating and adding multiple sheets with charts based on selected indicators

# Sheet for average income by city
mean_revenue_by_city = (
    df.groupby("ville")["rev"].mean().reset_index(name="Moyenne Revenu")
)
add_sheet_with_excel_chart(
    "Moyenne Revenu par Ville",
    columns=["ville", "rev"],
    graph_data=mean_revenue_by_city,
    x_col="Ville",
    y_col="Moyenne Revenu",
)

# Sheet for housing type distribution
logement_distribution = (
    df[["propr", "locat", "locat_hlm"]].sum().reset_index(name="Counts")
)
logement_distribution.columns = ["Type Logement", "Counts"]
add_sheet_with_excel_chart(
    "Répartition Type Logement",
    columns=["propr", "locat", "locat_hlm"],
    graph_data=logement_distribution,
    x_col="Type Logement",
    y_col="Counts",
    chart_type="bar",
)

# Sheet for average housing quality by commune
mean_logement_quality_by_commune = (
    df.groupby("nom_de_la_commune")["c_indice_qualite_logement"]
    .mean()
    .reset_index(name="Qualité Logement Moyenne")
)
add_sheet_with_excel_chart(
    "Qualité Logement par Commune",
    columns=["nom_de_la_commune", "c_indice_qualite_logement"],
    graph_data=mean_logement_quality_by_commune,
    x_col="Commune",
    y_col="Qualité Logement Moyenne",
)

# Sheet for education level distribution
education_distribution = (
    df[["et_niv0", "et_niv1", "et_niv2"]].sum().reset_index(name="Counts")
)
education_distribution.columns = ["Niveau Éducation", "Counts"]
add_sheet_with_excel_chart(
    "Répartition Niveau Éducation",
    columns=["et_niv0", "et_niv1", "et_niv2"],
    graph_data=education_distribution,
    x_col="Niveau Éducation",
    y_col="Counts",
    chart_type="column",
)

# Sheet for single-parent family rate by commune
familles_mono_by_commune = (
    df.groupby("nom_de_la_commune")["tx_fammono"]
    .mean()
    .reset_index(name="Taux Familles Monoparentales")
)
add_sheet_with_excel_chart(
    "Familles Monoparentales",
    columns=["nom_de_la_commune", "tx_fammono"],
    graph_data=familles_mono_by_commune,
    x_col="Commune",
    y_col="Taux Familles Monoparentales",
)

# Sheet for couple type distribution
couple_distribution = (
    df[["tx_coupsenf", "tx_coupaenf"]].sum().reset_index(name="Counts")
)
couple_distribution.columns = ["Type Couple", "Counts"]
add_sheet_with_excel_chart(
    "Répartition Type Couple",
    columns=["tx_coupsenf", "tx_coupaenf"],
    graph_data=couple_distribution,
    x_col="Type Couple",
    y_col="Counts",
    chart_type="bar",
)

# Sheet for average income quality by city
revenue_quality_by_city = (
    df.groupby("ville")["c_indice_qualite_rev"]
    .mean()
    .reset_index(name="Qualité Revenu Moyenne")
)
add_sheet_with_excel_chart(
    "Qualité Revenu par Ville",
    columns=["ville", "c_indice_qualite_rev"],
    graph_data=revenue_quality_by_city,
    x_col="Ville",
    y_col="Qualité Revenu Moyenne",
)

# Close the Excel writer to generate the file
writer.close()
print(f"Fichier Excel '{output_file}' créé avec succès.")

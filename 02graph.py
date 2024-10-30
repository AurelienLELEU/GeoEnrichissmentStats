import pandas as pd
import xlsxwriter
from sqlalchemy import create_engine
import configparser

# Load configuration from config.ini
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

# Connect to the MySQL database
engine = create_engine(db_url)

# Load data from the age_sexe_results table
df = pd.read_sql_table("02eg_age_sexe", con=engine)

# Create an Excel file with XlsxWriter
output_file = "02enriched_clients_with_charts.xlsx"  # Output Excel file name
writer = pd.ExcelWriter(output_file, engine="xlsxwriter")
workbook = writer.book


def add_sheet_with_excel_chart(sheet_name, columns, graph_data, x_col, y_col):
    # Add selected columns to a sheet
    filtered_df = df[columns]
    filtered_df.to_excel(
        writer, sheet_name=sheet_name, index=False, startrow=0, startcol=0
    )

    # Write chart data to the sheet
    graph_data.to_excel(
        writer,
        sheet_name=sheet_name,
        index=False,
        startrow=len(filtered_df) + 2,
        startcol=0,
    )

    # Get the sheet object
    worksheet = writer.sheets[sheet_name]

    # Create a column chart
    chart = workbook.add_chart({"type": "column"})

    # Configure the chart series (chart data)
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
        }
    )

    # Set the chart title and axis labels
    chart.set_title({"name": sheet_name + " Graph"})
    chart.set_x_axis({"name": x_col})
    chart.set_y_axis({"name": y_col})

    # Insert the chart into the Excel sheet
    worksheet.insert_chart("D2", chart)


# Sheet 1 - Chart of birth year by first name
sheet_1_data = (
    df.groupby("prenom")["e_annee_naissance"]
    .mean()
    .reset_index(name="Année Moyenne de Naissance")
)
add_sheet_with_excel_chart(
    "Année de Naissance",
    columns=["prenom", "e_annee_naissance"],
    graph_data=sheet_1_data,
    x_col="prenom",
    y_col="Année Moyenne de Naissance",
)

# Close the Excel file
writer.close()
print(f"Fichier Excel '{output_file}' créé avec succès.")

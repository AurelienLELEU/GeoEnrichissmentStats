import pandas as pd
import xlsxwriter
from sqlalchemy import create_engine
import configparser

# Load configuration from config.ini
config = configparser.ConfigParser()
config.read("./config.ini")

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

# Load data from the table '01eg_insee_iris'
df = pd.read_sql_table("01eg_insee_iris", con=engine)

# Create an Excel file with XlsxWriter
output_file = "01enriched_clients_with_charts.xlsx"  # Output Excel file name
writer = pd.ExcelWriter(output_file, engine="xlsxwriter")
workbook = writer.book


def add_sheet_with_excel_chart(
    sheet_name,
    columns,
    graph_data,
    x_col,
    y_cols,
    chart_type="column",
    is_percentage=False,
):
    # Add selected columns to a sheet
    filtered_df = df[columns]
    filtered_df.to_excel(
        writer, sheet_name=sheet_name, index=False, startrow=0, startcol=0
    )

    # Write graph data to the sheet
    graph_data.to_excel(
        writer,
        sheet_name=sheet_name,
        index=False,
        startrow=len(filtered_df) + 2,
        startcol=0,
    )

    # Access the sheet object
    worksheet = writer.sheets[sheet_name]

    # Create a chart
    chart = workbook.add_chart({"type": chart_type})

    # Configure the chart series for each column
    for i, y_col in enumerate(y_cols):
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
                    i + 1,
                    len(filtered_df) + 3 + len(graph_data) - 1,
                    i + 1,
                ],
                "fill": {"color": "blue" if y_col == "Homme" else "pink"},
            }
        )

    # Configure labels and title
    chart.set_title({"name": sheet_name + " Graph"})
    chart.set_x_axis({"name": x_col})
    chart.set_y_axis({"name": "Pourcentage" if is_percentage else "Counts"})

    if is_percentage:
        chart.set_y_axis({"major_gridlines": {"visible": False}, "min": 0, "max": 100})
        chart.set_plotarea(
            {"grouping": "stacked"}
        )  # Stacked for percentage distribution

    # Insert the chart into the Excel sheet
    worksheet.insert_chart("D2", chart)


# Sheet 1 - Chart by civility
sheet_1_data = df["civilit_"].value_counts().reset_index(name="Counts")
sheet_1_data.columns = ["civilit_", "Counts"]
add_sheet_with_excel_chart(
    "Stats de Sexe",
    columns=["civilit_", "nom", "prenom"],
    graph_data=sheet_1_data,
    x_col="civilit_",
    y_cols=["Counts"],
)

# Sheet 2 - Chart by city
sheet_2_data = df["ville"].value_counts().reset_index(name="Counts")
sheet_2_data.columns = ["Ville", "Counts"]
add_sheet_with_excel_chart(
    "Stats de Ville",
    columns=["ville", "nom", "prenom"],
    graph_data=sheet_2_data,
    x_col="Ville",
    y_cols=["Counts"],
)

# Sheet 3 - Percentage of men and women
df["sexe"] = df["civilit_"].apply(
    lambda x: "Homme" if x in ["M", "Mr", "Monsieur"] else "Femme"
)
gender_percentage = (
    df["sexe"].value_counts(normalize=True).reset_index(name="Pourcentage")
)
gender_percentage.columns = ["Sexe", "Pourcentage"]
add_sheet_with_excel_chart(
    "Pourcentage Sexe",
    columns=["sexe"],
    graph_data=gender_percentage,
    x_col="Sexe",
    y_cols=["Pourcentage"],
    chart_type="pie",
)

# Sheet 4 - Number of men and women by city (counts)
gender_city_counts = (
    df.groupby(["ville", "sexe"]).size().unstack().fillna(0).reset_index()
)
add_sheet_with_excel_chart(
    "Sexe par Ville (Counts)",
    columns=["ville", "sexe"],
    graph_data=gender_city_counts,
    x_col="ville",
    y_cols=["Femme", "Homme"],
)

# Sheet 6 - Chart by INSEE code
sheet_6_data = df["c_insee"].value_counts().reset_index(name="Counts")
sheet_6_data.columns = ["Code INSEE", "Counts"]
add_sheet_with_excel_chart(
    "Stats par Code INSEE",
    columns=["c_insee"],
    graph_data=sheet_6_data,
    x_col="Code INSEE",
    y_cols=["Counts"],
)

# Close the Excel file
writer.close()
print(f"Fichier Excel '{output_file}' créé avec succès.")

"""
Escriba el codigo que ejecute la accion solicitada.
"""
import os
import zipfile

import pandas as pd

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months

    """
    
    input_folder = "files/input"
    output_folder = "files/output"

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    dataframes = []

    for i in range(10):

        zip_path = (f"{input_folder}/"f"bank-marketing-campaing-{i}.csv.zip")

        with zipfile.ZipFile(zip_path) as z:

            csv_name = z.namelist()[0]
            with z.open(csv_name) as file:

                df = pd.read_csv(file)
                dataframes.append(df)

    df = pd.concat(dataframes, ignore_index=True)

    # CLIENT

    client = pd.DataFrame()
    client["client_id"] = df["client_id"]
    client["age"] = df["age"]
    client["job"] = (df["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False))
    client["marital"] = df["marital"]
    client["education"] = (df["education"].str.replace(".", "_", regex=False).replace("unknown", pd.NA))
    client["credit_default"] = (df["credit_default"].map(lambda x: 1 if x == "yes" else 0))
    client["mortgage"] = (df["mortgage"].map(lambda x: 1 if x == "yes" else 0))
    client.to_csv(f"{output_folder}/client.csv",index=False,)

    # CAMPAIGN

    campaign = pd.DataFrame()
    campaign["client_id"] = df["client_id"]
    campaign["number_contacts"] = (df["number_contacts"])
    campaign["contact_duration"] = (df["contact_duration"])
    campaign["previous_campaign_contacts"] = (df["previous_campaign_contacts"])
    campaign["previous_outcome"] = (df["previous_outcome"].map(lambda x: 1 if x == "success" else 0))
    campaign["campaign_outcome"] = (df["campaign_outcome"].map(lambda x: 1 if x == "yes" else 0))

    month_map = {
        "jan": "01",
        "feb": "02",
        "mar": "03",
        "apr": "04",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "aug": "08",
        "sep": "09",
        "oct": "10",
        "nov": "11",
        "dec": "12",
    }

    campaign["last_contact_date"] = ("2022-"+ df["month"].map(month_map)+ "-"+ df["day"].astype(str).str.zfill(2))
    campaign.to_csv(f"{output_folder}/campaign.csv",index=False,)

    # ECONOMICS

    economics = pd.DataFrame()

    economics["client_id"] = df["client_id"]
    economics["cons_price_idx"] = (df["cons_price_idx"])
    economics["euribor_three_months"] = (df["euribor_three_months"])
    economics.to_csv(f"{output_folder}/economics.csv",index=False,)

    return


if __name__ == "__main__":
    clean_campaign_data()

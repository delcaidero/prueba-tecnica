"""
Este módulo es una prueba practica para 
realizar una etl de un dataset
"""

import os
import pandas as pd
from sqlalchemy import create_engine, inspect

DATASET_URL = "https://api.covidtracking.com/v1/states/daily.csv"


def extract(dataset_url):
    """
    Obtiene el dataset en formato csv y devuelve un dataframe
    Args:
    -----
        dataset_url ([str]): URL del archivo csv a descargar
    """

    df = pd.read_csv(dataset_url)
    return df


def transform(df):
    """
    Filtra el dataset y convierte la fecha en datetime.
    Devuelve modelo con las columnas indicadas
    Args:
    -----
        df ([pandas df]): [Description]]
    """
    df_transformed = df.copy()
    df_transformed = df_transformed[
        df_transformed.totalTestResultsSource == "totalTestsViral"
    ]
    df_transformed = df_transformed[["date", "state", "positive", "deathConfirmed"]]
    df_transformed["date"] = pd.to_datetime(
        df_transformed["date"], format="%Y%m%d", errors="ignore"
    )
    return df_transformed


def connect_db():
    # postgresql+psycopg2://<username>:<password>@<host>:<port>
    connection_uri = "postgresql+psycopg2://{}:{}@{}:{}".format(
        "docker",
        "docker",
        "127.0.0.1",
        "5432",
    )

    engine = create_engine(connection_uri, pool_pre_ping=True)
    engine.connect()
    return engine


def load_data_to_db(df, table_name, engine):
    df.to_sql(table_name, engine, if_exists="append")


if __name__ == "__main__":
    db_engine = connect_db()

    original_df = extract(DATASET_URL)
    original_data_table_name = "original_historic_values"
    print(original_df.head(1).to_string())

    clean_df = transform(original_df)
    clean_table_name = "clean_historic_values"
    print(clean_df.head(1).to_string())

    load_data_to_db(original_df, original_data_table_name, db_engine)
    load_data_to_db(clean_df, clean_table_name, db_engine)

    db_engine.dispose()

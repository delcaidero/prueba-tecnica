"""
Este módulo es una prueba practica para 
realizar una etl de un dataset
"""

import os
import logging
import pandas as pd
from functools import wraps
from sqlalchemy import create_engine, inspect


#logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(
    level=logging.DEBUG, filename="app.log", filemode="w", format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


DATASET_URL = "https://api.covidtracking.com/v1/states/daily.csv"


def assign_db_parameters_to(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        to_execute = fn('docker','docker','postgres','5432')       
        return to_execute
    return inner


def to_log(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        logging.debug(f">>> Running {fn.__name__!r} ")
        to_execute = fn(*args, **kwargs)
        logging.debug(f">>> Executed {fn.__name__!r} ")
        return to_execute
    return inner


@to_log
def extract(dataset_url):
    """
    Obtiene el dataset en formato csv y devuelve un dataframe
    Args:
    -----
        dataset_url ([str]): URL del archivo csv a descargar
    """
    df = pd.read_csv(dataset_url)
    return df


@to_log
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


@to_log
@assign_db_parameters_to
def connect_db(username,password,host,port):
    """
    Creamos la conexión a la BBDD
    """
    # postgresql+psycopg2://<username>:<password>@<host>:<port>
    connection_uri = "postgresql+psycopg2://{}:{}@{}:{}".format(
        username, password, host, port,
    )
    engine = create_engine(connection_uri, pool_pre_ping=True)
    engine.connect()
    return engine


@to_log
def load_data_to_db(df, table_name, engine):
    """
    Realiza la carga en BBDDD
    """
    df.to_sql(table_name, engine, if_exists="append")


def etl():
    """
    Función principal
    """
    db_engine = connect_db()

    original_df = extract(DATASET_URL)
    original_data_table_name = "original_historic_values"

    clean_df = transform(original_df)
    clean_table_name = "clean_historic_values_all_states"
    print(clean_df.head(1).to_string())

    load_data_to_db(original_df, original_data_table_name, db_engine)
    load_data_to_db(clean_df, clean_table_name, db_engine)

    db_engine.dispose()


if __name__ == "__main__":
    etl()

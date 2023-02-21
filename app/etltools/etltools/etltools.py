"""
Este módulo es una prueba practica para 
realizar una etl de un dataset
"""

import os
import logging
import pandas as pd
from functools import wraps
from sqlalchemy import create_engine, inspect


class etljob():
    '''
    Prueba practica: Script de ETL para el dataset de covidtracking.com

    Crea la conexión a BBDD con los parámetros indicados
    Extrae los datos del dataset "https://api.covidtracking.com/v1/states/daily.csv"
    Transforma los datos:
    - Filtra los registros totalTestResultsSource == "totalTestsViral"
    - Extrae las columnas: ["date", "state", "positive", "deathConfirmed"]
    - Convierte "date" a DateTime
    Crea las siguientes tablas y carga los valores originales y los transformados


    Example:
    --------
    import pandas as pd
    import etltools.etltools as etl

    covidTracking = etl.etljob('docker','docker','postgres','5432')
    covidTracking.etl()


    Example:
    --------
    python etltools/etltools.py



    '''


    def __init__(self, db_user, db_user_pass, db_name, db_port):
        self.db_user = db_user
        self.db_user_pass = db_user_pass
        self.db_name = db_name
        self.db_port = db_port
        self.db_table_name_original_data = "original_historic_values"
        self.db_table_name_transformed_data = "clean_historic_values_all_states"
        self.dataset_url = "https://api.covidtracking.com/v1/states/daily.csv"

        logging.basicConfig(
            level=logging.DEBUG, 
            filename="app.log", 
            filemode="w", 
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )


    def to_log(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            logging.debug(f">>> Running {fn.__name__!r} ")
            to_execute = fn(*args, **kwargs)
            logging.debug(f">>> Executed {fn.__name__!r} ")
            return to_execute
        return inner


    @to_log
    def extract(self):
        """
        Obtiene el dataset en formato csv y devuelve un dataframe
        Args:
        -----
            dataset_url ([str]): URL del archivo csv a descargar
        """
        print("extract...")
        print(self.dataset_url)
        df = pd.read_csv(self.dataset_url)
        return df


    @to_log
    def transform(self,df):
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
    def connect_db(self):
        """
        Creamos la conexión a la BBDD
        """
        connection_uri = "postgresql+psycopg2://{}:{}@{}:{}".format(
            self.db_user,
            self.db_user_pass,
            self.db_name,
            self.db_port
            )
        engine = create_engine(connection_uri, pool_pre_ping=True)
        engine.connect()
        return engine


    @to_log
    def load_data_to_db(self, df, table_name, engine):
        """
        Realiza la carga en BBDDD
        """
        df.to_sql(table_name, engine, if_exists="append")


    def etl(self):
        """
        Función principal
        """
        db_engine = self.connect_db()

        original_df = self.extract()

        clean_df = self.transform(original_df)
        print(clean_df.head(1).to_string())

        self.load_data_to_db(original_df, self.db_table_name_original_data, db_engine)
        self.load_data_to_db(clean_df, self.db_table_name_transformed_data, db_engine)

        db_engine.dispose()


if __name__ == "__main__":
    covidTracking = etljob('docker','docker','postgres','5432')
    covidTracking.etl()

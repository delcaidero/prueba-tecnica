
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
    df_transformed = df_transformed[df_transformed.totalTestResultsSource == 'totalTestsViral' ]
    df_transformed = df_transformed[['date','state','positive','deathConfirmed']]
    df_transformed['date']= pd.to_datetime(df_transformed['date'], format='%Y%m%d', errors='ignore')
    return df_transformed

if __name__  == "__main__":
    df_ = extract(DATASET_URL)
    print("carga realizada")
    print(df_.head(1).to_string())

    clean_df = transform(df_)
    print("transformacion realizada")
    print(clean_df.head(1).to_string())



"""
Este módulo es una prueba practica para 
realizar una etl de un dataset
"""

import os
import pandas as pd
from sqlalchemy import create_engine, inspect

DATASET_URL = "https://api.covidtracking.com/v1/states/daily.csv"

def extract(dataset_url):
    print(f"Reading dataset from {dataset_url}")
    df = pd.read_csv(dataset_url)
    return df

if __name__  == "__main__":
    df_ = extract(DATASET_URL)
    print("carga realizada")

import pandas as pd
import requests as rd
import json
import os
import numpy as np

current_dir_key = os.path.dirname(os.path.abspath(__file__))
data_file_config = os.path.join(current_dir_key, 'config.json')
with open(data_file_config, 'r') as config:
    config = json.load(config)


def transformar_data(**kwargs):
    xcom_value = kwargs['ti'].xcom_pull(task_ids='extraer_data')
    print(xcom_value)

    df_clima = pd.DataFrame(xcom_value["data_day"])
    print(df_clima)
    df_clima = df_clima[["time","temperature_max","temperature_min","precipitation"]]

    current_dir_sede = os.path.dirname(os.path.realpath(__file__)) 
    filename_sede = os.path.join(current_dir_sede, "sede.csv") 
    df_sede = pd.read_csv(filename_sede)

    df_clima['sede1'] = np.where(df_clima['precipitation'] > 17, df_sede[df_sede['clima'] == 'lluvia']['sede1'] , df_sede[df_sede['clima'] == 'sol']['sede1'])
    df_clima['sede2'] = np.where(df_clima['precipitation'] > 17, df_sede[df_sede['clima'] == 'lluvia']['sede2'] , df_sede[df_sede['clima'] == 'sol']['sede2'])

    print(df_clima)
    return df_clima.to_json(orient="columns")
from datetime import timedelta,datetime
from pathlib import Path
import json
import requests
import psycopg2
from airflow import DAG
from airflow.models.xcom import XCom

#from sqlalchemy import create_engine
# Operadores
from airflow.operators.python_operator import PythonOperator
#from airflow.utils.dates import days_ago
import pandas as pd
import os
#from src.conexion_redshit import conectar_redshift
from src.extraer_data import extraer_data
from src.cargar_data import cargar_data
from src.transformar_data import transformar_data
from src.envio_mail import enviar_mail
from psycopg2.extras import execute_values


# argumentos por defecto para el DAG
default_args = {
    'owner': 'EmanuelGomez',
    'start_date': datetime(2024,1,1),
    'email_on_failure': True,
    'retries':0,
    'retry_delay': timedelta(minutes=5)
}

BC_dag = DAG(
    dag_id='dag_clima',
    default_args=default_args,
    description='Se conecta a Api para tomar valor del clima y por un archivo csv marcar la sedes donde se va a jugar dependiendo si llueve o no ',
    schedule_interval="@daily",
    catchup=False
)


# Tareas
##1. Extraccion
task_1 = PythonOperator(
    task_id='extraer_data',
    python_callable=extraer_data,
    #op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

#2. Transformacion
task_2 = PythonOperator(
    task_id='transformar_data',
    python_callable=transformar_data,
   # op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

task_3 = PythonOperator(
    task_id='cargar_data',
    python_callable=cargar_data,
   # op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

task_4 = PythonOperator(
    task_id='enviar_mail',
    python_callable=enviar_mail,
   # op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)



# Definicion orden de tareas
task_1 >> task_2 >> task_3 >> task_4
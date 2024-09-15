import psycopg2
import json
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
data_file = os.path.join(current_dir, 'config.json')
with open(data_file, 'r') as archivo:
    config = json.load(archivo)

HOST = config['HOST']
PORT = config['PORT']
DATABASE= config['DATABASE']
USER= config['USER']
PASSWORD = config['PASSWORD']    

# Función para establecer la conexión
def get_schema()->str:
    return os.getenv("REDSHIFT_SCHEMA")
def conectar_redshift():
    try:
        conn = psycopg2.connect(
            host=HOST,
            port=PORT,
            database=DATABASE,
            user=USER,
            password=PASSWORD
        )
        print("Conexión exitosa a Redshift")
        return conn
    except (Exception, psycopg2.Error) as error:
        print("Error al conectar a Redshift", error)





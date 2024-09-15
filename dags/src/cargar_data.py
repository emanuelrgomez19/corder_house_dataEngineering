#from extraer_data import extraer_data 
from src.conexion_redshit import conectar_redshift , get_schema
#from conexion_redshit import conectar_redshift
from psycopg2.extras import execute_values
import pandas as pd
import json

def truncate_table():
    conn = conectar_redshift()
    create_table_query = """ TRUNCATE TABLE emanuelrgomez19_coderhouse.clima_sede """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()
    print("Truncate table emanuelrgomez19_coderhouse.clima_sede.")
    conn.close()

def create_table_if_not_exists():
    conn = conectar_redshift()
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS emanuelrgomez19_coderhouse.clima_sede (
        time date,
        temperature_max NUMERIC,
        temperature_min NUMERIC,
        precipitation NUMERIC,
        sede1 VARCHAR(200),
        sede2 VARCHAR(200)
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()
    print("Table emanuelrgomez19_coderhouse.clima_sede is ready.")
    conn.close()

def cargar_data(**kwargs):

    create_table_if_not_exists()
    
    truncate_table()

    table_name = 'emanuelrgomez19_coderhouse.clima_sede'
    conn = conectar_redshift()
    cur = conn.cursor()

    xcom_value = kwargs['ti'].xcom_pull(task_ids='transformar_data')

    xcom_value_json = json.loads(xcom_value)
    df = pd.DataFrame(xcom_value_json)


    values = [tuple(x) for x in df.to_numpy()]

    cols= ["time","temperature_max","temperature_min","precipitation","sede1","sede2"]

    insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
    execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")
    
    print('Proceso terminado')

    conn.close()

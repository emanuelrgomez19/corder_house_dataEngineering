import requests as rd
import json
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
data_file = os.path.join(current_dir, 'config.json')
with open(data_file, 'r') as archivo:
    config = json.load(archivo)

#API_KEY = config["APIKEY"]
API_KEY = config["APIKEY_TIEMPO"]

def extraer_data():
     try:
        url = f'https://my.meteoblue.com/packages/basic-1h_basic-day?lat=-34.613&lon=-58.377&apikey={API_KEY}'    
        response = rd.get(url)  
        if response.status_code == 200:
            data = response.json()
            print(type(data))
            return data 
        else: 
            print(f"Error {response.status_code}")
     except ValueError as e:
        print("Error: ", e)
        raise e
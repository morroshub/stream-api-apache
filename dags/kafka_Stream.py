
#Imports 
import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}


#Creamos una funcion que hace una peticion GET a una URL y a través de su api obtiene un json.
def get_data():
    import requests

# Realizamos la petición GET a la URL de la API de usuarios aleatorios
    res = requests.get("https://randomuser.me/api/")

# Convertimos la respuesta a formato JSON
    res = res.json()

# Extraemos el primer resultado de la respuesta (información de un usuario)
    res = res['results'][0]

    return res


# Creamos una funcion que formatea la data proviniente de la API y como argumento usamos nuestra variable 'res' 
#   creando un diccionario de todos los datos correctamente formateados 

def format_data(res):
    data = {}
    location = res['location']
    data['id'] = uuid.uuid4()
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

# Creamos una funcion del streaming y usamos kafka
def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    # Configuración del productor Kafka | # ATENCION Puertos internos
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000) # maxblock = time out

    # Captura del tiempo actual para limitar la ejecución a 1 minuto
    curr_time = time.time()

    # Bucle principal para transmitir datos
    while True:
        # Comprobar si ha pasado 1 minuto
        if time.time() > curr_time + 60: #1 minute = 60 segs
            break

    # Obtener datos de alguna fuente
        try:
            res = get_data()

            # Formatear los datos según sea necesario 
            res = format_data(res)


            # Enviar datos formateados al tópico 'users_created' en Kafka
            producer.send('users_created', json.dumps(res).encode('utf-8'))

    # Manejar errores y registrarlos
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue


# Configuración del DAG de Airflow
with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:


# Tarea PythonOperator para ejecutar la función stream_data
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
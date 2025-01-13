import os
import time
import json
import joblib  # type: ignore
import numpy as np  # type: ignore
import pandas as pd  # type: ignore
import paho.mqtt.client as mqtt  # type: ignore
from datetime import datetime
import random
from dotenv import load_dotenv  # type: ignore

# Carica le variabili dal file .env
load_dotenv()

SENSOR_ID = os.getenv("SENSOR_ID") 
DAM_UNIQUE_ID = os.getenv("DAM_UNIQUE_ID")
SENSORS_TOPIC_PREFIX = os.getenv("SENSORS_TOPIC_PREFIX")
SENSORS_PUBLISH_DELAY = int(os.getenv("SENSORS_PUBLISH_DELAY"))

MQTT_TOPIC = f"{DAM_UNIQUE_ID}/{SENSORS_TOPIC_PREFIX}/{SENSOR_ID}"

MODEL_FILE = os.getenv("MODEL_FILE")
if not MODEL_FILE:
    raise ValueError("La variabile MODEL_FILE non è definita nel file .env.")


MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

# Stato del client MQTT
is_connected = False

# Carica il modello
try:
    model = joblib.load(MODEL_FILE)
    print(f"Modello {MODEL_FILE} caricato con successo.")
except Exception as e:
    print(f"Errore nel caricamento del modello {MODEL_FILE}: {e}")
    exit(1)

def on_connect(client, userdata, flags, rc):
    """Callback per la connessione al broker."""
    global is_connected
    if rc == 0:
        print(f"Sensor {SENSOR_ID}: Connected to MQTT Broker!")
        is_connected = True
    else:
        print(f"Sensor {SENSOR_ID}: Failed to connect, return code {rc}")

def on_disconnect(client, userdata, rc):
    """Callback per la disconnessione dal broker."""
    global is_connected
    print(f"Sensor {SENSOR_ID}: Disconnected from MQTT Broker.")
    is_connected = False

def reconnect(client):
    """Gestisce i tentativi di riconnessione al broker."""
    global is_connected
    while not is_connected:
        try:
            print(f"Sensor {SENSOR_ID}: Attempting to reconnect to MQTT Broker...")
            client.connect(MQTT_BROKER, MQTT_PORT, 180)
            client.loop_start()
            time.sleep(5)
        except Exception as e:
            print(f"Sensor {SENSOR_ID}: Reconnection failed: {e}")
            time.sleep(5)

def predict_volume(day_of_year, hour_of_day):
    """Calcola la portata basata sul giorno dell'anno e l'ora."""
    sin_day = np.sin(2 * np.pi * day_of_year / 365)
    cos_day = np.cos(2 * np.pi * day_of_year / 365)
    sin_hour = np.sin(2 * np.pi * hour_of_day / 24)
    cos_hour = np.cos(2 * np.pi * hour_of_day / 24)

    # Prepara i dati per il modello
    X_input = pd.DataFrame([{
        'sin_day': sin_day,
        'cos_day': cos_day,
        'sin_hour': sin_hour,
        'cos_hour': cos_hour
    }])

    # Predizione
    return model.predict(X_input)[0]

def apply_random_variability(value, min_percentage=5, max_percentage=10):
    """Applica una variabilità casuale compresa tra il min_percentage e il max_percentage al valore."""
    percentage_variation = random.uniform(min_percentage, max_percentage) / 100
    variation = value * percentage_variation
    # Decidi se aggiungere o sottrarre la variazione
    if random.choice([True, False]):
        return value + variation
    else:
        return value - variation

def publish_data(client, sensor_id):
    """Pubblica i dati del sensore su MQTT."""
    global is_connected
    while True:
        if not is_connected:
            print(f"Sensor {SENSOR_ID}: Not connected. Retrying...")
            reconnect(client)

        try:
            # Ottieni la data e l'ora corrente
            now = datetime.now()
            day_of_year = now.timetuple().tm_yday
            hour_of_day = now.hour

            # Genera la portata utilizzando il modello e aggiungi variabilità
            inflow = predict_volume(day_of_year, hour_of_day)
            inflow = apply_random_variability(inflow)

            # Costruisci i dati da pubblicare
            data = {
                "sensor_id": sensor_id,
                "inflow": float(inflow),
                "timestamp": now.strftime("%Y-%m-%d %H:%M:%S")
            }
            client.publish(MQTT_TOPIC, json.dumps(data))  # Converte il dizionario in una stringa JSON

            #print(f"Sensor {sensor_id} published: {data}")
        except Exception as e:
            print(f"Sensor {SENSOR_ID}: Error while publishing: {e}")
            is_connected = False
        time.sleep(SENSORS_PUBLISH_DELAY)

if __name__ == "__main__":
    # Configura il client MQTT
    client = mqtt.Client(f"Sensor_{SENSOR_ID}")
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

   
    try:
        reconnect(client)
        publish_data(client, SENSOR_ID)
    except KeyboardInterrupt:
        print(f"Sensor {SENSOR_ID}: Shutting down.")
    except Exception as e:
        print(f"Sensor {SENSOR_ID}: Critical error: {e}")
    finally:
        client.loop_stop()
        client.disconnect()

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



MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

# Stato del client MQTT
is_connected = False


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

def calculate_inflow(hour, minute):
    total_minutes = hour * 60 + minute  # Convertiamo ore e minuti in minuti totali
    # Test di esempio

    if 0 <= total_minutes < 360:  # Da 00:00 a 05:59 (360 minuti)
        return 0  # Produzione 0 tra le 00:00 e le 06:00
    elif 360 <= total_minutes <= 720:  # Da 06:00 a 11:59 (720 minuti)
        percentage = (total_minutes - 360) / 360  # Calcolo della percentuale su 360 minuti
        return percentage * 20  # Crescita progressiva da 0 a 20
    elif 720 < total_minutes <= 1080:  # Da 12:00 a 17:59 (1080 minuti)
        percentage = (1080 - total_minutes) / 360  # Calcolo della percentuale decrescente
        return percentage * 20  # Decrescita progressiva da 20 a 0
    elif 1080 < total_minutes <= 1440:  # Da 18:00 a 23:59 (1440 minuti)
        return 0  # Produzione 0 tra le 18:00 e le 00:00
    else:
        raise ValueError("Hour must be between 0 and 24, minute between 0 and 59")
    

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
            hour_of_day = now.hour
            minute_of_day = now.minute

            # Genera la portata utilizzando il modello e aggiungi variabilità
            inflow = calculate_inflow(hour_of_day, minute_of_day)
            inflow = apply_random_variability(inflow)
            print(f"Sensor {sensor_id} published: {inflow}")
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

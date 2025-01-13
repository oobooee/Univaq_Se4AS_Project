import os
import json
import time
import threading
from influxdb_client import InfluxDBClient, QueryApi  # type: ignore
from dotenv import load_dotenv  # type: ignore
import paho.mqtt.client as mqtt  # type: ignore

# Carica le variabili dal file .env
load_dotenv()

# Variabili dal .env
DAM_UNIQUE_ID = os.getenv("DAM_UNIQUE_ID")
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("DOCKER_INFLUXDB_INIT_ORG")
INFLUXDB_BUCKET = os.getenv("DOCKER_INFLUXDB_INIT_BUCKET")

VOLUME_SENSOR_DATA = os.getenv("VOLUME_SENSOR_DATA")
VOLUME_FIELD = os.getenv("VOLUME_FIELD")
HEIGHT_FIELD = os.getenv("HEIGHT_FIELD")
BUCKET_FLOWS_DATA = os.getenv("BUCKET_FLOWS_DATA")
POWER_GATE_ID = os.getenv("POWER_GATE_ID")
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
DAM_HEIGHT = float(os.getenv("DAM_HEIGHT"))
DAM_CRITICAL_HEIGHT = float(os.getenv("DAM_CRITICAL_HEIGHT"))
POWER_GATE_OUTFLOW = float(os.getenv("POWER_GATE_OUTFLOW"))
QUERY_INTERVAL = int(os.getenv("QUERY_INTERVAL"))
PLANNER_TOPIC_PREFIX = os.getenv("PLANNER_TOPIC_PREFIX")
GATE_TOPIC_PREFIX = os.getenv("GATE_TOPIC_PREFIX")
SPILLWAY_GATE_COUNT = int(os.getenv("SPILLWAY_GATE_COUNT"))

# Connessione a InfluxDB
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
query_api = client.query_api()

# MQTT Client
mqtt_client = mqtt.Client("Planner")
mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)

# Riconnessione MQTT
def reconnect_mqtt(client):
    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            client.loop_start()
            break
        except Exception as e:
            print(f"PLANNER: MQTT Reconnection failed: {e}")
            time.sleep(5)

# Recupera il volume attuale
def get_current_volume():
    query = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
        |> range(start: -30m)
        |> filter(fn: (r) => r._measurement == "{VOLUME_SENSOR_DATA}")
        |> filter(fn: (r) => r._field == "{VOLUME_FIELD}")
        |> last()
    '''
    try:
        result = query_api.query(org=INFLUXDB_ORG, query=query)
        for table in result:
            for record in table.records:
                return float(record.get_value())
    except Exception as e:
        print(f"PLANNER: Error retrieving current volume: {e}")
    return None

# Recupera l'altezza attuale
def get_current_height():
    query = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
        |> range(start: -30m)
        |> filter(fn: (r) => r._measurement == "{VOLUME_SENSOR_DATA}")
        |> filter(fn: (r) => r._field == "{HEIGHT_FIELD}")
        |> last()
    '''
    try:
        result = query_api.query(org=INFLUXDB_ORG, query=query)
        for table in result:
            for record in table.records:
                return float(record.get_value())
    except Exception as e:
        print(f"PLANNER: Error retrieving current height: {e}")
    return None


def handle_lake_below_60(volume, height):
    """Gestisce la logica quando il livello del lago è sotto il 60%."""
    actions = {}
    actions[POWER_GATE_ID] = 0  # Chiudi la Power Gate
    for i in range(1, SPILLWAY_GATE_COUNT + 1):
        actions[f"Spillway_Gate_{i}"] = 0  # Chiudi tutte le Spillway Gates
    print(f"PLANNER: Lake below 60%. Height: {height:.2f} m. Power Gate: 0%. Spillway Gates: 0%.")
    return actions


def handle_lake_between_60_and_critical(volume, height, critical_height, inflow):
    """Gestisce la logica quando il lago è tra il 60% e la soglia critica."""
    actions = {}
    # Calcolo percentuale di apertura della Power Gate basato su inflow e altezza
    power_gate_percentage = min(100, max(10, inflow * 10))  # Apertura minima del 10%
    
    # Se il livello è più vicino alla soglia critica, aumenta l'apertura
    if height > 0.85 * DAM_HEIGHT:
        power_gate_percentage = min(100, max(10, power_gate_percentage + (height - 0.85 * DAM_HEIGHT) * 50))
    
    actions[POWER_GATE_ID] = power_gate_percentage
    for i in range(1, SPILLWAY_GATE_COUNT + 1):
        actions[f"Spillway_Gate_{i}"] = 0  # Mantieni chiuse le Spillway Gates
    
    print(f"PLANNER: Lake between 60% and critical. Height: {height:.2f} m. "
          f"Inflow: {inflow:.2f} m³/s. Power Gate: {power_gate_percentage:.2f}%. Spillway Gates: 0%.")
    return actions



def handle_lake_above_critical(volume, height, critical_height):
    """Gestisce la logica quando il lago è sopra la soglia critica."""
    actions = {}
    actions[POWER_GATE_ID] = 100  # Apri completamente la Power Gate
    spillway_percentage = min(100, (height - critical_height) * (100 / (DAM_HEIGHT - critical_height)))
    for i in range(1, SPILLWAY_GATE_COUNT + 1):
        actions[f"Spillway_Gate_{i}"] = spillway_percentage
    print(f"PLANNER: Lake above critical. Height: {height:.2f} m. "
          f"Power Gate: 100%. Spillway Gates: {spillway_percentage:.2f}%.")
    return actions


def generate_action_plan(volume, height):
    """Genera un piano d'azione delegando a funzioni specifiche."""
    critical_height = DAM_CRITICAL_HEIGHT * DAM_HEIGHT
    lake_percentage = height / DAM_HEIGHT  # Percentuale del livello del lago

    if height < 0.60 * DAM_HEIGHT:
        return handle_lake_below_60(volume, height)
    elif 0.60 * DAM_HEIGHT <= height < critical_height:
        return handle_lake_between_60_and_critical(volume, height, critical_height)
    elif height >= critical_height:
        return handle_lake_above_critical(volume, height, critical_height)




# Esegue il ciclo del Planner
def planner_loop():
    while True:
        try:
            volume = get_current_volume()
            height = get_current_height()

            if None in (volume, height):
                print("PLANNER: Missing data for plan generation.")
                time.sleep(QUERY_INTERVAL)
                continue

            plan = generate_action_plan(volume, height)

            # Pubblica il piano d'azione sul topic centrale
            topic = f"{DAM_UNIQUE_ID}/{PLANNER_TOPIC_PREFIX}"
            mqtt_client.publish(topic, json.dumps(plan), qos=2)
            print(f"PLANNER: Published action plan: {plan} on topic {topic}")

        except Exception as e:
            print(f"PLANNER: Error in planner loop: {e}")

        time.sleep(QUERY_INTERVAL)
if __name__ == "__main__":
    try:
        print("PLANNER: Initializing...")
        reconnect_mqtt(mqtt_client)
        threading.Thread(target=planner_loop, daemon=True).start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("PLANNER: Stopping...")
    except Exception as e:
        print(f"PLANNER: Critical Error: {e}")
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()

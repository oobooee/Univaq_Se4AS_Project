import os
import json
import time
import threading
import paho.mqtt.client as mqtt  # type: ignore
from dotenv import load_dotenv  # type: ignore
from influxdb_client import InfluxDBClient, Point, WriteOptions  # type: ignore

# Carica le variabili dal file .env
load_dotenv()

DAM_UNIQUE_ID = os.getenv("DAM_UNIQUE_ID")
# Topic dinamici
SENSORS_TOPIC_PREFIX = os.getenv("SENSORS_TOPIC_PREFIX")
GATE_TOPIC_PREFIX = os.getenv("GATE_TOPIC_PREFIX")
# Parametri MQTT

MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("DOCKER_INFLUXDB_INIT_ORG")
INFLUXDB_BUCKET = os.getenv("DOCKER_INFLUXDB_INIT_BUCKET")

BUCKET_SENSOR_DATA=os.getenv("BUCKET_SENSOR_DATA")
BUCKET_GATE_DATA=os.getenv("BUCKET_GATE_DATA")
BUCKET_FLOWS_DATA=os.getenv("BUCKET_FLOWS_DATA")
SENSOR_TAG =os.getenv("SENSOR_TAG")
SENSOR_FIELD =os.getenv("SENSOR_FIELD")
GATE_TAG =os.getenv("GATE_TAG")
GATE_FIELD_STATE =os.getenv("GATE_FIELD_STATE")
GATE_FIELD_FLOW =os.getenv("GATE_FIELD_FLOW")
GATE_OUTFLOW = float(os.getenv("GATE_OUTFLOW")) 
TIMESTAMP =os.getenv("TIMESTAMP")

POWER_GATE_ID = os.getenv("POWER_GATE_ID")
POWER_GATE_OUTFLOW = float(os.getenv("POWER_GATE_OUTFLOW"))

# Configurazione client InfluxDB
client = InfluxDBClient(
    url=INFLUXDB_URL,
    token=INFLUXDB_TOKEN,
    org=INFLUXDB_ORG
)
write_api = client.write_api(
    write_options=WriteOptions(
        batch_size=10,
        flush_interval=5000,
        retry_interval=1000,
        max_retries=3,
        max_retry_delay=5000,
        exponential_base=2
    )
)

# Stato del client MQTT
is_connected = False
sensor_data = {}
gate_states = {}
data_lock = threading.Lock()


def validate_percentage(value, min_value=0, max_value=100):
    """Valida che il valore rientri nei limiti percentuali."""
    return max(min(value, max_value), min_value)

def on_connect(client, userdata, flags, rc):
    """Callback per la connessione al broker."""
    global is_connected
    if rc == 0:
        print("MONITOR: Connected to MQTT Broker!")
        is_connected = True

        # Sottoscrivi ai topic dinamici
        sensor_topic = f"{DAM_UNIQUE_ID}/{SENSORS_TOPIC_PREFIX}/#"
        gate_topic = f"{DAM_UNIQUE_ID}/{GATE_TOPIC_PREFIX}/#"
        client.subscribe(sensor_topic, qos=2)
        client.subscribe(gate_topic, qos=2)
        print(f"MONITOR: Subscribed to {sensor_topic} and {gate_topic}")
    else:
        print(f"MONITOR: Failed to connect, return code {rc}")

def on_disconnect(client, userdata, rc):
    """Callback per la disconnessione dal broker."""
    global is_connected
    print("MONITOR: Disconnected from MQTT Broker.")
    is_connected = False

def on_message(client, userdata, msg):
    global sensor_data, gate_states
    topic = msg.topic
    try:
        payload = json.loads(msg.payload)
    except json.JSONDecodeError:
        print(f"MONITOR: Invalid JSON payload on topic {topic}")
        return

    with data_lock:
        if topic.startswith(f"{DAM_UNIQUE_ID}/{SENSORS_TOPIC_PREFIX}"):
            sensor_id = topic.split("/")[-1]
            sensor_data[sensor_id] = payload.get(SENSOR_FIELD, 0)
            try:
                point = Point(f"{BUCKET_SENSOR_DATA}") \
                    .tag(f"{SENSOR_TAG}", sensor_id) \
                    .field(f"{SENSOR_FIELD}", payload.get(f"{SENSOR_FIELD}", 0)) \
                    .time(payload.get(f"{TIMESTAMP}", time.strftime("%Y-%m-%dT%H:%M:%SZ")))
                write_api.write(bucket=INFLUXDB_BUCKET, record=point)
            except Exception as e:
                print(f"MONITOR: Error writing sensor data to InfluxDB for {sensor_id}: {e}")

        elif topic.startswith(f"{DAM_UNIQUE_ID}/{GATE_TOPIC_PREFIX}"):
            gate_id = topic.split("/")[-2]
            open_percentage = payload.get("open_percentage", 0)
            #open_percentage_v = validate_percentage(open_percentage, 0, 100)
            # Aggiorna lo stato del gate (senza calcolare l'outflow)
            gate_states[gate_id] = {
                "open_percentage": open_percentage
            }
            try:
                # Scrive solo la percentuale di apertura su InfluxDB
                point = Point(f"{BUCKET_GATE_DATA}") \
                    .tag(f"{GATE_TAG}", gate_id) \
                    .field(f"{GATE_FIELD_STATE}", float(open_percentage)) \
                    .time(payload.get(f"{TIMESTAMP}", time.strftime("%Y-%m-%dT%H:%M:%SZ")))
                write_api.write(bucket=INFLUXDB_BUCKET, record=point)
            except Exception as e:
                print(f"MONITOR: Error writing gate state to InfluxDB for {gate_id}: {e}")

        

def calculate_and_write_global_flow():
    """Calcola e scrive il flusso globale e i flussi specifici di ogni gate su InfluxDB."""
    while True:
        try:
            points = []  # Per raccogliere tutti i dati da scrivere in un batch
            with data_lock:
                total_inflow = float(sum(sensor_data.values()))  # Somma gli inflow
                print(f"sensor_data: {sensor_data}")
                total_outflow = 0.0  # Inizializza il totale dell'outflow

                # Calcola i flussi specifici di ogni gate e li aggiunge al batch
                for gate_id, gate_data in gate_states.items():
                    if str(gate_id) == str(POWER_GATE_ID):
                        gate_outflow = (gate_data.get("open_percentage", 0) / 100) * POWER_GATE_OUTFLOW
                    else:
                        gate_outflow = (gate_data.get("open_percentage", 0) / 100) * GATE_OUTFLOW

                    # Aggiungi al batch il flusso specifico del gate
                    points.append(
                        Point(f"{BUCKET_SENSOR_DATA}")
                        .tag(f"{GATE_TAG}", gate_id)
                        .field(f"{GATE_FIELD_FLOW}", gate_outflow)
                        .time(time.strftime("%Y-%m-%dT%H:%M:%SZ"))
                    )

                    # Somma l'outflow corrente al totale
                    total_outflow += gate_outflow

                # Aggiungi al batch i flussi globali
                points.append(
                    Point(f"{BUCKET_FLOWS_DATA}")
                    .field("total_inflow", total_inflow)
                    .field("total_outflow", total_outflow)
                    .time(time.strftime("%Y-%m-%dT%H:%M:%SZ"))
                )

            # Scrivi tutti i punti in un unico batch
            write_api.write(bucket=INFLUXDB_BUCKET, record=points)
            #print("MONITOR: Written global flow and individual gate flows to InfluxDB")

        except Exception as e:
            print(f"MONITOR: Error writing flows to InfluxDB: {e}")

        time.sleep(1)


def reconnect(client):
    """Gestisce i tentativi di riconnessione al broker."""
    global is_connected
    while not is_connected:
        try:
            print("MONITOR: Attempting to reconnect to MQTT Broker...")
            client.connect(MQTT_BROKER, MQTT_PORT, 3600)
            client.loop_start()
            time.sleep(2)
        except Exception as e:
            print(f"MONITOR: Reconnection failed: {e}")
            time.sleep(5)

if __name__ == "__main__":
    client = mqtt.Client("Monitor")
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    try:
        threading.Thread(target=calculate_and_write_global_flow, daemon=True).start()

        reconnect(client)
        print("MONITOR: Processing messages...")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("MONITOR: Shutting down gracefully.")
    except Exception as e:
        print(f"MONITOR: Critical error: {e}")
    finally:
        client.loop_stop()
        client.disconnect()
        if write_api:
            write_api.__del__()
        client.close()
        print("MONITOR: Resources released.")

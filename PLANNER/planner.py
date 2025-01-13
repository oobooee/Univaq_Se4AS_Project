import os
import time
import json

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

MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
PLANNER_TOPIC_PREFIX = os.getenv("PLANNER_TOPIC_PREFIX")
GATE_TOPIC_PREFIX = os.getenv("GATE_TOPIC_PREFIX")
POWER_GATE_ID = os.getenv("POWER_GATE_ID")
SPILLWAY_GATE_COUNT = int(os.getenv("SPILLWAY_GATE_COUNT"))
DAM_HEIGHT = float(os.getenv("DAM_HEIGHT"))
DAM_CRITICAL_HEIGHT = float(os.getenv("DAM_CRITICAL_HEIGHT"))
DAM_MIN_HEIGHT  = float(os.getenv("DAM_MIN_HEIGHT"))
QUERY_INTERVAL = int(os.getenv("QUERY_INTERVAL"))
BUCKET_FLOWS_DATA = os.getenv("BUCKET_FLOWS_DATA")
VOLUME_SENSOR_DATA = os.getenv("VOLUME_SENSOR_DATA")
VOLUME_FIELD = os.getenv("VOLUME_FIELD")
HEIGHT_FIELD = os.getenv("HEIGHT_FIELD")
POWER_GATE_OUTFLOW = float(os.getenv("POWER_GATE_OUTFLOW"))
# Connessione a InfluxDB
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
query_api = client.query_api()

# Stato MQTT
mqtt_client = mqtt.Client("FSM")
mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)




# Classe per la macchina a stati finiti
class DamFSM:
    def __init__(self):
        self.state = "IDLE"  # Stato iniziale
        self.volume = 0
        self.height = 0
        self.inflow = 0
        self.outflow = 0
        self.actions = {}
        self.critical_height = DAM_CRITICAL_HEIGHT * DAM_HEIGHT  # Altezza critica assoluta
        print(f"Initial Critical Height: {self.critical_height}")

    def set_state(self, new_state):
        print(f"FSM: Transitioning from {self.state} to {new_state}")
        self.state = new_state

    def fetch_data(self):
        """Recupera i dati da InfluxDB."""
        try:
            self.inflow = self.get_last_value(BUCKET_FLOWS_DATA, "total_inflow")
            self.outflow = self.get_last_value(BUCKET_FLOWS_DATA, "total_outflow")
            self.volume = self.get_last_value(VOLUME_SENSOR_DATA, VOLUME_FIELD)
            self.height = self.get_last_value(VOLUME_SENSOR_DATA, HEIGHT_FIELD)
            print(f"FSM: Data - Inflow: {self.inflow}, Outflow: {self.outflow}, Volume: {self.volume}, Height: {self.height}")
            print(f"Critical Height: {self.critical_height}, Min Height: {DAM_MIN_HEIGHT * DAM_HEIGHT}")
        except Exception as e:
            print(f"FSM: Error fetching data: {e}")

    def get_last_value(self, measurement, field):
        """
        Recupera l'ultimo valore di un campo specifico da una misura InfluxDB.
        """
        query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
            |> range(start: -5m)
            |> filter(fn: (r) => r._measurement == "{measurement}")
            |> filter(fn: (r) => r._field == "{field}")
            |> last()
        '''
        try:
            result = query_api.query(org=INFLUXDB_ORG, query=query)
            for table in result:
                for record in table.records:
                    return float(record.get_value())
        except Exception as e:
            print(f"Error retrieving {field} from {measurement}: {e}")
        return 0.0

    def execute_state(self):
        """Esegue la logica associata allo stato corrente."""
        print(f"FSM: Current State: {self.state}")
        if self.state == "IDLE":
            self.idle()
        elif self.state == "FILL":
            self.fill()
        elif self.state == "BALANCE":
            self.balance()
        elif self.state == "EMERGENCY":
            self.emergency()

    def idle(self):
        """Stato IDLE: Nessuna azione, monitoraggio passivo."""
        print("FSM: IDLE - Monitoring conditions...")
        if self.height < DAM_MIN_HEIGHT * DAM_HEIGHT:
            self.set_state("FILL")
        elif self.height >= self.critical_height:
            self.set_state("EMERGENCY")

    def fill(self):
        """Stato FILL: Riempimento del lago."""
        print("FSM: FILL - Increasing lake level...")
        self.actions[POWER_GATE_ID] = 0
        for i in range(1, SPILLWAY_GATE_COUNT + 1):
            self.actions[f"Spillway_Gate_{i}"] = 0
        if self.height >= DAM_MIN_HEIGHT * DAM_HEIGHT:
            print("FSM: Height reached 60% threshold, transitioning to BALANCE.")
            self.set_state("BALANCE")
        elif self.height >= self.critical_height:
            print("FSM: Height exceeded critical level, transitioning to EMERGENCY.")
            self.set_state("EMERGENCY")

    def balance(self):
        """Stato BALANCE: Mantiene il livello del lago tra il 60% e il 98%."""
        print("FSM: BALANCE - Maintaining lake level...")

        # Calcola la percentuale di apertura della Power Gate in base all'inflow
        power_gate_percentage = min(100, (self.inflow / POWER_GATE_OUTFLOW) * 100)

        # Mantieni chiuse le Spillway Gates in questo stato
        for i in range(1, SPILLWAY_GATE_COUNT + 1):
            self.actions[f"Spillway_Gate_{i}"] = 0

        # Aggiungi un controllo di coerenza tra inflow e Power Gate outflow
        calculated_outflow = (power_gate_percentage / 100) * POWER_GATE_OUTFLOW
        if calculated_outflow < self.inflow:
            print("FSM: Warning - Power Gate outflow is less than inflow.")
        else:
            print(f"FSM: Power Gate outflow matches inflow: {calculated_outflow:.2f} m³/s")

        # Imposta l'apertura della Power Gate
        self.actions[POWER_GATE_ID] = power_gate_percentage

        # Transizione in caso di superamento della soglia critica
        if self.height >= DAM_CRITICAL_HEIGHT * DAM_HEIGHT:
            self.set_state("EMERGENCY")

    def emergency(self):
        """Stato EMERGENCY: Livello critico, abbassare rapidamente."""
        print("FSM: EMERGENCY - Reducing lake level!")
        self.actions[POWER_GATE_ID] = 100
        for i in range(1, SPILLWAY_GATE_COUNT + 1):
            self.actions[f"Spillway_Gate_{i}"] = 100
        if self.height < self.critical_height:
            print("FSM: Height dropped below critical level, transitioning to BALANCE.")
            self.set_state("BALANCE")

    def publish_actions(self):
        """Pubblica le azioni correnti come comando MQTT."""
        try:
            topic = f"{DAM_UNIQUE_ID}/{PLANNER_TOPIC_PREFIX}"
            mqtt_client.publish(topic, json.dumps(self.actions), qos=2)
            print(f"FSM: Published actions: {self.actions}")
        except Exception as e:
            print(f"FSM: Error publishing actions: {e}")

    # Riconnessione MQTT
def reconnect_mqtt():
    while True:
        try:
            mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
            mqtt_client.loop_start()
            break
        except Exception as e:
            print(f"FSM: MQTT Reconnection failed: {e}")
            time.sleep(5)


# Main
if __name__ == "__main__":
    fsm = DamFSM()
    reconnect_mqtt()
    try:
        while True:
            fsm.fetch_data()
            fsm.execute_state()
            fsm.publish_actions()
            time.sleep(QUERY_INTERVAL)
    except KeyboardInterrupt:
        print("FSM: Stopping...")
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
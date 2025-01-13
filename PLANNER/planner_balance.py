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
BUCKET_PREDICTED_DATA = os.getenv("BUCKET_PREDICTED_DATA")
# Connessione a InfluxDB
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
query_api = client.query_api()

# Stato MQTT
mqtt_client = mqtt.Client("FSM")
mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)

class BalanceFSM:
    import time
    def __init__(self):
        self.state = "INITIAL"

    def execute(self, parent_fsm):
        if self.state == "INITIAL":
            self.initial(parent_fsm)
        elif self.state == "MID":
            self.mid(parent_fsm)
        elif self.state == "FINAL":
            self.final(parent_fsm)

    def initial(self, parent_fsm):
        print("FSM BALANCE: State INITIAL - Minimum Power Gate.")
        
        # Azzera gli Spillway Gates
        for i in range(1, parent_fsm.SPILLWAY_GATE_COUNT + 1):
            parent_fsm.actions[f"Spillway_Gate_{i}"] = 0

        inflow = parent_fsm.inflow
        target_outflow = min(inflow * 0.5, parent_fsm.POWER_GATE_OUTFLOW)
        target_percentage = min((target_outflow / parent_fsm.POWER_GATE_OUTFLOW) * 100, 100)
        print("INITIALLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL")
        new_percentage = self.interpolate(parent_fsm, target_percentage)
        parent_fsm.actions[parent_fsm.POWER_GATE_ID] = new_percentage

        print(f"INITIAL: Target: {target_percentage:.2f}%, New: {new_percentage:.2f}%")

        threshold = (
            0.80 * (parent_fsm.DAM_CRITICAL_HEIGHT - parent_fsm.DAM_MIN_HEIGHT) * parent_fsm.DAM_HEIGHT
            + (parent_fsm.DAM_MIN_HEIGHT * parent_fsm.DAM_HEIGHT)
        )
        # Passa a MID se l'altezza supera l'80%
        if parent_fsm.height >= threshold:
            self.state = "MID"

    def mid(self, parent_fsm):
        print("FSM BALANCE: State MID - Intermediate Power Gate.")

        # Azzera gli Spillway Gates
        for i in range(1, parent_fsm.SPILLWAY_GATE_COUNT + 1):
            parent_fsm.actions[f"Spillway_Gate_{i}"] = 0

        inflow = parent_fsm.inflow
        target_outflow = min(inflow * 0.7, parent_fsm.POWER_GATE_OUTFLOW)
        target_percentage = min((target_outflow / parent_fsm.POWER_GATE_OUTFLOW) * 100, 100)
        print("MIDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD")
        new_percentage = self.interpolate(parent_fsm, target_percentage)
        parent_fsm.actions[parent_fsm.POWER_GATE_ID] = new_percentage

        print(f"MID: Target: {target_percentage:.2f}%, New: {new_percentage:.2f}%")
        threshold = (
            0.95 * (parent_fsm.DAM_CRITICAL_HEIGHT - parent_fsm.DAM_MIN_HEIGHT) * parent_fsm.DAM_HEIGHT
            + (parent_fsm.DAM_MIN_HEIGHT * parent_fsm.DAM_HEIGHT)
        )
        print(f"Threshold: {threshold:.2f}, Current Height: {parent_fsm.height:.2f}")
        # Passa a FINAL se l'altezza supera il threshold
        if parent_fsm.height >= threshold:
            self.state = "FINAL"

    def final(self, parent_fsm):
        print("FSM BALANCE: State FINAL - Using Predictions.")

        # Azzera gli Spillway Gates
        for i in range(1, parent_fsm.SPILLWAY_GATE_COUNT + 1):
            parent_fsm.actions[f"Spillway_Gate_{i}"] = 0

        daily_term = self.get_daily_predictions()
        weekly_term = self.get_weekly_predictions()
        monthly_term = self.get_monthly_predictions()
        quarterly_term = self.get_quarterly_predictions()
        semiannual_term = self.get_semiannual_predictions()

        daily_inflow = sum(value for _, value in daily_term) / len(daily_term) if daily_term else 0
        weekly_inflow = sum(value for _, value in weekly_term) / len(weekly_term) if weekly_term else 0
        monthly_inflow = sum(value for _, value in monthly_term) / len(monthly_term) if monthly_term else 0
        quarterly_inflow = sum(value for _, value in quarterly_term) / len(quarterly_term) if quarterly_term else 0
        semiannual_inflow = sum(value for _, value in semiannual_term) / len(semiannual_term) if semiannual_term else 0

      

        inflow = parent_fsm.inflow
        predicted_inflow = (
            0.5 * inflow +
            0.2 * daily_inflow +
            0.1 * weekly_inflow +
            0.1 * monthly_inflow +
            0.05 * quarterly_inflow +
            0.05 * semiannual_inflow  
        )
  
       
        target_outflow = min(predicted_inflow, parent_fsm.POWER_GATE_OUTFLOW)
        target_percentage = min((target_outflow / parent_fsm.POWER_GATE_OUTFLOW) * 100, 100)
        print("FINALLLLLLLLLLLLLLLLLLLLL")
        new_percentage = self.interpolate(parent_fsm, target_percentage)
        parent_fsm.actions[parent_fsm.POWER_GATE_ID] = new_percentage

        print(f"FINAL: Target: {target_percentage:.2f}%, New: {new_percentage:.2f}%")
        

        threshold = (
            0.95 * (parent_fsm.DAM_CRITICAL_HEIGHT - parent_fsm.DAM_MIN_HEIGHT) * parent_fsm.DAM_HEIGHT
            + (parent_fsm.DAM_MIN_HEIGHT * parent_fsm.DAM_HEIGHT)
        )
        print(f"Threshold: {threshold:.2f}, Current Height: {parent_fsm.height:.2f}")
        # Passa a MID se l'altezza supera l'80%
        if parent_fsm.height <= threshold:
            self.state = "MID"

        if parent_fsm.height >= parent_fsm.DAM_CRITICAL_HEIGHT * parent_fsm.DAM_HEIGHT:
            parent_fsm.set_state("EMERGENCY")


    def interpolate(self, parent_fsm, target_percentage):
        current_percentage = parent_fsm.actions.get(parent_fsm.POWER_GATE_ID, 0)
        new_percentage = current_percentage + (target_percentage - current_percentage) * 0.1
        # Limita il valore al massimo del 100%
        return min(max(new_percentage, 0), 100)

    

    def get_daily_predictions(self):
        """Recupera le previsioni delle prossime 24 ore aggregate orariamente."""
        return self.query_predictions_simple(duration_seconds=24 * 3600, aggregation="mean")


    def get_weekly_predictions(self):
        """Recupera le previsioni della prossima settimana aggregate giornalmente."""
        return self.query_predictions_simple(duration_seconds=7 * 24 * 3600, aggregation="mean")

    def get_monthly_predictions(self):
        """Recupera le previsioni dei prossimi 30 giorni aggregate settimanalmente."""
        return self.query_predictions_simple(duration_seconds=30 * 24 * 3600, aggregation="mean")

    def get_quarterly_predictions(self):
        """Recupera le previsioni dei prossimi 90 giorni aggregate settimanalmente."""
        return self.query_predictions_simple(duration_seconds=90 * 24 * 3600, aggregation="mean")
    
    def get_semiannual_predictions(self):
        """Recupera le previsioni dei prossimi 6 mesi aggregate mensilmente."""
        return self.query_predictions_simple(duration_seconds=180 * 24 * 3600, aggregation="mean")


    def query_predictions_simple(self, duration_seconds, aggregation="mean"):
        """
        Funzione per recuperare previsioni aggregate usando timestamp Unix.
        :param duration_seconds: Durata in secondi (es. 86400 per 1 giorno, 604800 per 7 giorni).
        :param aggregation: Funzione di aggregazione ('mean', 'sum').
        :return: Lista di tuple (timestamp, valore).
        """
        # Ottieni i timestamp attuali
        current_time = int(self.time.time())  # Timestamp attuale
        start_time = current_time  # Partenza dall'ora corrente
        end_time = current_time + duration_seconds  # Aggiungi la durata in secondi

        query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
            |> range(start: {start_time}, stop: {end_time})
            |> filter(fn: (r) => r._measurement == "{BUCKET_PREDICTED_DATA}")
            |> filter(fn: (r) => r._field == "total_inflow")
            |> aggregateWindow(every: {duration_seconds}s, fn: {aggregation}, createEmpty: false)
            |> yield(name: "{aggregation}")
        '''
        try:
            result = query_api.query(org=INFLUXDB_ORG, query=query)
            predictions = [(record.get_time(), record.get_value()) for table in result for record in table.records]

            print(f"\nPredictions ({aggregation}, {duration_seconds}s):")
            for time, value in predictions:
                print(f"{time}: {value:.2f} m³/s")
            
            return predictions
        except Exception as e:
            print(f"Error retrieving predictions: {e}")
            return []










# Classe per la macchina a stati finiti
class DamFSM:
    def __init__(self):
        self.state = "IDLE"  # Stato iniziale
        self.volume = 0
        self.height = 0
        self.inflow = 0
        self.outflow = 0
        self.actions = {}
        self.POWER_GATE_ID = POWER_GATE_ID  # Inizializza POWER_GATE_ID dal file .env
        self.SPILLWAY_GATE_COUNT = SPILLWAY_GATE_COUNT  # Inizializza SPILLWAY_GATE_COUNT dal file .env
        self.DAM_HEIGHT = DAM_HEIGHT  # Inizializza DAM_HEIGHT dal file .env
        self.DAM_CRITICAL_HEIGHT = DAM_CRITICAL_HEIGHT  # Inizializza DAM_CRITICAL_HEIGHT dal file .env
        self.DAM_MIN_HEIGHT = DAM_MIN_HEIGHT  # Inizializza DAM_MIN_HEIGHT dal file .env
        self.POWER_GATE_OUTFLOW = POWER_GATE_OUTFLOW  # Inizializza POWER_GATE_OUTFLOW dal file .env

        # Inizializza la macchina a stati secondaria per BALANCE
        self.balance_fsm = None  # Sarà istanziato al primo ingresso in BALANCE

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
            |> range(start: -1m)
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

        # Controllo per passare a FILL
        if self.height < DAM_MIN_HEIGHT * DAM_HEIGHT:
            print(f"FSM: Transitioning to FILL - Height below {DAM_MIN_HEIGHT * DAM_HEIGHT:.2f}")
            self.set_state("FILL")
            return

        # Controllo per passare a BALANCE
        if DAM_MIN_HEIGHT * DAM_HEIGHT <= self.height < DAM_CRITICAL_HEIGHT * DAM_HEIGHT:
            print(f"FSM: Transitioning to BALANCE - Height within safe range.")
            self.set_state("BALANCE")
            return

        # Controllo per passare a EMERGENCY
        if self.height >= DAM_CRITICAL_HEIGHT * DAM_HEIGHT:
            print(f"FSM: Transitioning to EMERGENCY - Height above {DAM_CRITICAL_HEIGHT * DAM_HEIGHT:.2f}")
            self.set_state("EMERGENCY")
            return



    def fill(self):
        """Stato FILL: Riempimento del lago."""
        print("FSM: FILL - Increasing lake level...")
        self.actions[POWER_GATE_ID] = 0
        #print(f"-----Switching to balance when {self.height} >= {DAM_MIN_HEIGHT * DAM_HEIGHT}")
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
        # Inizializza la FSM secondaria per BALANCE se non esiste
        if not self.balance_fsm:
            self.balance_fsm = BalanceFSM()
            print("FSM: Initialized Balance FSM.")

        # Esegui lo stato corrente della FSM secondaria
        self.balance_fsm.execute(self)



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
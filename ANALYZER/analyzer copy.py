import os
import time
from influxdb_client import InfluxDBClient, QueryApi, Point, WriteOptions  # type: ignore
from dotenv import load_dotenv  # type: ignore
import threading
# Carica le variabili dal file .env
load_dotenv()

# Variabili dal .env
DAM_UNIQUE_ID = os.getenv("DAM_UNIQUE_ID")
DAM_VOLUME = float(os.getenv("DAM_VOLUME"))  # Volume iniziale
DAM_TOTAL_VOLUME = float(os.getenv("DAM_TOTAL_VOLUME"))  # Volume massimo

DAM_HEIGHT = float(os.getenv("DAM_HEIGHT", 0))  # Altezza della diga (non usata per ora)
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("DOCKER_INFLUXDB_INIT_ORG")
INFLUXDB_BUCKET = os.getenv("DOCKER_INFLUXDB_INIT_BUCKET")

BUCKET_FLOWS_DATA=os.getenv("BUCKET_FLOWS_DATA")
VOLUME_SENSOR_DATA= os.getenv("VOLUME_SENSOR_DATA")
VOLUME_FIELD=os.getenv("VOLUME_FIELD")
HEIGHT_FIELD=os.getenv("HEIGHT_FIELD")

QUERY_INTERVAL = int(os.getenv("QUERY_INTERVAL"))  # Intervallo delle query e del ciclo in secondi

# Connessione a InfluxDB
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
query_api = client.query_api()

write_api = client.write_api(
    write_options=WriteOptions(
        batch_size=10,
        flush_interval=5000,
        retry_interval=1000,
        max_retries=3,
        max_retry_delay=5000,
        exponential_base=2
    ))
# Variabile per il volume dinamico
current_volume = DAM_VOLUME
# Variabili globali per i timestamp
previous_time = None
current_time = None


def calculate_initial_volume():
    try:
        inflow_query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
            |> range(start: 0)
            |> filter(fn: (r) => r._measurement == "{BUCKET_FLOWS_DATA}")
            |> filter(fn: (r) => r._field == "total_inflow")
            |> sum()
        '''
        outflow_query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
            |> range(start: 0)
            |> filter(fn: (r) => r._measurement == "{BUCKET_FLOWS_DATA}")
            |> filter(fn: (r) => r._field == "total_outflow")
            |> sum()
        '''
        inflow_result = query_api.query(org=INFLUXDB_ORG, query=inflow_query)
        outflow_result = query_api.query(org=INFLUXDB_ORG, query=outflow_query)

        total_inflow = sum([record.get_value() for table in inflow_result for record in table.records])
        total_outflow = sum([record.get_value() for table in outflow_result for record in table.records])

        initial_volume = max(0, total_inflow - total_outflow)
        print(f"INITIAL VOLUME: {initial_volume} m³ (Inflow: {total_inflow} m³, Outflow: {total_outflow} m³)")
        return initial_volume
    except Exception as e:
        print(f"Error in calculate_initial_volume: {e}")
        return DAM_VOLUME  # Fallback al volume di default


def calculate_and_update_volume():
    """Aggiorna il volume attuale basandosi su inflow e outflow recenti."""
    global current_volume, previous_time, current_time

    # Inizializza il timestamp iniziale
    if previous_time is None:
        previous_time = int(time.time()) - 1

    while True:
        try:
            # Calcola il timestamp corrente
            current_time = int(time.time())

            # Stampa i timestamp per verificare
            print(f"Timestamps: previous_time={previous_time}, current_time={current_time}")

            # Passa i due timestamp alle funzioni
            inflow = get_total_inflow(previous_time, current_time)
            outflow = get_total_outflow(previous_time, current_time)

            inflow = max(0, inflow)
            outflow = max(0, outflow)

            # Aggiorna il volume corrente
            current_volume += inflow - outflow
            print(f"ANALYZER: Updated Volume: {current_volume} m³ (Inflow: {inflow} m³, Outflow: {outflow} m³)")
            
            # Calcola l'altezza del lago
            lake_height = calculate_lake_height(current_volume)
            print(f"ANALYZER: Calculated Lake Height: {lake_height} m")
            
            # Scrivi solo il volume attuale su InfluxDB
            point = Point(f"{VOLUME_SENSOR_DATA}") \
                .field(f"{VOLUME_FIELD}", float(current_volume)) \
                .field(f"{HEIGHT_FIELD}", float(lake_height)) \
                .time(time.strftime("%Y-%m-%dT%H:%M:%SZ"))
            write_api.write(bucket=INFLUXDB_BUCKET, record=point)

            # Aggiorna il valore di `previous_time`
            previous_time = current_time

        except Exception as e:
            print(f"Error in calculate_and_update_volume: {e}")
        
        time.sleep(QUERY_INTERVAL)

def get_total_inflow(start_time, end_time):
    """Recupera la somma totale dell'inflow da InfluxDB tra start_time e end_time."""
    query = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
        |> range(start: {start_time}, stop: {end_time})
        |> filter(fn: (r) => r._measurement == "{BUCKET_FLOWS_DATA}")
        |> filter(fn: (r) => r._field == "total_inflow")
        |> sum()
    '''
    try:
        result = query_api.query(org=INFLUXDB_ORG, query=query)
        for table in result:
            for record in table.records:
                return float(record.get_value())
    except Exception as e:
        print(f"Analyzer: Error retrieving total inflow: {e}")
    return 0.0

def get_total_outflow(start_time, end_time):
    """Recupera la somma totale dell'outflow da InfluxDB tra start_time e end_time."""
    query = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
        |> range(start: {start_time}, stop: {end_time})
        |> filter(fn: (r) => r._measurement == "{BUCKET_FLOWS_DATA}")
        |> filter(fn: (r) => r._field == "total_outflow")
        |> sum()
    '''
    try:
        result = query_api.query(org=INFLUXDB_ORG, query=query)
        for table in result:
            for record in table.records:
                return float(record.get_value())
    except Exception as e:
        print(f"Analyzer: Error retrieving total outflow: {e}")
    return 0.0

def calculate_lake_height(volume):
    """Calcola l'altezza del lago basandosi sul volume attuale."""
    try:
        height = (volume / DAM_TOTAL_VOLUME) * DAM_HEIGHT
        return max(0, min(height, DAM_HEIGHT))  # Limita l'altezza tra 0 e DAM_HEIGHT
    except Exception as e:
        print(f"Error calculating lake height: {e}")
        return 0.0


if __name__ == "__main__":
    try:
        print("Initializing Analyzer...")
        current_volume = calculate_initial_volume()  # Calcola il volume iniziale una sola volta
        print(f"Starting Analyzer with initial volume: {current_volume} m³")

        # Avvia il ciclo di aggiornamento del volume in un thread separato
        volume_thread = threading.Thread(target=calculate_and_update_volume, daemon=True)
        volume_thread.start()

        # Mantieni il programma in esecuzione
        while True:
            time.sleep(1)  # Mantieni il main thread attivo
    except KeyboardInterrupt:
        print("Analyzer stopped.")
    except Exception as e:
        print(f"Error: {e}")

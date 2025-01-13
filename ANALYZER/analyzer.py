import os
import time
from influxdb_client import InfluxDBClient, QueryApi, Point, WriteOptions  # type: ignore
from dotenv import load_dotenv  # type: ignore
import threading
import random
import joblib # type: ignore
import numpy as np # type: ignore
import pandas as pd # type: ignore
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, WriteOptions # type: ignore
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
BUCKET_PREDICTED_DATA = os.getenv("BUCKET_PREDICTED_DATA")
VOLUME_SENSOR_DATA= os.getenv("VOLUME_SENSOR_DATA")
VOLUME_FIELD=os.getenv("VOLUME_FIELD")
HEIGHT_FIELD=os.getenv("HEIGHT_FIELD")

QUERY_INTERVAL = int(os.getenv("QUERY_INTERVAL"))  # Intervallo delle query e del ciclo in secondi
DUMMY_VOLUME = float(os.getenv("DUMMY_VOLUME", 0))  # Volume forzato (default 0, disabilitato)
USE_DUMMY_VOLUME = os.getenv("USE_DUMMY_VOLUME", "false").lower() == "true"  # Abilita/disabilita il DUMMY_VOLUME

MODEL_BOITE = "boite_random_forest.pkl"
MODEL_PIAVE = "piave_random_forest.pkl"

model_boite = joblib.load(MODEL_BOITE)
model_piave = joblib.load(MODEL_PIAVE)

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
dummy_volume_used = False


def calculate_initial_volume():
    global current_volume
    try:
        if USE_DUMMY_VOLUME and DUMMY_VOLUME > 0:
            print(f"INITIAL VOLUME OVERRIDDEN: {DUMMY_VOLUME} m³ (Dummy Volume Enabled)")
            return DUMMY_VOLUME

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
    global current_volume, previous_time, current_time, dummy_volume_used

    # Inizializza il timestamp iniziale
    if previous_time is None:
        previous_time = int(time.time()) - 1

    while True:
        try:
            current_time = int(time.time())
            print(f"Timestamps: previous_time={previous_time}, current_time={current_time}")

            # Usa DUMMY_VOLUME solo una volta se abilitato
            if USE_DUMMY_VOLUME and DUMMY_VOLUME > 0 and not dummy_volume_used:
                print(f"USING DUMMY VOLUME: {DUMMY_VOLUME} m³ (Dummy Volume Enabled)")
                current_volume = DUMMY_VOLUME
                dummy_volume_used = True  # Evita di sovrascrivere nuovamente
            else:
                inflow = get_total_inflow(previous_time, current_time)
                outflow = get_total_outflow(previous_time, current_time)

                inflow = max(0, inflow)
                outflow = max(0, outflow)

                # Aggiorna il volume corrente
                current_volume += inflow - outflow

            print(f"ANALYZER: Updated Volume: {current_volume} m³ (Inflow: {inflow} m³, Outflow: {outflow} m³)")

            # Calcola l'altezza
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
        height = max(0, min(height, DAM_HEIGHT))  # Limita l'altezza tra 0 e DAM_HEIGHT
        # Applica la fluttuazione casuale
        #height = apply_random_fluctuation(height)
        return height
    except Exception as e:
        print(f"Error calculating lake height: {e}")
        return 0.0

# Funzione per applicare una fluttuazione casuale
def apply_random_fluctuation(height):
    """Applica una fluttuazione casuale in percentuale all'altezza."""
    try:
        fluctuation_percentage = random.uniform(0.001, 0.01)  # Tra 0.1% (0.001) e 2% (0.02)
        fluctuation = height * fluctuation_percentage
        # Fluttua verso l'alto o verso il basso
        if random.choice([True, False]):
            height += fluctuation
        else:
            height -= fluctuation
        return max(0, height)  # Garantisce che l'altezza non sia negativa
    except Exception as e:
        print(f"Error applying fluctuation: {e}")
        return height


def predict_volume(model, day_of_year, hour_of_day):
    """Prevede la portata basata sul giorno dell'anno e l'ora."""
    sin_day = np.sin(2 * np.pi * day_of_year / 365)
    cos_day = np.cos(2 * np.pi * day_of_year / 365)
    sin_hour = np.sin(2 * np.pi * hour_of_day / 24)
    cos_hour = np.cos(2 * np.pi * hour_of_day / 24)

    # Prepara i dati di input per il modello
    X_input = pd.DataFrame([{
        'sin_day': sin_day,
        'cos_day': cos_day,
        'sin_hour': sin_hour,
        'cos_hour': cos_hour
    }])

    return model.predict(X_input)[0]

def update_influx_with_long_term_predictions():
    """Genera e aggiorna le previsioni di portata oraria per i prossimi 7 giorni e calcola la media giornaliera."""
    now = datetime.now()
    predictions = []

    for day_offset in range(0, 365):  # Prevedi per 365 giorni (1 anno)
        daily_total = 0
        for hour in range(0, 24):  # Prevedi per ogni ora del giorno
            future_date = now + timedelta(days=day_offset, hours=hour)
            day_of_year = future_date.timetuple().tm_yday
            hour_of_day = future_date.hour

            # Prevedi per ciascun fiume
            boite_inflow = predict_volume(model_boite, day_of_year, hour_of_day)
            piave_inflow = predict_volume(model_piave, day_of_year, hour_of_day)
            total_inflow = boite_inflow + piave_inflow

            daily_total += total_inflow  # Somma totale giornaliera

            # Scrivi il risultato orario in InfluxDB
            point = Point(f"{BUCKET_PREDICTED_DATA}") \
                .field("total_inflow", float(total_inflow)) \
                .field("boite_inflow", float(boite_inflow)) \
                .field("piave_inflow", float(piave_inflow)) \
                .time(future_date.strftime("%Y-%m-%dT%H:%M:%SZ"))

            predictions.append(point)
            #print(f"Predicted {future_date}: Boite={boite_inflow:.2f}, Piave={piave_inflow:.2f}, Total={total_inflow:.2f}")

        # Calcola la media giornaliera
        daily_average = daily_total / 24
        #print(f"Daily Average Prediction for {future_date.date()}: Total Inflow={daily_average:.2f}")

    # Scrittura finale su InfluxDB
    write_api.write(bucket=INFLUXDB_BUCKET, record=predictions)
    print("Long-term predictions successfully updated.")



def prediction_thread():
    """Thread dedicato per aggiornare le previsioni periodicamente."""
    while True:
        print("Starting long-term prediction update...")
        try:
            update_influx_with_long_term_predictions()
        except Exception as e:
            print(f"Error during long-term predictions: {e}")
        print("Prediction update complete....")
        time.sleep(3600)  # Ripeti ogni ora


if __name__ == "__main__":


    try:
        print("Initializing Analyzer...")
        current_volume = calculate_initial_volume()  # Calcola il volume iniziale una sola volta
        print(f"Starting Analyzer with initial volume: {current_volume} m³")

        # Avvia il ciclo di aggiornamento del volume in un thread separato
        volume_thread = threading.Thread(target=calculate_and_update_volume, daemon=True)
        volume_thread.start()

        
        prediction_thread_handle = threading.Thread(target=prediction_thread, daemon=True)
        prediction_thread_handle.start()

        # Mantieni il programma in esecuzione
        while True:
            time.sleep(1)  # Mantieni il main thread attivo
    except KeyboardInterrupt:
        print("Analyzer stopped.")
    except Exception as e:
        print(f"Error: {e}")

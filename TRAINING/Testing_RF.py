import os
import pandas as pd # type: ignore
import numpy as np # type: ignore
import joblib # type: ignore
from datetime import datetime, timedelta
import random


# Funzione per generare 5 date casuali durante l'anno con orari casuali
def generate_random_test_dates(year, n_dates=5):
    test_dates = []
    for _ in range(n_dates):
        # Genera un giorno casuale nell'anno
        random_day = random.randint(1, 365)
        random_date = datetime(year, 1, 1) + timedelta(days=random_day - 1)

        # Aggiungi un'ora casuale alla data
        random_hour = random.randint(0, 23)
        test_dates.append(random_date.replace(hour=random_hour))
    return test_dates


# Funzione per testare un modello su una serie di date
def test_model(model, river_name, test_dates):
    print(f"Testing model: {river_name}")
    results = []
    for date in test_dates:
        # Crea feature stagionali per la data
        day_of_year = date.timetuple().tm_yday
        hour_of_day = date.hour
        sin_day = np.sin(2 * np.pi * day_of_year / 365)
        cos_day = np.cos(2 * np.pi * day_of_year / 365)
        sin_hour = np.sin(2 * np.pi * hour_of_day / 24)
        cos_hour = np.cos(2 * np.pi * hour_of_day / 24)

        # Prepara i dati di input per il modello
        X_input = pd.DataFrame([{
            "sin_day": sin_day,
            "cos_day": cos_day,
            "sin_hour": sin_hour,
            "cos_hour": cos_hour
        }])

        # Predici il valore
        prediction = model.predict(X_input)[0]
        print("Test su 5 date random ed orari random")
        results.append({"Date": date, "Prediction": prediction})

    # Stampa i primi n risultati
    for result in results:
        print(f"Data: {result['Date'].strftime('%Y-%m-%d %H:%M')}, Predizione: {result['Prediction']:.2f} m³/s")

def calculate_total_volume(model):
    total_volume = 0
    start_date = datetime(2024, 1, 1)  # Modifica l'anno se necessario
    for day in range(365):  # Per ciascun giorno dell'anno
        for hour in range(24):  # Per ciascuna ora
            current_date = start_date + timedelta(days=day, hours=hour)
            # Crea feature stagionali per la data
            day_of_year = current_date.timetuple().tm_yday
            hour_of_day = current_date.hour
            sin_day = np.sin(2 * np.pi * day_of_year / 365)
            cos_day = np.cos(2 * np.pi * day_of_year / 365)
            sin_hour = np.sin(2 * np.pi * hour_of_day / 24)
            cos_hour = np.cos(2 * np.pi * hour_of_day / 24)

            # Prepara i dati di input per il modello
            X_input = pd.DataFrame([{
                "sin_day": sin_day,
                "cos_day": cos_day,
                "sin_hour": sin_hour,
                "cos_hour": cos_hour
            }])

            # Predici il valore e somma
            prediction = model.predict(X_input)[0]
            total_volume += prediction

    return total_volume
def predict_volume(day_of_year, hour_of_day):
    # Calcola le feature sinusoidali
    sin_day = np.sin(2 * np.pi * day_of_year / 365)
    cos_day = np.cos(2 * np.pi * day_of_year / 365)
    sin_hour = np.sin(2 * np.pi * hour_of_day / 24)
    cos_hour = np.cos(2 * np.pi * hour_of_day / 24)

    # Prepara le feature per la predizione
    X_input = pd.DataFrame([{
        'sin_day': sin_day,
        'cos_day': cos_day,
        'sin_hour': sin_hour,
        'cos_hour': cos_hour
    }])

    # Predizione del volume
    prediction = model.predict(X_input)[0]
    return prediction

def predict_manual():
    try:
        # Richiedi input manuale
        date_input = input("Inserisci una data nel formato YYYY-MM-DD: ")
        hour_input = int(input("Inserisci un'ora (0-23): "))

        # Converti la data in giorno dell'anno
        manual_date = datetime.strptime(date_input, "%Y-%m-%d")
        day_of_year_manual = manual_date.timetuple().tm_yday

        # Predizione con i valori forniti
        predicted_volume_manual = predict_volume(day_of_year_manual, hour_input)

        # Output del risultato
        print(f"Data manuale inserita: {manual_date.strftime('%Y-%m-%d')} Ora: {hour_input}")
        print(f"Volume predetto per il giorno {day_of_year_manual} e ora {hour_input}: {predicted_volume_manual:.2f} m³")
    except Exception as e:
        print(f"Errore nell'inserimento dei dati: {e}")

# Cerca tutti i file .pkl nella cartella corrente
current_dir = os.getcwd()
pkl_files = [file for file in os.listdir(current_dir) if file.endswith(".pkl")]

# Genera date casuali di test
test_dates = generate_random_test_dates(2024, n_dates=5)  # Cambia l'anno se necessario

# Testa ciascun modello
for pkl_file in pkl_files:
    try:
        # Carica il modello
        model = joblib.load(pkl_file)
        river_name = os.path.splitext(pkl_file)[0]
        print(f"Modello {river_name} caricato con successo.")
        # Testa il modello
        # Testa su 5 gg random ad orario random
        test_model(model, river_name, test_dates)
        # Calcola il volume in m3 anno per un confronto con i dati reali..
        calculate_total_vol = calculate_total_volume(model)
        print("---------------------------")
        print(f"Volume totale annuo --->>> {calculate_total_vol:.2f}   m³")

        # Calcola la predizione ad oggi, questa funzione sarà usata nel progetto.
        now = datetime.now()
        day_of_year = now.timetuple().tm_yday
        hour_of_day = now.hour
        # Predizione basata sull'ora corrente
        predicted_volume = predict_volume(day_of_year, hour_of_day)
        print("---------------------------")
        print(f"Volume predetto per oggi {now.strftime('%Y-%m-%d %H:%M:%S')} con {pkl_file} --->>> {predicted_volume:.2f}   m³")
        print("---------------------------")


    except Exception as e:
        print(f"Errore con il file {pkl_file}: {e}")


# Chiamata della funzione manuale
#predict_manual()


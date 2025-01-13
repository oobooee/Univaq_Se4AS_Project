import os
import pandas as pd # type: ignore
import numpy as np # type: ignore
from sklearn.ensemble import RandomForestRegressor # type: ignore
from sklearn.model_selection import train_test_split, GridSearchCV # type: ignore
from sklearn.metrics import mean_squared_error # type: ignore
import joblib # type: ignore

# Directory base contenente le cartelle dei fiumi
base_dir = "fiumi_dati"  # Modifica con il percorso corretto

# Funzione per calcolare la media oraria e preparare il dataset
def calculate_hourly_average(file):
    # Carica il dataset
    df = pd.read_csv(file, sep=";")

    # Converti le colonne in numerico
    df['PORT_MED'] = pd.to_numeric(df['PORT_MED'], errors='coerce')
    df['ORA'] = pd.to_numeric(df['ORA'], errors='coerce')
    df['GIORNO'] = pd.to_numeric(df['GIORNO'], errors='coerce')
    df['MESE'] = pd.to_numeric(df['MESE'], errors='coerce')

    # Rimuovi righe con valori mancanti
    df = df.dropna(subset=['PORT_MED', 'ORA', 'GIORNO', 'MESE'])

    # Aggiungi una colonna per la data completa
    df['DateTime'] = pd.to_datetime(dict(year=df['ANNO'], month=df['MESE'], day=df['GIORNO'], hour=df['ORA']))

    # Raggruppa per data e ora e calcola la media
    hourly_avg = df.groupby('DateTime')['PORT_MED'].mean().reset_index()
    return hourly_avg

# Funzione per eseguire il training per ciascun fiume
def train_model_for_river(river_name, river_files):
    # Calcola la media oraria per ciascun file
    hourly_dfs = [calculate_hourly_average(file) for file in river_files]

    # Combina tutti i dataset
    combined_hourly = pd.concat(hourly_dfs, ignore_index=True)
    combined_hourly.to_csv(f"{river_name}_hourly_mean_flow.csv", index=False)
    print(f"Dataset aggregato orario salvato come '{river_name}_hourly_mean_flow.csv'.")

    # Aggiungi feature temporali (giorno dell'anno, ora del giorno)
    combined_hourly['DayOfYear'] = combined_hourly['DateTime'].apply(lambda x: pd.to_datetime(x).timetuple().tm_yday)
    combined_hourly['HourOfDay'] = pd.to_datetime(combined_hourly['DateTime']).dt.hour

    # Aggiungi feature stagionali (seno e coseno per giorno e ora)
    combined_hourly['sin_day'] = np.sin(2 * np.pi * combined_hourly['DayOfYear'] / 365)
    combined_hourly['cos_day'] = np.cos(2 * np.pi * combined_hourly['DayOfYear'] / 365)
    combined_hourly['sin_hour'] = np.sin(2 * np.pi * combined_hourly['HourOfDay'] / 24)
    combined_hourly['cos_hour'] = np.cos(2 * np.pi * combined_hourly['HourOfDay'] / 24)

    # Definisci le feature (X) e il target (y)
    features = ['sin_day', 'cos_day', 'sin_hour', 'cos_hour']
    X = combined_hourly[features]
    y = combined_hourly['PORT_MED']

    # Dividi i dati in training e test
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, shuffle=True)

    # Ottimizzazione tramite Grid Search
    param_grid = {
        'n_estimators': [50, 100, 200],
        'max_depth': [10, 20, None],
        'min_samples_split': [2, 5, 10]
    }

    grid_search = GridSearchCV(
        RandomForestRegressor(random_state=42),
        param_grid,
        cv=3,
        scoring='neg_mean_squared_error',
        verbose=1
    )
    grid_search.fit(X_train, y_train)

    # Migliori parametri trovati
    print(f"Best parameters for {river_name}: {grid_search.best_params_}")

    # Modello ottimizzato
    best_model = grid_search.best_estimator_

    # Valutazione del modello
    y_pred = best_model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    print(f"Mean Squared Error for {river_name}: {mse}")

    # Salva il modello
    model_filename = f"{river_name}_random_forest.pkl"
    joblib.dump(best_model, model_filename)
    print(f"Modello Random Forest ottimizzato salvato come '{model_filename}'.")

# Cerca tutte le cartelle per ciascun fiume
for river_folder in os.listdir(base_dir):
    river_path = os.path.join(base_dir, river_folder)
    if os.path.isdir(river_path):
        # Cerca tutti i file CSV nella cartella
        river_files = [os.path.join(river_path, file) for file in os.listdir(river_path) if file.endswith(".csv")]
        if river_files:
            river_name = river_folder  # Usa il nome della cartella come identificatore del fiume
            train_model_for_river(river_name, river_files)

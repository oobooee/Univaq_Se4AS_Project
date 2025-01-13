# Usa un'immagine Python slim
FROM python:3.12-slim

# Imposta la directory di lavoro nel container
WORKDIR /app

# Copia tutti i file necessari nel container
COPY . ./

# Installa le librerie elencate in lib.txt
RUN pip install --no-cache-dir -r lib.txt

# Comando per avviare il sensore
CMD ["python", "-u", "pump_solar_1.py"]


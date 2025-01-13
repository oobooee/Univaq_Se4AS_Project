# Usa un'immagine Python slim
FROM python:3.12-slim

# Imposta la directory di lavoro nel container
WORKDIR /app

# Copia i file necessari nel container
COPY . ./

# Installa le dipendenze
RUN pip install --no-cache-dir -r lib.txt

# Comando per avviare il programma
CMD ["python", "-u", "gate_2.py"]

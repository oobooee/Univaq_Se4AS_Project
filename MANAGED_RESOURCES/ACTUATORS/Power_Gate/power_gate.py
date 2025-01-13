import os
import json
import time
import paho.mqtt.client as mqtt  # type: ignore
from dotenv import load_dotenv  # type: ignore

# Carica le variabili dal file .env
load_dotenv()

GATE_ID = os.getenv("POWER_GATE_ID")
DAM_UNIQUE_ID = os.getenv("DAM_UNIQUE_ID")
GATE_TOPIC_PREFIX = os.getenv("GATE_TOPIC_PREFIX")
COMMAND_TOPIC = f"{DAM_UNIQUE_ID}/{GATE_TOPIC_PREFIX}/{GATE_ID}/command"
STATE_TOPIC = f"{DAM_UNIQUE_ID}/{GATE_TOPIC_PREFIX}/{GATE_ID}/state"
GATE_PUBLISH_DELAY = int(os.getenv("GATE_PUBLISH_DELAY"))

MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")



# Stato della gate
gate_open_percentage = 0


def on_connect(client, userdata, flags, rc):
    """Callback per la connessione al broker."""
    if rc == 0:
        print(f"{GATE_ID}: Connected to MQTT Broker!")
        client.subscribe(COMMAND_TOPIC)
    else:
        print(f"{GATE_ID}: Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    """Gestisce i comandi ricevuti via MQTT."""
    global gate_open_percentage
    try:
        command = json.loads(msg.payload)
        if "open_percentage" in command:
            gate_open_percentage = max(0, min(100, command["open_percentage"]))
            print(f"{GATE_ID}: Set to {gate_open_percentage}%")
    except Exception as e:
        print(f"{GATE_ID}: Error processing command: {e}")

def publish_state(client):
    """Pubblica lo stato corrente della gate."""
    global gate_open_percentage
    state = {
        "gate_id": GATE_ID,
        "open_percentage": gate_open_percentage
    }
    client.publish(STATE_TOPIC, json.dumps(state))
    print(f"{GATE_ID}: Published state: {state}")

if __name__ == "__main__":
    # Configura il client MQTT
    client = mqtt.Client(GATE_ID)
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 180)
        client.loop_start()

        # Loop principale per pubblicare lo stato
        while True:
            publish_state(client)
            time.sleep(GATE_PUBLISH_DELAY)

    except KeyboardInterrupt:
        print(f"{GATE_ID}: Shutting down.")
    finally:
        client.loop_stop()
        client.disconnect()

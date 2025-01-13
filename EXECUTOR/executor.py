import os
import json
import time
import threading
from paho.mqtt.client import Client  # type: ignore
from dotenv import load_dotenv  # type: ignore

# Carica le variabili dal file .env
load_dotenv()

# Variabili dal .env
DAM_UNIQUE_ID = os.getenv("DAM_UNIQUE_ID")
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
PLANNER_TOPIC_PREFIX = os.getenv("PLANNER_TOPIC_PREFIX")
GATE_TOPIC_PREFIX = os.getenv("GATE_TOPIC_PREFIX")

# Stato MQTT
is_connected = False
gate_states = {}
data_lock = threading.Lock()
stop_event = threading.Event()

# Configurazione MQTT
mqtt_client = Client("Executor")
mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)

def on_connect(client, userdata, flags, rc):
    """Callback per la connessione al broker MQTT."""
    global is_connected
    if rc == 0:
        print("EXECUTOR: Connected to MQTT Broker!")
        is_connected = True
        action_topic = f"{DAM_UNIQUE_ID}/{PLANNER_TOPIC_PREFIX}"
        mqtt_client.subscribe(action_topic)
        print(f"EXECUTOR: Subscribed to {action_topic}")
    else:
        print(f"EXECUTOR: Connection failed with return code {rc}")

def on_disconnect(client, userdata, rc):
    """Callback per la disconnessione dal broker MQTT."""
    global is_connected
    print("EXECUTOR: Disconnected from MQTT Broker.")
    is_connected = False

def on_message(client, userdata, msg):
    """Gestione dei messaggi MQTT."""
    global gate_states
    topic = msg.topic
    try:
        payload = json.loads(msg.payload)
    except json.JSONDecodeError:
        print(f"EXECUTOR: Invalid JSON payload on topic {topic}")
        return

    if topic == f"{DAM_UNIQUE_ID}/{PLANNER_TOPIC_PREFIX}":
        process_command(payload)

def process_command(payload):
    """Elabora i comandi ricevuti dal Planner."""
    global gate_states
    try:
        for gate_id, open_percentage in payload.items():
            if not isinstance(open_percentage, (int, float)):
                print(f"EXECUTOR: Invalid open_percentage for gate {gate_id}: {open_percentage}")
                continue

            with data_lock:
                gate_states[gate_id] = {
                    "open_percentage": max(0, min(100, open_percentage))  # Limita tra 0 e 100
                }
                print(f"EXECUTOR: Command processed - Gate {gate_id} set to {open_percentage}%")
                send_gate_command(gate_id, open_percentage)
    except Exception as e:
        print(f"EXECUTOR: Error processing command: {e}")

def send_gate_command(gate_id, open_percentage):
    """Pubblica il comando per una specifica porta sul topic MQTT appropriato."""
    try:
        command_topic = f"{DAM_UNIQUE_ID}/{GATE_TOPIC_PREFIX}/{gate_id}/command"

        payload = {"open_percentage": open_percentage}
        mqtt_client.publish(command_topic, json.dumps(payload), qos=1)
        print(f"EXECUTOR: Published command - Gate {gate_id}: {open_percentage}% on topic {command_topic}")
    except Exception as e:
        print(f"EXECUTOR: Error sending gate command: {e}")

def reconnect_mqtt():
    """Gestisce i tentativi di riconnessione al broker."""
    global is_connected
    while not is_connected and not stop_event.is_set():
        try:
            print("EXECUTOR: Attempting to reconnect to MQTT Broker...")
            mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
            mqtt_client.loop_start()
            time.sleep(5)
        except Exception as e:
            print(f"EXECUTOR: Reconnection failed: {e}")
            time.sleep(10)

if __name__ == "__main__":
    try:
        mqtt_client.on_connect = on_connect
        mqtt_client.on_disconnect = on_disconnect
        mqtt_client.on_message = on_message

        reconnect_mqtt_thread = threading.Thread(target=reconnect_mqtt, daemon=True)
        reconnect_mqtt_thread.start()

        print("EXECUTOR: Processing commands...")
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        print("EXECUTOR: Stopping gracefully.")
    except Exception as e:
        print(f"EXECUTOR: Critical error: {e}")
    finally:
        stop_event.set()
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        print("EXECUTOR: Resources released.")

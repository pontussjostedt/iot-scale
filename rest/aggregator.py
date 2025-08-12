import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import psycopg2
import env

con = psycopg2.connect(
    dbname=env.PG_DB,
    user=env.PG_USERNAME,
    password=env.PG_PASSWORD,
    host=env.PG_HOST,
    port=env.PG_HOST
)
cur = con.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    topic TEXT,
    msg TEXT
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS experiments (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    name TEXT NOT NULL,
    about TEXT
);
""")
con.commit()

def on_connect(client, userdata, flags, rc, _):
    print("Connected to MQTT with result code :)", rc)
    client.subscribe("scale/data/#")

def on_message(client, userdata, msg):
    print(f"MQTT message received on {msg.topic}: {msg.payload.decode()}")
    cur.execute("INSERT INTO data(topic, msg) VALUES (%s, %s)", (msg.topic, msg.payload.decode()))
    con.commit()

def start_mqtt():
    client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    client.username_pw_set(env.MOSQUITTO_USERNAME, env.MOSQUITTO_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(env.MOSQUITTO_IP, env.MOSQUITTO_PORT, 60) 
    client.loop_forever()

start_mqtt()

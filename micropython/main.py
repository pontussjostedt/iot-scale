from machine import Pin
from simple import MQTTClient
from hx711_gpio import HX711
import time
import env
import network
from dht import DHT11

TOPIC_TARE = b"scale/tare"
TOPIC_WEIGHT = b"scale/data/weight"
TOPIC_TEMPERATURE = b"scale/data/temperature"
TOPIC_HUMIDITY = b"scale/data/humidity"

data_pin = Pin(19, Pin.IN, pull=Pin.PULL_DOWN)
clock_pin = Pin(18, Pin.OUT)
led = Pin(25, Pin.OUT)
pin_4 = Pin(4)

tare_flag = False

def init_wifi(ssid: str, password: str):
    
    def __inner() -> bool:
        wlan = network.WLAN(network.STA_IF)
        wlan.active(True)
        
        if not wlan.isconnected():
            wlan.connect(ssid, password)
            
        timeout = 10
        while not wlan.isconnected() and timeout > 0:
            print('.', end='')
            led.toggle()
            time.sleep(1)
            timeout -= 1
            
        led.high()
        if wlan.isconnected():
            print("CONNECTED!! :)")
            return True
        else:
            print("failed to connect :(((")
            return False
    while not __inner():
        pass
        
    
        
def read_weight(hx711: HX711, offset: float, scale: float) -> float:
    raw_wt = hx711.read_average()
    weight = (raw_wt - offset) * scale
    return weight

class Publisher:
    def __init__(self, tolerance: float, topic: bytes, last_value: float | None = None) -> None:
        self._last_value = last_value
        self._tolernace = tolerance
        self.topic = topic
        
    def offer(self, new_value: float):
        if self._last_value is None or abs(self._last_value - new_value) > self._tolernace:
            self._last_value = new_value
            self.publish(new_value)
        
    def publish(self, new_value: float):
        print(f"{self.topic}: {new_value}")
        c.publish(self.topic, f"{new_value}".encode())
        
        
def subscribe_callback(topic, _msg):
    global tare_flag
    if topic == TOPIC_TARE:
        tare_flag = True
        
        
init_wifi(env.WIFI_USERNAME, env.WIFI_PASSWORD) 

c = MQTTClient(
    env.MOSQUITTO_CLIENT_ID, 
    env.MOSQUITTO_IP, 
    port=env.MOSQUITTO_PORT, 
    user=env.MOSQUITTO_USERNAME, 
    password=env.MOSQUITTO_PASSWORD
)
c.set_callback(subscribe_callback)
c.connect()

hx711 = HX711(clock_pin, data_pin)
dht = DHT11(pin_4)

c.subscribe(TOPIC_TARE)

interval = 5_00
offset = hx711.read_average(15)

weight_publisher = Publisher(0.05, TOPIC_WEIGHT, )
temperature_publisher = Publisher(0.5, TOPIC_TEMPERATURE)
humidity_publisher = Publisher(0.5, TOPIC_HUMIDITY)

led.high()

while True:
    c.check_msg()
    
    if tare_flag:
        offset = hx711.read_average(15)
        tare_flag =  False
    
    led.toggle()
    
    dht.measure()
    weight = read_weight(hx711, offset, 0.000862944162) # magic number from experimental data is scale factor.
    temp = dht.temperature()
    hum = dht.humidity()
    
    weight_publisher.offer(weight)
    temperature_publisher.offer(temp)
    hum = humidity_publisher.offer(hum)
        
        
    time.sleep(1)
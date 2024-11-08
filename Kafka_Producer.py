# Imports
import json
import time
import random
from kafka import KafkaProducer # type: ignore
from datetime import datetime

# Generating weather data
def generate_weather_data():

    data = {'country' : 'Brazil',
            'temperature_celsius' : random.uniform(15, 40),
            'humidity' : random.uniform(30, 70),
            'precip_mm' : random.uniform(0, 10),
            'last_updated' : datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
    return data

# Producer configuration
producer_config = KafkaProducer(bootstrap_servers = 'localhost:9092',
                   value_serializer = lambda v: json.dumps(v).encode('utf-8'))

# Seding data each second
try:
    for _ in range(60):
        weather_data = generate_weather_data()
        producer_config.send('global_weather', value = weather_data)
        print(f'Sent: {weather_data}')
        time.sleep(1)
finally:
    producer_config.close()
    


# Imports
import pandas as pd # type: ignore
import json
import csv
from kafka import KafkaConsumer # type: ignore


# Configuration of the Kafka Consumer
kafka_consumer = KafkaConsumer('global_weather', bootstrap_servers = 'localhost:9092',
                               auto_offset_reset = 'latest', value_deserializer = lambda x: json.loads(x.decode('utf-8')))

# Creating an csv file to save the data
with open('weather_new_data.csv', mode = 'w', newline = '' )as csv_file:
    fieldnames = ['country', 'temperature_celsius', 'humidity', 'precip_mm', 'last_updated']
    writer = csv.DictWriter(csv_file, fieldnames = fieldnames)
    writer.writeheader()
    
    print('Listening for messages...')
    for message in kafka_consumer:
        data = message.value
        print(f'Consumed: {data}')
        writer.writerow(data)
        

# Reading the data from the new csv file

df = pd.read_csv('weather_new_data.csv')

# Printing a sumary of the new data
print(df.describe())
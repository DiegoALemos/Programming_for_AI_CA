# Imports
from flask import Flask, jsonify, request # type: ignore
from flask_restful import Api, Resource # type: ignore
from kafka import KafkaProducer # type: ignore
import pandas as pd # type: ignore
import json
from flask_cors import CORS # type: ignore

# Starting Flask
app = Flask(__name__)
# Enabling Cross-Origin Resources Sharing to access the endpoints
CORS(app)
api =  Api(app)

# Reading the dataset
df = pd.read_csv('/Users/diegolemos/Masters/ProgrammingForAi/CA/Cleaned_Global_Weather.csv')

# Creating the endpoint
class WeatherData(Resource):
    def get(self):
        data = df.to_dict(orient='records')
        response = jsonify(data)
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response
 

# Connection to the kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

class LiveUpdate(Resource):
    def post(self):
        data = request.get_json()
        producer.send('weather_updates', data)
        return{'status': 'Data sent to Kafka'}, 200
    
# Rotes
api.add_resource(WeatherData, '/data')
api.add_resource(LiveUpdate, '/update')

if __name__ == '__main__':
    app.run(debug = True, port = 5001)
 
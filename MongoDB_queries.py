# Imports
from pymongo import MongoClient # type: ignore
import pandas as pd # type: ignore
from datetime import datetime


# Configuration and conecting MongoDB
clientMongo = MongoClient('mongodb://127.0.0.1:27017')
db = clientMongo['GlobalWeather_data']
collection = db['GlobalWeather_data']
print('Connected to MongoDB')

# Read data from csv file
data = pd.read_csv('/Users/diegolemos/Masters/ProgrammingForAi/CA/Cleaned_Global_Weather.csv')

# Converting data to a dictionary
dict_data = data.to_dict(orient='records')

# Inseting collection data
collection.insert_many(dict_data)
print('Data imported successfuly...')

# MongoDB queries

# All records from a especific month (June)
june_records = list(collection.find({
    'last_updated': {
        '$gte': datetime(2024, 6, 1),
        '$lt': datetime(2024, 7, 1)
    }
}))
print('Records of june: ', june_records)

# # All records from a especific location
irelands_records = list(collection.find({'country' : 'Ireland'}))
print('Records of Ireland: ', irelands_records)

india_records = list(collection.find({'country' : 'India'}))
print('Records of India: ', india_records)

# Top 3 location with the highest temperature
hottest_locations = list(collection.find().sort('temperature_celsius', -1))

# The querie was bring the same country 3 times as it has many records of teh same place in the dataset.
# So I will include a loop an if statement to not repet the country on the ranking of top 3 hottest locations.
top_3_countries = []
seen_countries = set()

for record in hottest_locations:
    country = record['country']
    if country not in seen_countries:
        top_3_countries.append(record)
        seen_countries.add(country)
    if len(top_3_countries) == 3:
        break
    
print('Top 3 hottest locations are : ',top_3_countries)

# Top 3 location with the lowest precipitation
lowest_preciptations = list(collection.find().sort('precip_mm', 1).limit(3))
print('Top 3 locations with the lowest precipitation are : ', lowest_preciptations)

# Top 3 location with the higher humidity
highest_humidity = list(collection.find().sort('humidity', -1).limit(3))
print('Top 3 locations with the higher humidy are : ', highest_humidity)

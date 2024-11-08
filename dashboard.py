# Imports
import streamlit as st # type: ignore
import pandas as pd # type: ignore
import requests # type: ignore
import plotly.express as px # type: ignore
import time
import plotly.graph_objs as go # type: ignore
import json
from kafka import KafkaConsumer # type: ignore

# Kafka new csv file path
csv_path = 'weather_new_data.csv'

# Api Flask Url
data_url = 'http://localhost:5001/data'

# Dashboard data
st.title('Global Weather Dashboard')
st.sidebar.header('Filters')

# Loading data from API Flask
try:
    response = requests.get(data_url)
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame(data)
    
    st.write("Initial data loaded from API:", df)
    
    if df.empty:
        st.error("No data available to display")
    else:
        # Filter and visualization

        # Filter by date
        date_filter = st.sidebar.date_input("Choose a date")
        if date_filter:
             df = df[df['last_updated'].str.contains(date_filter.strftime('%Y-%m-%d'))]
        
        # Filter by country
        countries = df['country'].unique()
        country_filter = st.sidebar.multiselect('Choose the country', countries, default=countries)
        df = df[df['country'].isin(country_filter)]
        
        # Filter by temperature
        if df['temperature_celsius'].isnull().all():
            st.warning('No temperature data available.')
        else:
            min_temperature, max_temperature = int(df['temperature_celsius'].min()), int(df['temperature_celsius'].max())
            temperature_filter = st.sidebar.slider('Range of temperature', min_temperature, max_temperature, (min_temperature, max_temperature))
            df = df[(df['temperature_celsius'] >= temperature_filter[0]) & (df ['temperature_celsius'] <= temperature_filter[1])]
        
        # Filter by humidity
        if df['humidity'].isnull().all():
            st.warning('No humidity data available.')
        else:    
            min_humidity, max_humidity = int(df['humidity'].min()), int(df['humidity'].max())
            humidity_filter = st.sidebar.slider('Range of humidity', min_humidity, max_humidity, (min_humidity, max_humidity))
            df = df[(df['humidity'] >= humidity_filter[0]) & (df['humidity']<= humidity_filter[1])]
        
        # Visualizing Filters
        st.write('Filtered data', df)
        
        # Graphic with mean temperature per country
        if not df.empty:
            fig = px.bar(df, x='country', y='temperature_celsius', title='Mean Temperature by country')
            st.plotly_chart(fig)
            
            # Graphic with mean humidy per country
            if not df.empty:
                fig_humidity = px.bar(df, x='country', y='humidity', title='Mean humidity by country')
                st.plotly_chart(fig_humidity)
        else:
            st.warning("No data available after applying filters, please ajust filters.")

except requests.exceptions.RequestException as e:
    st.error('Error loading the data from Api: ' + str(e))
    
    
# Updating in real time from kafka

st.header("Live Data Updates")

def get_live_updates():
    # Consuming real time data from Kafka
    kafka_consumer = KafkaConsumer(
        'global_weather', bootstrap_servers = 'localhost:9092',
        auto_offset_reset = 'latest',
        value_deserializer = lambda x: json.loads(x.decode('utf-8'))
    )
    
    data_points = []
    timeout = time.time() + 10
    
    for message in kafka_consumer:
        data = message.value
        data_points.append(data)
        
        # Breaking the looping after 10 seconds
        if time.time() > timeout:
            break
        
    return data_points

st.subheader('Updates in Real Time')

# Visualization in real time


data_points = get_live_updates()
if data_points:
    # Converting dataframe to a graph
    live_df = pd.DataFrame(data_points)
    st.write("Live data", live_df)
        
    # Live graph
    fig_live = go.Figure()
    fig_live.add_trace(go.Scatter(x=live_df["last_updated"], y = live_df['temperature_celsius'], mode = 'lines+makers', name = 'Temperature'))
        
    fig_live.update_layout(title = 'Updates in real time - Temperature', xaxis_title = 'Time', yaxix_title = 'Temperature')   
    st.plotly_chart(fig_live)
else:
    st.warning('No live data available at the moment')
    

st.header('Real-Time Data from CSV')

# Button to update the csv data
if st.button('Update Data in Real Time from CSV'):
    # Read the cvd updated from the Kafka Consumer
    df_csv = pd.read_csv(csv_path)
    
    # Display data
    st.write('Weather data recived from CSV:')
    st.dataframe(df_csv)
    
    #  Graphic from the updated csv
    if not df_csv.empty:
        fig_csv = go.Figure()
        fig_csv.add_trace(go.Scatter(x=df_csv['last_updated'], y=df_csv['temperature_celsius'], mode = 'lines+markers', name = 'Temperature'))
        
        fig_csv.update_layout(title = 'Updates in Real Time - Temperature', xaxis_title='Time', yaxis_title = 'Temperature')
        st.plotly_chart(fig_csv)
    else:
        st.warning('No data available on the csv')
        


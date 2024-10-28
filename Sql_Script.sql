-- Creating the database
create database GlobalWeatherDB;

-- Chosing the database to be used
use GlobalWeatherDB;

-- Creating the table WeatherData with all the columns from the cleaned Global dataset.
create table WeatherData(
id int auto_increment primary key,
latitude decimal(5, 2),
longitude decimal(5,2),
last_update datetime,
temperature_celsius decimal(4,1),
wind_kph decimal(4, 1),
wind_degree int,
precip_mm decimal(4,1),
humidity int,
cloud int,
feels_like_celsius decimal(4, 1),
visibility_km decimal (4, 1),
uv_index decimal(3, 1),
gusp_kph decimal(4, 1),
air_quality_carbon_monoxide decimal(6, 1),
air_quality_ozone decimal(6, 1),
country_encoded int,
country varchar (255)
);


-- Importing the dataset to mysql
-- I tried to import the dataset using the the sql comands as described below, but unfortunatly it wasen't succefful.
-- I am facing an erro saing that the local data is disabled, and when I tried to eneble it I get access denied, even though I am using the correct password.
-- I tried to bypass this as an administrator but it didn't work either, I even dowloaded a new version of mysql and no success.
-- So the solution to import the data was using Python, the code will be on the "Global_Weather_DF.ipynb" file.
load data local infile '/Users/diegolemos/Masters/ProgrammingForAi/CA/Cleaned_Global_Weather.csv'
into table WeatherData
fields terminated by ',' enclosed by ""
lines terminated by '\n'
ignore 1 rows
(latitude, longitude, last_update, temperature_celsius, wind_kph, wind_degree, precip_mm, humidity, cloud,feels_like_celsius,
visibility_km, uv_index, gusp_kph, air_quality_carbon_monoxide, air_quality_ozone, country_encoded, country);

-- Checking if the data was imported. 
select count(*) from WeatherData;

-- Selecting the 5 biggests temperatures
select country, max(temperature_celsius) as max_temperature
from WeatherData
group by country
order by max_temperature desc
limit 5;

-- Selecting the 5 smollest precipitation
select country, min(precip_mm) as min_precipitation
from WeatherData
group by country
order by min_precipitation desc
limit 5;

-- Selecting temperatures bigger the 35 deggres
select * from WeatherData
where temperature_celsius > 35;

-- Selecting the 5 more windiest countries
select country, max(wind_kph) as max_windiest
from WeatherData
group by country
order by max_windiest desc
limit 5;

-- Selecting precipitations bigger the 15 mm
select * from WeatherData
where precip_mm > 15;

-- selecting average temperature per country
select country, avg(temperature_celsius) as avg_temperature
from WeatherData
group by country;

-- Selecting total precipitation per country
select country, sum(precio_mm) as total_precipitation
from WeatherData
group by country;

-- selecting average air quality ozone per country
select country, avg(air_quality_Ozone) as avg_ozone
from WeatherData
group by country;




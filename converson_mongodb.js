// Importing the mongo client to connect with the database
const { MongoClient } = require("mongodb");

// Configuration and connection to MongoDB
const url = "mongodb://127.0.0.1:27017";
const client = new MongoClient(url);
const databaseName = "GlobalWeather_data";
const collectionName = "GlobalWeather_data";

// When trying to get from mongoDB the top 3 hottest countries, I realize that the results weren't corrects.
// Then I notice that mongoDB is considering the temperature as a string not a number.
// So I need to convert the numeric columns back to a number.

async function convertTemperature() {
  try {
    // connection to MongoDB
    await client.connect();
    console.log("Conectando ao MongoDB");

    //Accessing the database
    const db = client.db(databaseName);
    const collection = db.collection(collectionName);

    // Converting numeric columns to a float
    const result = await collection.updateMany(
      {
        $or: [
          { temperature_celsius: { $type: "string" } },
          { wind_kph: { $type: "string" } },
          { precip_mm: { $type: "string" } },
          { humidity: { $type: "string" } },
          { cloud: { $type: "string" } },
          { feels_like_celsius: { $type: "string" } },
          { visibility_km: { $type: "string" } },
          { uv_index: { $type: "string" } },
          { gust_kph: { $type: "string" } },
          { air_quality_Carbon_Monoxide: { $type: "string" } },
          { air_quality_Ozone: { $type: "string" } },
          { latitude: { $type: "string" } },
          { longitude: { $type: "string" } },
          { country_encoded: { $type: "string" } },
        ],
      },
      [
        {
          $set: {
            temperature_celsius: { $toDouble: "$temperature_celsius" },
            wind_kph: { $toDouble: "$wind_kph" },
            precip_mm: { $toDouble: "$precip_mm" },
            humidity: { $toDouble: "$humidity" },
            cloud: { $toDouble: "$cloud" },
            feels_like_celsius: { $toDouble: "$feels_like_celsius" },
            visibility_km: { $toDouble: "$visibility_km" },
            uv_index: { $toDouble: "$uv_index" },
            gust_kph: { $toDouble: "$gust_kph" },
            air_quality_Carbon_Monoxide: {
              $toDouble: "$air_quality_Carbon_Monoxide",
            },
            air_quality_Ozone: { $toDouble: "$air_quality_Ozone" },
            latitude: { $toDouble: "$latitude" },
            longitude: { $toDouble: "$longitude" },
            country_encoded: { $toDouble: "$country_encoded" },
          },
        },
      ]
    );
    console.log(`${result.modifiedCount} numeric columns updeted to float. `);
  } catch (error) {
    console.error(
      "Error when trying to convert numeric columns to float",
      error
    );
  } finally {
    await client.close();
    console.log("Connection to mongodb closed ");
  }
}

convertTemperature();

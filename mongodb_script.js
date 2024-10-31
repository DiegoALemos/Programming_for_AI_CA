// Importing the mongo client to connect with the database
const { MongoClient } = require("mongodb");

// Configuration and connection to MongoDB
const url = "mongodb://127.0.0.1:27017";
const client = new MongoClient(url);
const databaseName = "GlobalWeather_data";
const collectionName = "GlobalWeather_data";

// Function to execute queries
async function queries() {
  try {
    // connection to MongoDB
    await client.connect();
    console.log("Conectando ao MongoDB");

    //Accessing the database
    const db = client.db(databaseName);
    const collection = db.collection(collectionName);

    // MongoDB Queries

    // All records from a especific month (June)
    const juneRecords = await collection
      .find({
        last_updated: {
          $gte: new Date("2024-06-01"),
          $lt: new Date("2024-07-01"),
        },
      })
      .toArray();
    console.log("Records from June: ", juneRecords);

    // All records from a especific location
    const irelandRecords = await collection
      .find({ country: "Ireland" })
      .toArray();
    console.log("Records from Ireland: ", irelandRecords);

    const indiaRecords = await collection.find({ country: "India" }).toArray();
    console.log("Records from India: ", indiaRecords);

    // Top 3 records with the highest temperature
    const hottestLocations = await collection
      .find({})
      .sort({ temperature_celsius: -1 })
      .limit(3)
      .toArray();
    console.log(
      "The top 3 record with the hottest temperature are: ",
      hottestLocations
    );

    // Top 3 records with the lowest preciptation
    const lowestPreciptation = await collection
      .find({})
      .sort({ precip_mm: 1 })
      .limit(3)
      .toArray();
    console.log(
      "The top 3 record with the lowest preciptations are: ",
      lowestPreciptation
    );

    // Top 3 records with highest humidity
    const highestHumidity = await collection
      .find({})
      .sort({ humidity: -1 })
      .limit(3)
      .toArray();
    console.log(
      "The top 3 record with the higher humidity are: ",
      highestHumidity
    );
  } catch (error) {
    console.error("A error heppened: ", error);
  } finally {
    await client.close();
    console.log("Connection to mongodb closed ");
  }
}

queries();

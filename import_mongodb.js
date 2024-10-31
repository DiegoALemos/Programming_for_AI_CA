// Importing the File System,  Node.js's module that allow us to work with files and sytemns
const fs = require("fs");

// Importing the csv-parser to be able to read the csv file
const csv = require("csv-parser");

// Importing the mongo client to connect with the database
const { MongoClient } = require("mongodb");

// Configuration and conecting MongoDB
const url = "mongodb://127.0.0.1:27017";
const client = new MongoClient(url);
const databaseName = "GlobalWeather_data";
const collectionName = "GlobalWeather_data";

// Function to import the data frame

async function importDfToMongoDB() {
  try {
    // connecting to MongoDB
    await client.connect();
    console.log("conecting to MongoDB");

    const db = client.db(databaseName);
    const collection = db.collection(collectionName);

    // Reading the csv file and inserting to the database
    const data = [];
    fs.createReadStream(
      "/Users/diegolemos/Masters/ProgrammingForAi/CA/Cleaned_Global_Weather.csv"
    )
      .pipe(csv())
      .on("data", (row) => {
        // Converting the column "Last_update" to date object to make it easier for the queries
        if (row.last_updated) {
          row.last_updated = new Date(row.last_updated);
        }
        data.push(row);
      })
      .on("end", async () => {
        await collection.insertMany(data);
        console.log(
          `The import of ${data.length} of data is concluded successfuly!`
        );
        await client.close();
      });
  } catch (error) {
    console.error("That was an error on the import data: ", error);
  }
}

// Calling the function
importDfToMongoDB();

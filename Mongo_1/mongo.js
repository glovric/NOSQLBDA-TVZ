const { MongoClient } = require('mongodb');
const { getMissingValues, 
        updateMissingValues, 
        createStatisticsContinuous, 
        createFrequenciesCategorical, 
        createLessThanMeansContinuous, 
        createGreaterThanMeansContinuous,
        embedCategorical, 
        embedContinuous, 
        findStdBiggerThanMeans, 
        dropDatabase } = require('./utils');

const url = 'mongodb://localhost:27017';
const client = new MongoClient(url);

async function main() {

    try {

        // Connect to Mongo, database and collection
        await client.connect();
        console.log('Connected to MongoDB.');
        const db = client.db('NOSQL');
        const collection = db.collection('weather_dataset');

        // 1. Updating missing values
        console.log("1. Finding missing values...");
        const missing = await getMissingValues(collection);

        if(missing.length > 0) {
            console.log("Updating missing values.")
            await updateMissingValues(collection);
        }
        else {
            console.log("No missing values.");
        }

        // 2. Finding mean, standard deviation, creating a new document
        console.log("2. Creating statistics for the dataset.");
        const statistics = db.collection("statistika_weather_dataset");
        await createStatisticsContinuous(collection, statistics);

        // 3. Calculating frequencies of categorical variables
        console.log("3. Creating frequencies for the dataset.");
        const frequencies = db.collection("frekvencije_weather_dataset");
        await createFrequenciesCategorical(collection, frequencies);

        // 4. New documents with continuous values
        console.log("4. Creating documents with values lesser than and greater than means.");
        const statistics_1 = db.collection("statistika1_weather_dataset");
        const statistics_2 = db.collection("statistika2_weather_dataset");
        await createLessThanMeansContinuous(collection, statistics, statistics_1);
        await createGreaterThanMeansContinuous(collection, statistics, statistics_2);

        // 5. Embedding frequencies
        console.log("5. Embedding frequencies.");
        const embed = db.collection("emb_weather_dataset");
        await embedCategorical(collection, frequencies, embed);

        // 6. Embedding statistics
        console.log("6. Embedding statistics.");
        const embed_2 = db.collection("emb2_weather_dataset");
        await embedContinuous(collection, statistics, embed_2);

        // 7. Std, means
        console.log("7. Finding standard deviations 10% bigger than means.");
        await findStdBiggerThanMeans(embed_2);

    } finally {
        if (client) {
            await client.close();
            console.log("MongoDB connection closed.");
        }
    }
}

main().catch(console.error);
//dropDatabase(client);
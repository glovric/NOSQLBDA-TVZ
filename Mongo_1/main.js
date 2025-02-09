const { MongoClient } = require('mongodb');
const { url,
        getMissingValues, 
        updateMissingValues, 
        createStatisticsContinuous, 
        createFrequenciesCategorical, 
        createLessThanMeansContinuous, 
        createGreaterThanMeansContinuous,
        embedCategorical, 
        embedContinuous, 
        findStdBiggerThanMeans,
        createIndexAndQuery } = require('./utils');

const client = new MongoClient(url);

async function main() {

    try {

        // Connect to Mongo, database and collection
        await client.connect();
        console.log('Connected to MongoDB.');
        const db = client.db('NOSQL');
        const baseCollection = db.collection('water_dataset');

        // 1. Updating missing values
        console.log("1. Finding missing values.");
        const missing = await getMissingValues(baseCollection);

        if(missing.length > 0) {
            console.log("Updating missing values.")
            await updateMissingValues(baseCollection);
        }
        else {
            console.log("No missing values.");
        }

        // 2. Finding mean, standard deviation, creating a new document
        console.log("2. Creating statistics for the dataset.");
        const statistics = db.collection("statistika_water_dataset");
        await createStatisticsContinuous(baseCollection, statistics);

        // 3. Calculating frequencies of categorical variables
        console.log("3. Creating frequencies for the dataset.");
        const frequencies = db.collection("frekvencije_water_dataset");
        await createFrequenciesCategorical(baseCollection, frequencies);

        // 4. New documents with continuous values
        console.log("4. Creating documents with values lesser than and greater than means.");
        const statistics_1 = db.collection("statistika1_water_dataset");
        const statistics_2 = db.collection("statistika2_water_dataset");
        await createLessThanMeansContinuous(baseCollection, statistics, statistics_1);
        await createGreaterThanMeansContinuous(baseCollection, statistics, statistics_2);

        // 5. Embedding frequencies
        console.log("5. Embedding frequencies.");
        const embed = db.collection("emb_water_dataset");
        await embedCategorical(baseCollection, frequencies, embed);

        // 6. Embedding statistics
        console.log("6. Embedding statistics.");
        const embed_2 = db.collection("emb2_water_dataset");
        await embedContinuous(baseCollection, statistics, embed_2);

        // 7. Std, means
        console.log("7. Finding standard deviations 10% bigger than means.");
        await findStdBiggerThanMeans(embed_2);

        // 8. Index, query
        console.log("8. Creating compund index and querying.");
        const queryResult = await createIndexAndQuery(baseCollection);
        console.log(queryResult[0]);

    } finally {
        if (client) {
            await client.close();
            console.log("MongoDB connection closed.");
        }
    }
}

main().catch(console.error);
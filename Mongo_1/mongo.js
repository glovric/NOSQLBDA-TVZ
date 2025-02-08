const { MongoClient, ObjectId } = require('mongodb');

const url = 'mongodb://localhost:27017';

const client = new MongoClient(url);

const columnsContinuous = [
    "Specific conductance (Maximum)",
    "pH, standard units (Maximum)",
    "pH, standard units (Minimum)",
    "Specific conductance (Minimum)",
    "Specific conductance (Mean)",
    "Dissolved oxygen, milligrams per liter (Maximum)",
    "Dissolved oxygen, milligrams per liter (Mean)",
    "Dissolved oxygen, milligrams per liter (Minimum)",
    "Temperature, degrees Celsius (Mean)",
    "Temperature, degrees Celsius (Minimum)",
    "Temperature, degrees Celsius (Maximum)"
]

const columnsCategorical = ["married", "education"];

const missingContinuousCondition = columnsContinuous.map(c => ({
    "$or": [
        { [c]: { "$exists": false } },
        { [c]: { "$eq": null } },
        { [c]: { "$eq": NaN } }
    ]
}));

const missingCategoricalCondition = columnsCategorical.map(c => ({
    "$or": [
        { [c]: { "$exists": false } },
        { [c]: { "$eq": null } },
        { [c]: { "$eq": NaN } }
    ]
}));

const missingConditions = [
    ...missingContinuousCondition,
    ...missingCategoricalCondition
];

async function getMissingValues(collection) {
    const cursor = await collection.find({ "$or": missingConditions });
    return cursor.toArray();
}   

async function updateMissingValues(collection) {

    const cursor = await collection.find({ "$or": missingConditions });

    for await (const doc of cursor) {
        let updateObj = {};

        for (const c of columnsContinuous) {
            if (doc[c] === null || doc[c] === NaN || !doc.hasOwnProperty(c)) {
                updateObj[c] = -1;
            }
        }

        for (const c of columnsCategorical) {
            if (doc[c] === null || doc[c] === NaN || !doc.hasOwnProperty(c)) {
                updateObj[c] = "empty";
            }
        }

        if (Object.keys(updateObj).length > 0) {
            await collection.updateOne(
                { _id: doc._id },
                { $set: updateObj }
            );
        }
    }
}

async function createStatisticsContinuous(collection, collection_new) {

    const cursor = collection.find();
    const data = await cursor.toArray();
    const newDocument = {}
    
    for(let c of columnsContinuous) {
        let meanCurrent = 0;

        for(let d of data) {
            meanCurrent += d[c];
        }

        meanCurrent /= data.length;
        newDocument[c] = {};
        newDocument[c]["mean"] = meanCurrent;
    }

    for(let c of columnsContinuous) {
        let std_current = 0;

        for(let d of data) {
            std_current += (d[c] - newDocument[c]["mean"])**2;
        }

        std_current /= data.length;
        std_current = Math.sqrt(std_current);
        newDocument[c]["std"] = std_current;
    }

    for(let c of columnsContinuous) {
        newDocument[c]["nomissing"] = data.length;
    }

    await collection_new.insertOne(newDocument);
}

async function createFrequenciesCategorical(collection, collection_new) {

    const cursor = collection.find();
    const data = await cursor.toArray();
    const id = new ObjectId();

    for (let c of columnsCategorical) {

        for (let d of data) {

            const categoryValue = d[c];

            if (categoryValue !== null && categoryValue !== undefined) {

                await collection_new.updateOne(
                    { _id: id },
                    { 
                        $inc: { [`${c}.${categoryValue}`]: 1 } 
                    },
                    { upsert: true }
                );

            }
    }

}

}

async function createLessThanMeansContinuous(collection, collection_statistics, collection_statistics1) {

        const cursor = collection.find();
        const data = await cursor.toArray();

        const cursor_statistics = collection_statistics.find();
        const statistics_data = await cursor_statistics.toArray();
        const row_data = statistics_data[0];

        const statistics_1_object = {}

        for (let c of columnsContinuous) {
            statistics_1_object[c] = {"mean": row_data[c]["mean"], "values": []}
            for (let d of data) {
                if (d[c] <= row_data[c]["mean"]) {
                    statistics_1_object[c]["values"].push(d[c]);
                }
            }
        }

        await collection_statistics1.insertOne(statistics_1_object);
}

async function createGreaterThanMeansContinuous(collection, collection_statistics, collection_statistics2) {

    const cursor = collection.find();
    const data = await cursor.toArray();

    const cursor_statistics = collection_statistics.find();
    const statistics_data = await cursor_statistics.toArray();
    const row_data = statistics_data[0];

    const statistics_2_object = {}

    for (let c of columnsContinuous) {
        statistics_2_object[c] = {"mean": row_data[c]["mean"], "values": []}
        for (let d of data) {
            if (d[c] > row_data[c]["mean"]) {
                statistics_2_object[c]["values"].push(d[c]);
            }
        }
    }

    await collection_statistics2.insertOne(statistics_2_object);
}

async function embedCategorical(collection, collection_categorical, collection_new) {

    const all_data = await collection.find().toArray();
    const categorical_data = await collection_categorical.find().toArray();
    const data = categorical_data[0];

    for(let doc of all_data) { // rows

        doc["categories"] = {}; // init empty json

        for(let c of columnsCategorical) { // iterate columns
            doc["categories"][c] = data[c];
        }

        await collection_new.insertOne(doc);

    }
}

async function embedContinuous(collection, collection_statistics, collection_new) {

    const all_data = await collection.find().toArray();
    const statistics_data = await collection_statistics.find().toArray();
    const data = statistics_data[0];

    for(let doc of all_data) { // rows

        doc["statistics"] = {}; // init empty json

        for(let c of columnsContinuous) { // iterate columns
            doc["statistics"][c] = data[c];
        }

        await collection_new.insertOne(doc);

    }
}

async function main() {

    try {

        // Connect to Mongo, database and collection
        await client.connect();
        console.log('Connected to MongoDB.');
        const db = client.db('NOSQL');
        const collection = db.collection('weather_dataset');

        // 1. Updating missing values
        console.log("Finding missing values...");
        const missing = await getMissingValues(collection);

        if(missing.length > 0) {

            for(let doc of missing) {
                console.log(doc);
            }
            console.log("Updating missing values.")
            await updateMissingValues(collection);

        }
        else {
            console.log("No missing values.");
        }

        // 2. Finding mean, standard deviation, creating a new document
        console.log("Creating statistics for the dataset.");
        const statistics = db.collection("statistika_weather_dataset");
        //await createStatisticsContinuous(collection, statistics);

        // 3. Calculating frequencies of categorical variables
        console.log("Creating frequencies for the dataset.");
        const frequencies = db.collection("frekvencije_weather_dataset");
        //await createFrequenciesCategorical(collection, frequencies);

        // 4. New documents with continuous values
        console.log("Creating documents with values lesser than and greater than means.");
        const statistics_1 = db.collection("statistika1_weather_dataset");
        const statistics_2 = db.collection("statistika2_weather_dataset");
        //await createLessThanMeansContinuous(collection, statistics, statistics_1);
        //await createGreaterThanMeansContinuous(collection, statistics, statistics_2);

        // 5. Embedding frequencies
        const embed = db.collection("emb_weather_dataset");
        //await embedCategorical(collection, frequencies, embed);

        // 6. Embedding statistics
        const embed_2 = db.collection("emb2_weather_dataset");
        //await embedContinuous(collection, statistics, embed_2);


    } finally {
        if (client) {
            await client.close();
            console.log("MongoDB connection closed.");
        }
    }
}

main().catch(console.error);
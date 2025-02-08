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
    const newDocument = {"mean": {}, "std": {}, "nomissing": {}};
    
    for(let c of columnsContinuous) {
        let meanCurrent = 0;

        for(let d of data) {
            meanCurrent += d[c];
        }

        meanCurrent /= data.length;
        newDocument["mean"][c] = meanCurrent;
    }

    for(let c of columnsContinuous) {
        let std_current = 0;

        for(let d of data) {
            std_current += (d[c] - newDocument["mean"][c])**2;
        }

        std_current /= data.length;
        std_current = Math.sqrt(std_current);
        newDocument["std"][c] = std_current;
    }

    for(let c of columnsContinuous) {
        newDocument["nomissing"][c] = data.length;
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
        await createStatisticsContinuous(collection, statistics);

        // 3. Calculating frequencies of categorical variables
        console.log("Creating frequencies for the dataset.");
        const frequencies = db.collection("frekvencije_weather_dataset");
        await createFrequenciesCategorical(collection, frequencies);

    } finally {
        if (client) {
            await client.close();
            console.log("MongoDB connection closed.");
        }
    }
}

main().catch(console.error);
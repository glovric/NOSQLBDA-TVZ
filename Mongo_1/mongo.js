const { MongoClient } = require('mongodb');

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

const columnsCategorical = []

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

async function run() {

    try {

        await client.connect();
        console.log('Connected to MongoDB.');

        const db = client.db('NOSQL');
        const collection = db.collection('weather_dataset');

        // 1.
        console.log("Finding missing values...");

        const missing = await getMissingValues(collection);
        if(missing.length > 0) {
            for(let doc of missing) {
                console.log(doc);
            }
        }
        else {
            console.log("No missing values.");
        }

        await updateMissingValues(collection);



    } finally {
        // Close the MongoDB connection
        if (client) {
            await client.close();
            console.log("MongoDB connection closed.");
        }
    }
}

run().catch(console.error);
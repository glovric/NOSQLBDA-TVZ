const { ObjectId } = require('mongodb');
const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');

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
];

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

module.exports = {
    columnsContinuous,
    columnsCategorical,
    missingConditions,

    dropDatabase: async function (client, dbName) {
        try {
            await client.connect();
            const db = client.db(dbName);
            await db.dropDatabase();
            console.log(`Database ${dbName} dropped successfully`);
        } catch (error) {
            console.error('Error dropping database:', error);
        } finally {
            await client.close();
        }
    },

    loadDatabase: async function (client, dbName, collectionName, collectionPath) {
        try {       
            const db = client.db(dbName);
            const collection = db.collection(collectionName);
        
            const results = [];
        
            fs.createReadStream(collectionPath) // Path to your CSV file
            .pipe(csv()) // Pipe CSV through the parser
            .on('data', (data) => {
                // Convert numeric values from strings to floats (or integers)
                for (const key in data) {
                    if (data.hasOwnProperty(key)) {
                        // Check if the value is a valid float
                        const numericValue = parseFloat(data[key]);
                        if (!isNaN(numericValue)) {
                            data[key] = numericValue;  // Convert to float if valid
                        }
                    }
                }
                results.push(data); // Push each row into the results array
            })
            .on('end', async () => {
                try {
                // Insert parsed data into MongoDB
                const insertResult = await collection.insertMany(results);
                console.log(`${insertResult.insertedCount} records inserted into ${collectionName}.`);
                } catch (err) {
                console.error('Error inserting data into MongoDB:', err);
                } finally {
                client.close(); // Close the MongoDB connection
                }
            });
            } catch (err) {
                console.error('Error connecting to MongoDB:', err);
            }
    },

    getMissingValues: async function (collection) {
        const cursor = await collection.find({ "$or": missingConditions });
        return cursor.toArray();
    },

    updateMissingValues: async function (collection) {
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
    },

    createStatisticsContinuous: async function (collection, collection_new) {
        const cursor = collection.find();
        const data = await cursor.toArray();
        const newDocument = {};

        for (let c of columnsContinuous) {
            let meanCurrent = 0;

            for (let d of data) {
                meanCurrent += d[c];
            }

            meanCurrent /= data.length;
            newDocument[c] = {};
            newDocument[c]["mean"] = meanCurrent;
        }

        for (let c of columnsContinuous) {
            let std_current = 0;

            for (let d of data) {
                std_current += (d[c] - newDocument[c]["mean"]) ** 2;
            }

            std_current /= data.length;
            std_current = Math.sqrt(std_current);
            newDocument[c]["std"] = std_current;
        }

        for (let c of columnsContinuous) {
            newDocument[c]["nomissing"] = data.length;
        }

        await collection_new.insertOne(newDocument);
    },

    createFrequenciesCategorical: async function (collection, collection_new) {
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
    },

    createLessThanMeansContinuous: async function (collection, collection_statistics, collection_statistics1) {
        const cursor = collection.find();
        const data = await cursor.toArray();

        const cursor_statistics = collection_statistics.find();
        const statistics_data = await cursor_statistics.toArray();
        const row_data = statistics_data[0];

        const statistics_1_object = {};

        for (let c of columnsContinuous) {
            statistics_1_object[c] = { "mean": row_data[c]["mean"], "values": [] };
            for (let d of data) {
                if (d[c] <= row_data[c]["mean"]) {
                    statistics_1_object[c]["values"].push(d[c]);
                }
            }
        }

        await collection_statistics1.insertOne(statistics_1_object);
    },

    createGreaterThanMeansContinuous: async function (collection, collection_statistics, collection_statistics2) {
        const cursor = collection.find();
        const data = await cursor.toArray();

        const cursor_statistics = collection_statistics.find();
        const statistics_data = await cursor_statistics.toArray();
        const row_data = statistics_data[0];

        const statistics_2_object = {};

        for (let c of columnsContinuous) {
            statistics_2_object[c] = { "mean": row_data[c]["mean"], "values": [] };
            for (let d of data) {
                if (d[c] > row_data[c]["mean"]) {
                    statistics_2_object[c]["values"].push(d[c]);
                }
            }
        }

        await collection_statistics2.insertOne(statistics_2_object);
    },

    embedCategorical: async function (collection, collection_categorical, collection_new) {
        const all_data = await collection.find().toArray();
        const categorical_data = await collection_categorical.find().toArray();
        const data = categorical_data[0];

        for (let doc of all_data) {
            doc["categories"] = {};

            for (let c of columnsCategorical) {
                doc["categories"][c] = data[c];
            }

            await collection_new.insertOne(doc);
        }
    },

    embedContinuous: async function (collection, collection_statistics, collection_new) {
        const all_data = await collection.find().toArray();
        const statistics_data = await collection_statistics.find().toArray();
        const data = statistics_data[0];

        for (let doc of all_data) {
            doc["statistics"] = {};

            for (let c of columnsContinuous) {
                doc["statistics"][c] = data[c];
            }

            await collection_new.insertOne(doc);
        }
    },

    findStdBiggerThanMeans: async function (collection_emb2) {
        const embed_2_data = await collection_emb2.find().toArray();
        const embed_2_statistics = embed_2_data[0]["statistics"];

        for (let c of columnsContinuous) {
            let data = embed_2_statistics[c];
            let mean = data["mean"];
            let std = data["std"];

            if (std === 0) {
                continue;
            }
            if (std > mean * 0.10) {
                data["std&mean10%"] = mean;
                embed_2_statistics[c] = data;
            }
        }

        for (let doc of embed_2_data) {
            await collection_emb2.updateOne(
                { "_id": doc["_id"] },
                { $set: { "statistics": embed_2_statistics } },
                { upsert: true }
            );
        }
    }
};

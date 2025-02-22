const { ObjectId } = require('mongodb');
const fs = require('fs');
const csv = require('csv-parser');

const url = 'mongodb://localhost:27017';

const columnsContinuous = [
    "Specific conductance (Maximum)",
    "pH, standard units (Maximum)",
    "pH, standard units (Minimum)",
    "Specific conductance (Minimum)",
    "Specific conductance (Mean)",
    "Dissolved oxygen (Maximum)",
    "Dissolved oxygen (Mean)",
    "Dissolved oxygen (Minimum)",
    "Temperature (Mean)",
    "Temperature (Minimum)",
    "Temperature (Maximum)",
    "Target"
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

    url,

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

    dropCollections: async function (client, dbName, collections) {
        try {
            await client.connect();
            const db = client.db(dbName);
            for(let c of collections) {
                const collection = db.collection(c);
                const result = await collection.drop();
                console.log(`Collection ${c} dropped successfully:`, result);
            }
          } catch (err) {
            console.error('Error dropping collection:', err);
          } finally {
            await client.close();
          }  
    },

    loadDatabase: async function (client, dbName, collectionName, collectionPath) {
        try {       
            const db = client.db(dbName);
            const collection = db.collection(collectionName);
        
            const results = [];
        
            fs.createReadStream(collectionPath)
            .pipe(csv())
            .on('data', (data) => {
                for (const key in data) {
                    if (data.hasOwnProperty(key)) {
                        const numericValue = parseFloat(data[key]);
                        if (!isNaN(numericValue)) {
                            data[key] = numericValue;
                        }
                    }
                }
                results.push(data);
            })
            .on('end', async () => {
                try {
                const insertResult = await collection.insertMany(results);
                console.log(`${insertResult.insertedCount} records inserted into ${collectionName}.`);
                } catch (err) {
                console.error('Error inserting data into MongoDB:', err);
                } finally {
                client.close();
                }
            });
            } catch (err) {
                console.error('Error connecting to MongoDB:', err);
            }
    },

    // 1.
    getMissingValues: async function (collection) {
        const cursor = await collection.find({ "$or": missingConditions });
        return cursor.toArray();
    },

    // 1.
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

    // 2.
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
            let counterNoMissing = 0;

            for(let d of data) {

                if (d[c] !== null && d[c] !== NaN && d.hasOwnProperty(c)) {
                    counterNoMissing += 1;
                }

                newDocument[c]["nomissing"] = counterNoMissing;
            }
        }   

        await collection_new.insertOne(newDocument);
    },

    // 3.
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

    // 4.
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

    // 4.
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

    // 5.
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

    // 6.
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

    // 7.
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
    },

    // 8.
    createIndexAndQuery: async function (collection) {
        await collection.createIndex({
            "Specific conductance (Maximum)": 1,
            "pH, standard units (Maximum)": -1,
            "Dissolved oxygen (Mean)": 1
        })

        const result = await collection.find({
            "Specific conductance (Maximum)": { $gt: 0.5 },
            "pH, standard units (Maximum)": { $lt: 1 },
            "Dissolved oxygen (Mean)": { $gt: 0.01 }
        }).toArray();

        return result;
    }
};

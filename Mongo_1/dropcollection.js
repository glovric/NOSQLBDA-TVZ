const { MongoClient } = require('mongodb');
const { url, dropCollections } = require('./utils');

const client = new MongoClient(url);
const collectionsToDrop = [
    "statistika_water_dataset",
    "statistika1_water_dataset",
    "statistika2_water_dataset",
    "frekvencije_water_dataset",
    "emb_water_dataset",
    "emb2_water_dataset",
]

dropCollections(client, "NOSQL", collectionsToDrop);
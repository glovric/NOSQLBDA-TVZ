const { MongoClient } = require('mongodb');
const { url, loadDatabase } = require('./utils');

const client = new MongoClient(url);
loadDatabase(client, "NOSQL", "water_dataset", "../data/water_dataset_reshaped.csv");
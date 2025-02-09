const { MongoClient } = require('mongodb');
const { loadDatabase } = require('./utils');

const url = 'mongodb://localhost:27017';
const client = new MongoClient(url);

loadDatabase(client, "NOSQL", "water_dataset", "../data/water_dataset_reshaped.csv");
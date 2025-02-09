const { MongoClient } = require('mongodb');
const { dropDatabase } = require('./utils');

const url = 'mongodb://localhost:27017';
const client = new MongoClient(url);

dropDatabase(client, "NOSQL");
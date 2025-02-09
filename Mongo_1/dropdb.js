const { MongoClient } = require('mongodb');
const { url, dropDatabase } = require('./utils');

const client = new MongoClient(url);
dropDatabase(client, "NOSQL");
const { MongoClient } = require('mongodb');
const fs = require('fs');

const uri = 'mongodb://localhost:27017';
const dbName = 'NOSQL';

async function exportDatabase() {
  const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

  try {

    await client.connect();
    console.log('Connected to MongoDB');

    const db = client.db(dbName);
    const collections = await db.listCollections().toArray();

    for (let collection of collections) {
      const collectionName = collection.name;
      console.log(`Exporting collection: ${collectionName}`);

      const documents = await db.collection(collectionName).find().toArray();

      fs.writeFileSync(`result_data/${collectionName}.json`, JSON.stringify(documents, null, 2));

      console.log(`Collection ${collectionName} exported to ${collectionName}.json`);
    }
  } catch (err) {
    console.error('Error exporting database:', err);
  } finally {
    await client.close();
  }
}

exportDatabase();

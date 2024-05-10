import axios from 'axios';
import {config} from '../environment.config';
import {Database} from 'arangojs';
import * as https from "https";

function buildArangoDBUrl(path: string) {
  if (path[0] !== '/') {
    throw new Error('Path must start with a "/" character.');
  }
  return `${config.arangodb.endpoint}${path}`;
}

async function getArangoJWT(maxRetries: number = 1) {
  let retries = 0;

  while (retries < maxRetries) {
    try {
      const response = await axios.post(buildArangoDBUrl('/_open/auth'), {
        username: config.arangodb.username,
        password: config.arangodb.password
      }, {
        httpsAgent: new https.Agent({
          rejectUnauthorized: false
        }),
        auth: {
          username: config.arangodb.username,
          password: config.arangodb.password,
        },
      });
      return response.data['jwt'];
    } catch (error) {
      retries++;
      console.log('ArangoDB not ready, retrying...');
      await new Promise(resolve => setTimeout(resolve, 1000));
      if (retries === maxRetries) {
        throw new Error(`Failed to get JWT after ${maxRetries} attempts.`);
      }
    }
  }
}

function getArangoJSDatabaseInstance() {
  return new Database({
    url: config.arangodb.endpoint,
    databaseName: config.arangodb.database,
    auth: {
      username: config.arangodb.username,
      password: config.arangodb.password
    }
  });
}

async function executeQuery(query: string, bindParams: unknown = {}) {
  const db = getArangoJSDatabaseInstance();
  return await db.query(query, bindParams);
}

async function createDocumentCollection(collectionName: string, tryDrop: boolean = true) {
  const db = getArangoJSDatabaseInstance();

  if (tryDrop) {
    try {
      await db.collection(collectionName).drop();
    } catch (ignore) {
      // Do nothing
    }
  }
  await db.collection(collectionName).create();
  return db.collection(collectionName);
}

export const arangodb = {
  getArangoJWT, createDocumentCollection, getArangoJSDatabaseInstance, executeQuery
};


module.exports = arangodb;

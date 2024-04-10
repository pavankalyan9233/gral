import {Database, aql} from 'arangojs';
import fs from 'fs';
import path from 'path';
import {promisify} from 'util';
import {exec as execSync} from 'child_process';

const exec = promisify(execSync);
import axios from 'axios';
import readline from 'readline';
import PQueue from "p-queue";
import * as https from "https";
import * as environment from "../config/environment.js";

const CONCURRENCY = environment.config.import_configuration.concurrency;
const MAX_QUEUE_SIZE = environment.config.import_configuration.max_queue_size;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export class GraphImporter {
  constructor(arangoConfig, graphName, dropGraph = false) {
    this.arangoEndpoint = arangoConfig.endpoint;
    this.arangoUser = arangoConfig.username;
    this.arangoPassword = arangoConfig.password;
    this.databaseName = arangoConfig.databaseName;
    this.graphName = graphName;
    this.dropGraph = dropGraph;

    let agentOptions = {};
    if (arangoConfig.ca) {
      // This is specifically here to support ArangoGraph connections
      agentOptions.ca = Buffer.from(arangoConfig.ca, "base64");
    }

    this.db = new Database({
      databaseName: this.databaseName,
      url: this.arangoEndpoint,
      auth: {username: this.arangoUser, password: this.arangoPassword},
      agentOptions: agentOptions
    });
    this.db.useBasicAuth(this.arangoUser, this.arangoPassword);
  }

  localDataExists() {
    const dataPath = path.join(__dirname, '..', 'data', this.graphName);
    return fs.existsSync(dataPath);
  }

  async countLinesUsingWc(filePath) {
    let lineCount;
    try {
      const {stdout} = await exec(`cat ${filePath} | wc -l`);
      lineCount = parseInt(stdout);
    } catch (error) {
      console.error('Error executing wc -l:', error.message);
    }

    // lineCount is a string, cast it to a number
    lineCount = parseInt(lineCount);

    // return as numeric, if not numeric throw error
    if (isNaN(lineCount)) {
      throw new Error('Error counting lines');
    }

    return lineCount;
  }

  insertManyDocumentsIntoCollection = async (db, coll, maker, limit, batchSize) => {
    // This function uses the asynchronous API of `arangod` to quickly
    // insert a lot of documents into a collection. You can control which
    // documents to insert with the `maker` function. The arguments are:
    //  db - name of the database (string)
    //  coll - name of the collection, must already exist (string)
    //  maker - a callback function to produce documents, it is called
    //          with a single integer, the first time with 0, then with 1
    //          and so on. The callback function should return an object
    //          or a list of objects, which will be inserted into the
    //          collection. You can either specify the `_key` attribute or
    //          not. Once you return either `null` or `false`, no more
    //          callbacks will be done.
    //  limit - an integer, if `limit` documents have been received, no more
    //          callbacks are issued.
    //  batchSize - an integer, this function will use this number as batch
    //              size.
    // Example:
    //   insertManyDocumentsIntoCollection("_system", "coll",
    //       i => {Hallo:i}, 1000000, 1000);
    // will insert 1000000 documents into the collection "coll" in the
    // `_system` database in batches of 1000. The documents will all have
    // the `Hallo` attribute set to one of the numbers from 0 to 999999.
    // This is useful to quickly generate test data. Be careful, this can
    // create a lot of parallel load!
    let done = false;
    let l = [];
    let jobs = [];
    let counter = 1;
    let documentCount = 0;
    while (true) {
      if (!done) {
        while (l.length < batchSize) {
          let d;
          if (Array.isArray(maker)) {
            d = maker;
          } else {
            d = maker(counter);
          }
          if (d === null || d === false) {
            done = true;
            break;
          }
          if (Array.isArray(d)) {
            l = l.concat(d);
            documentCount += d.length;
          } else if (typeof (d) === "object") {
            l.push(d);
            documentCount += 1;
          }
          counter += 1;
          if (documentCount >= limit || Array.isArray(maker)) {
            done = true;
          }
        }
      }

      if ((done && l.length > 0) || l.length >= batchSize) {
        let response = await axios.post(
          `${this.arangoEndpoint}/_db/${encodeURIComponent(db)}/_api/document/${encodeURIComponent(coll)}`,
          JSON.stringify(l),
          {
            httpsAgent: new https.Agent({
              rejectUnauthorized: false
            }),
            headers: {
              "x-arango-async": "store"
            },
            auth: {
              username: this.arangoUser,
              password: this.arangoPassword,
            },
          });
        l = [];
        jobs.push(response.headers["x-arango-async-id"]);
      }

      let i = 0;
      while (i < jobs.length) {
        try {
          let r = await axios.put(`${this.arangoEndpoint}/_db/${encodeURIComponent(db)}/_api/job/${jobs[i]}`, null, {
            httpsAgent: new https.Agent({
              rejectUnauthorized: false
            }),
            auth: {
              username: this.arangoUser,
              password: this.arangoPassword,
            },
          });

          if (r.status === 204) {
            i += 1;
          } else if (r.status === 202) {
            if (r.data[0].error) {
              console.error(`Job ${jobs[i]} failed: ${r.data[0].errorMessage}`);
            }
            jobs = jobs.slice(0, i).concat(jobs.slice(i + 1));
          }
        } catch (error) {
          if (error.response && error.response.status === 404) {
            // Ignore and continue to the next iteration
            continue;
          } else {
            // If the error is not a 404 error, rethrow it
            console.log(`Error: ${error.message}`);
            throw error;
          }
        }
      }

      if (done) {
        if (jobs.length === 0) {
          break;
        }
        await sleep(500);
      }
    }
  };

  async processEdgeFile(filePath, batchSize = 10000) {
    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity, // Recognize all line breaks
    });

    let counter = 0;
    let docs = [];
    const queue = new PQueue({concurrency: CONCURRENCY});

    queue.on('active', () => {
      console.log(`Working on edges. Queue Size: ${queue.size} - Still Pending: ${queue.pending}`);
    });

    for await (const line of rl) {
      // Assuming each line contains two numeric values separated by a space
      const [fromSource, toSource] = line.split(' ').map(Number);
      docs.push({
        _from: `${this.graphName}_v/${fromSource}`,
        _to: `${this.graphName}_v/${toSource}`,
      });

      if (docs.length >= batchSize) {

        while (true) {
          if (queue.size < MAX_QUEUE_SIZE) {
            break;
          } else {
            console.log("=> Queue rate limiting. Reached 1k elements. Sleeping 5 seconds.")
            await sleep(5000);
          }
        }

        queue.add(() => this.insertManyDocumentsIntoCollection(this.databaseName, this.graphName + '_e',
          docs, docs.length, batchSize));
        docs = [];
      }
    }

    // wait for all futures
    await queue.onIdle();
    console.log('12. All work is done');
    console.log(`-> Inserted ${counter * batchSize} edges into collection ${this.graphName}_e`);
  }

  async insertEdges() {
    console.log(`Will now insert edges into collection ${this.graphName}_e. This will take a while...`)
    const filePath = new URL(`../data/${this.graphName}/${this.graphName}.e`, import.meta.url).pathname;
    await this.processEdgeFile(filePath, 10000);
  }

  async insertVertices() {
    const filePath = new URL(`../data/${this.graphName}/${this.graphName}.v`, import.meta.url).pathname;
    const lineCount = await this.countLinesUsingWc(filePath);

    console.log(`Will now insert vertices into collection ${this.graphName}_v. This will take a while...`)
    await this.insertManyDocumentsIntoCollection(this.databaseName, this.graphName + '_v',
      function (i) {
        return {_key: JSON.stringify(i)};
      },
      lineCount, 10000);
    console.log(`-> Inserted ${lineCount} vertices into collection ${this.graphName}_v`);
  }

  async createGraph(edgeDefinitions, options) {
    const graph = this.db.graph(this.graphName);
    let exists = await graph.exists();

    if (this.dropGraph && exists) {
      await graph.drop(true);
      console.log(`Graph ${this.graphName} dropped`);
      exists = false;
    }

    if (!exists) {
      const edgeCollectionName = `${this.graphName}_e`;
      const vertexCollectionName = `${this.graphName}_v`;
      const edgeDefinitions = [{
        collection: edgeCollectionName,
        from: [vertexCollectionName],
        to: [vertexCollectionName],
      }];

      await graph.create(edgeDefinitions, {});

      console.log(`Graph ${this.graphName} created with edge collection ${edgeCollectionName} and vertex collection ${vertexCollectionName}`);
    } else {
      throw new Error(`Graph ${this.graphName} already exists`);
    }
  }
}
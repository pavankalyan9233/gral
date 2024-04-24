import {Database} from 'arangojs';
import fs from 'fs';
import path from 'path';
import {promisify} from 'util';
import {exec as execSync} from 'child_process';

const exec = promisify(execSync);
import axios from 'axios';
import readline from 'readline';
import PQueue from "p-queue";
import * as https from "https";

// Parameter for the queue update log messages
let printMessages = true; // Flag to control printing messages
const intervalTime = 2000; // 5 seconds in milliseconds

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export class GraphImporter {
  constructor(arangoConfig, graphName, dropGraph = false, importOptions) {
    this.arangoEndpoint = arangoConfig.endpoint;
    this.arangoUser = arangoConfig.username;
    this.arangoPassword = arangoConfig.password;
    this.databaseName = arangoConfig.databaseName;
    this.graphName = graphName;
    this.dropGraph = dropGraph;
    this.concurrency = importOptions.concurrency;
    this.max_queue_size = importOptions.maxQueueSize;
    this.expectedAmountOfVertices = null;
    this.expectedAmountOfEdges = null;

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

  async checkIfFirstVertexIdIsZero(filePath) {
    const file = fs.readFileSync(filePath, 'utf-8');
    const lines = file.split('\n');
    const firstLine = lines[0];
    const firstVertexId = parseInt(firstLine.split(' ')[0]);
    return firstVertexId === 0;
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

  insertManyDocumentsIntoCollection = async (db, coll, maker, limit, batchSize, vertexInsert = true, startAtZero = false) => {
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
    if (startAtZero) {
      // Some datasets we want to import start with 0 vertex IDs, some with 1.
      // This flat allows us to start at 0 if needed. This depends on the dataset.
      // The conventions are not consistent, we need to handle this here.
      counter = 0;
    }

    let documentCount = 0;

    const {expectedAmountOfVertices, _} = await this.getVertexAndEdgeCountsToInsert();

    let docsToBeInserted;
    if (vertexInsert) {
      docsToBeInserted = expectedAmountOfVertices;
    } else {
      // We are inserting edges here and passing an array instead of a maker method
      docsToBeInserted = maker.length;
    }

    while (true) {
      if (!done) {
        while (l.length < batchSize && documentCount < docsToBeInserted) {
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

    let docs = [];
    const queue = new PQueue({concurrency: this.concurrency});

    queue.on('active', () => {
      if (printMessages) {
        console.log(`Working on edges. Queue Size: ${queue.size} - Still Pending: ${queue.pending}`);
        printMessages = false; // Set flag to false to prevent immediate consecutive prints
        setTimeout(() => {
          printMessages = true; // Set flag to true after 5 seconds
        }, intervalTime);
      }
    });

    for await (const line of rl) {
      // Assuming each line contains two numeric values separated by a space
      const [fromSource, toSource] = line.split(' ').map(Number);
      docs.push({
        _from: `${this.graphName}_v/${fromSource}`,
        _to: `${this.graphName}_v/${toSource}`,
      });

      if (docs.length === batchSize) {
        while (true) {
          if (queue.size < this.max_queue_size) {
            break;
          } else {
            console.log(`=> Queue rate limiting. Reached ${this.max_queue_size} elements. Sleeping 5 seconds.`)
            await sleep(5000);
          }
        }

        const copyDocs = [...docs];
        queue.add(() => this.insertManyDocumentsIntoCollection(this.databaseName, this.graphName + '_e',
          copyDocs, copyDocs.length, batchSize, false));
        docs = [];
      }
    }

    if (docs.length > 0) {
      // last batch might still contain documents
      const copyDocs = [...docs];
      queue.add(() => this.insertManyDocumentsIntoCollection(this.databaseName, this.graphName + '_e',
        copyDocs, copyDocs.length, batchSize, false));
    }

    // wait for all futures
    await queue.onIdle();
    console.log(`-> Done inserting edges into collection ${this.graphName}_e`);
  }

  async insertEdges() {
    console.log(`Will now insert edges into collection ${this.graphName}_e. This will take a while...`)
    const filePath = new URL(`../data/${this.graphName}/${this.graphName}.e`, import.meta.url).pathname;
    await this.processEdgeFile(filePath, 10000);
  }

  async insertVertices() {
    const filePath = new URL(`../data/${this.graphName}/${this.graphName}.v`, import.meta.url).pathname;
    const lineCount = await this.countLinesUsingWc(filePath);
    const startsWithZero = await this.checkIfFirstVertexIdIsZero(filePath);
    console.log(`Will now insert vertices into collection ${this.graphName}_v. This will take a while...`)

    await this.insertManyDocumentsIntoCollection(this.databaseName, this.graphName + '_v',
      function (i) {
        return {_key: JSON.stringify(i)};
      },
      lineCount, 10000, true, startsWithZero);
  }

  async createGraphWithVerticesAndEdges(vertices, edges) {
    await this.insertVerticesArray(vertices);
    await this.insertEdgesArray(edges);
  }

  async insertVerticesArray(vList) {
    const vertexCollection = this.db.collection(this.getVertexCollectionName());
    await vertexCollection.saveAll(vList);
  }

  async insertEdgesArray(eList) {
    const edgeCollection = this.db.collection(this.getEdgeCollectionName());
    await edgeCollection.saveAll(eList);
  }

  getVertexCollectionName() {
    return `${this.graphName}_v`;
  }

  getEdgeCollectionName() {
    return `${this.graphName}_e`;
  }

  async createGraph() {
    const graph = this.db.graph(this.graphName);
    let exists = await graph.exists();

    if (this.dropGraph && exists) {
      await graph.drop(true);
      console.log(`Graph ${this.graphName} dropped`);
      exists = false;
    }

    if (!exists) {
      const edgeCollectionName = this.getEdgeCollectionName();
      const vertexCollectionName = this.getVertexCollectionName();
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

  readGraphProperties() {
    const filePath = new URL(`../data/${this.graphName}/${this.graphName}.properties`, import.meta.url).pathname;
    const content = fs.readFileSync(filePath, 'utf-8');
    const lines = content.split('\n').filter(line => line.trim() !== '' && !line.startsWith('#'));

    const result = {};
    let currentSection = '';

    lines.forEach(line => {
      if (line.startsWith('graph.')) {
        const section = line.substring(6, line.indexOf('.', 6));
        const keyValuePair = line.substring(line.indexOf('.', 6) + 1).split('=').map(item => item.trim());

        if (!result[section]) {
          result[section] = {};
        }

        result[section][keyValuePair[0]] = keyValuePair[1];
        currentSection = section;
      } else {
        const keyValuePair = line.split('=').map(item => item.trim());
        result[currentSection][keyValuePair[0]] = keyValuePair[1];
      }
    });

    return result;
  }

  async getVertexAndEdgeCountsToInsert() {

    if (this.expectedAmountOfVertices && this.expectedAmountOfEdges) {
      // returning cached value in case that method has been called already
      return {
        expectedAmountOfVertices: this.expectedAmountOfVertices,
        expectedAmountOfEdges: this.expectedAmountOfEdges
      };
    }

    const graphProperties = this.readGraphProperties();
    const expectedAmountOfVertices = graphProperties[`${this.graphName}`]['meta.vertices'];
    const expectedAmountOfEdges = graphProperties[`${this.graphName}`]['meta.edges'];

    this.expectedAmountOfVertices = expectedAmountOfVertices;
    this.expectedAmountOfEdges = expectedAmountOfEdges;

    const graph = this.db.graph(this.graphName);
    let exists = await graph.exists();
    if (!exists) {
      throw new Error(`Graph ${this.graphName} does not exist`);
    }
    return {expectedAmountOfVertices, expectedAmountOfEdges};
  }

  async verifyGraph() {
    const {expectedAmountOfVertices, expectedAmountOfEdges} = await this.getVertexAndEdgeCountsToInsert();
    const vertexCollection = this.db.collection(`${this.graphName}_v`);
    const edgeCollection = this.db.collection(`${this.graphName}_e`);
    const vProperties = await vertexCollection.count();
    const eProperties = await edgeCollection.count();
    const vCount = vProperties.count;
    const eCount = eProperties.count;

    if (vCount !== parseInt(expectedAmountOfVertices)) {
      throw new Error(`Expected amount of vertices (${expectedAmountOfVertices}) does not match actual amount of vertices (${vCount})`);
    }
    if (eCount !== parseInt(expectedAmountOfEdges)) {
      throw new Error(`Expected amount of edges (${expectedAmountOfEdges}) does not match actual amount of edges (${eCount})`);
    }
    console.log(`Graph ${this.graphName} verified. Expected amount of vertices: ${expectedAmountOfVertices}, actual amount of vertices: ${vCount}, expected amount of edges: ${expectedAmountOfEdges}, actual amount of edges: ${eCount}`)
  }
}
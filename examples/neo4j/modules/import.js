import * as util from "node:util";
import neo4j from 'neo4j-driver';
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

import {fileURLToPath} from 'url';

const __filename = fileURLToPath(import.meta.url);

import {dirname} from 'path';

const __dirname = dirname(__filename);

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const log_file = fs.createWriteStream(__dirname + '/debug.log', {flags: 'w'});
const writeToFile = function (d) { //
  log_file.write(util.format(d) + '\n');
};

let customIdToNodeIdMap = {};

export class GraphImporter {
  constructor(neo4jConfig, graphName, dropGraph = false, importOptions) {
    this.neo4jEndpoint = neo4jConfig.endpoint;
    this.neo4jUser = neo4jConfig.username;
    this.neo4jPassword = neo4jConfig.password;
    this.databaseName = neo4jConfig.databaseName;
    this.graphName = graphName;
    this.dropGraph = dropGraph;
    this.concurrency = importOptions.concurrency;
    this.max_queue_size = importOptions.maxQueueSize;
    this.expectedAmountOfVertices = null;
    this.expectedAmountOfEdges = null;

    this.driver = neo4j.driver(this.neo4jEndpoint, neo4j.auth.basic(this.neo4jUser, this.neo4jPassword), {database: this.databaseName});
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

  insertManyDocumentsIntoCollection = async (db, coll, maker, limit, batchSize, generatorInsertion = true, startAtZero = false, isNodeInsertion = true) => {
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
    if (generatorInsertion) {
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

      const session = this.driver.session();
      if ((done && l.length > 0) || l.length >= batchSize) {


        try {
          if (isNodeInsertion) {
            jobs.push(session.writeTransaction(async tx => {
              for (const nodeData of l) {
                const keys = Object.keys(nodeData.properties);
                const beginString = '{';
                const endString = '}';
                let propertiesString = '';
                for (let i = 0; i < keys.length; i++) {
                  const key = keys[i];
                  if (nodeData.properties[key] === undefined) {
                    continue;
                  }
                  propertiesString += key + ': $' + key;
                  if (i < keys.length - 1) {
                    propertiesString += ', ';
                  }
                }
                const fullPropertiesString = beginString + propertiesString + endString;
                const label = nodeData.label;
                const propertiesObject = {label, ...nodeData.properties};
                let cypherQuery = `CREATE (n:\`${label}\` ${fullPropertiesString}) RETURN id(n) AS nodeId`;

                //writeToFile('fullPropertiesString:', fullPropertiesString);
                //writeToFile("query");
                //writeToFile(cypherQuery);
                //writeToFile('propertiesObject:');
                //writeToFile(propertiesObject);

                await tx.run(cypherQuery, propertiesObject).then(result => {
                  const nodeId = result.records[0].get('nodeId').low;
                  customIdToNodeIdMap[nodeData.properties.customId] = nodeId;
                });
              }
              l = [];
            }));
          } else {
            const edgeLabel = this.getEdgeLabel();
            jobs.push(session.writeTransaction(async tx => {
              let formattedEdgeData = [];
              for (const edgeData of l) {
                const fromId = parseInt(edgeData._from.split('/')[1]);
                const toId = parseInt(edgeData._to.split('/')[1]);
                const fromCustomId = customIdToNodeIdMap[fromId];
                const toCustomId = customIdToNodeIdMap[toId];

                formattedEdgeData.push({fromCustomId, toCustomId});
              }

              const label = this.getVertexLabel();
              const cypherQuery = `
                  UNWIND $relationships AS relData
                  MATCH (a), (b)
                  WHERE id(a) = relData.fromCustomId AND id(b) = relData.toCustomId
                  CREATE (a)-[:\`${edgeLabel}\`]->(b)
                  // UNWIND $relationships AS relData
                  // MATCH (a:\`${label}\` {customId: relData.fromCustomId}), (b:\`${label}\` {customId: relData.toCustomId})
                  // CREATE (a)-[:\`${edgeLabel}\`]->(b)
                `
              ;
              //writeToFile("query");
              //writeToFile(cypherQuery);
              //writeToFile(JSON.stringify(formattedEdgeData));

              await tx.run(cypherQuery, {relationships: formattedEdgeData}).then(result => {
                //console.log(JSON.stringify(result))
                //console.log(result.summary.counters)
              });

              l = [];
            }));
          }
        } catch (e) {
          console.log(e)
        }
      }

      await Promise.all(jobs).then(() => {
        done = true;
      });

      if (done) {
        break;
      }
    }
  };

  async processVertexFile(filePath, batchSize = 10000) {
    // TODO Minor: At some point we can merge this with edge insert. Also we can now reduce complexity
    //  of insertManyDocumentsIntoCollection. But this is not important and a priority right now.
    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity, // Recognize all line breaks
    });

    let docs = [];
    const queue = new PQueue({concurrency: this.concurrency});

    queue.on('active', () => {
      if (printMessages) {
        console.log(`Working on vertices. Queue Size: ${queue.size} - Still Pending: ${queue.pending}`);
        printMessages = false; // Set flag to false to prevent immediate consecutive prints
        setTimeout(() => {
          printMessages = true; // Set flag to true after 5 seconds
        }, intervalTime);
      }
    });

    for await (const line of rl) {
      // Assuming each line contains two numeric values separated by a space
      const properties = {customId: parseInt(line)};
      const label = this.getVertexLabel();
      docs.push({label, properties});

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
        queue.add(() => this.insertManyDocumentsIntoCollection(this.databaseName, this.graphName + '_v',
          copyDocs, copyDocs.length, batchSize, false, undefined, true));
        docs = [];
      }
    }

    if (docs.length > 0) {
      // last batch might still contain documents
      const copyDocs = [...docs];
      queue.add(() => this.insertManyDocumentsIntoCollection(this.databaseName, this.graphName + '_v',
        copyDocs, copyDocs.length, batchSize, false, undefined, true));
    }

    // wait for all futures
    await queue.onIdle();

    console.log(`-> Done inserting vertices into collection ${this.graphName}_v`);
  }

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
          copyDocs, copyDocs.length, batchSize, false, undefined, false));
        docs = [];
      }
    }

    if (docs.length > 0) {
      // last batch might still contain documents
      const copyDocs = [...docs];
      queue.add(() => this.insertManyDocumentsIntoCollection(this.databaseName, this.graphName + '_e',
        copyDocs, copyDocs.length, batchSize, false, undefined, false));
    }

    // wait for all futures
    await queue.onIdle();
    console.log(`-> Done inserting edges into collection ${this.graphName}_e`);
  }

  async insertEdges() {
    console.log(`Will now insert edges into collection ${this.graphName}_e. This will take a while...`)
    const filePath = new URL(`../../data/${this.graphName}/${this.graphName}.e`, import.meta.url).pathname;
    await this.processEdgeFile(filePath, 10000);
  }

  async insertVertices() {
    console.log(`Will now insert edges into collection ${this.graphName}_v. This will take a while...`)
    const filePath = new URL(`../../data/${this.graphName}/${this.graphName}.v`, import.meta.url).pathname;
    await this.processVertexFile(filePath, 10000);
  }

  async dropNodeLabels() {
    const label = this.getVertexLabel();
    const query = `
      MATCH (n:\`${label}\`)
      WITH n LIMIT 500000
      DELETE n
    `;

    for (let i = 0; i < 20; i++) {
      const session = this.driver.session();
      await session.run(query)
        .then(result => {

        })
        .catch(error => {
          console.error('Error executing query:', error);
        })
        .finally(() => {
          session.close();
        });
    }
    console.log(`Deleted nodes.`);
  }

  async dropEdgeLabels() {
    const label = this.getEdgeLabel();
    const query = `
      MATCH ()-[r:\`${label}\`]->()
      WITH r LIMIT 500000
      DELETE r
    `;

    for (let i = 0; i < 20; i++) {
      const session = this.driver.session();
      await session.run(query)
        .then(result => {

        })
        .catch(error => {
          console.error('Error executing query:', error);
        })
        .finally(() => {
          session.close();
        });
    }
    console.log(`Deleted relationships.`);
  }

  async createGraph() {
    const nodeLabelsList = [
      this.getVertexLabel()
    ];

    const relationShipList = [
      this.getEdgeLabel()
    ];

    const cypherQuery = `
      CALL gds.graph.project(
        "${this.graphName}",
        ${JSON.stringify(nodeLabelsList)},
        ${JSON.stringify(relationShipList)}
      )`
    ;

    const session = this.driver.session();
    await session.run(cypherQuery);
    await session.close();
    console.log('Graph created successfully.');
  }

  async checkGraphExists() {
    console.log(this.graphName)
    const checkGraphCypherQuery =
      `CALL gds.graph.exists("${this.graphName}") YIELD exists RETURN exists`
    ;
    console.log(checkGraphCypherQuery)

    const session = this.driver.session();
    const result = await session.run(checkGraphCypherQuery);
    await session.close();

    return result.records[0].get('exists');
  }

  async prepareGraph() {
    if (this.dropGraph) {
      // edges need to be deleted first
      await this.dropEdgeLabels();
      await this.dropNodeLabels();
    }

    const graphExists = await this.checkGraphExists();

    if (!graphExists) {
      await this.createGraph();
    }
  }

  getVertexLabel() {
    return `${this.graphName}_v`;
  }

  getEdgeLabel() {
    return `${this.graphName}_e`;
  }

  readGraphProperties() {
    const filePath = new URL(`../../data/${this.graphName}/${this.graphName}.properties`, import.meta.url).pathname;
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

    return {expectedAmountOfVertices, expectedAmountOfEdges};
  }

  async getNodeLabelCount() {
    const session = this.driver.session();
    const label = this.getVertexLabel();
    const query = `
      MATCH (n:\`${label}\`)
      RETURN count(n) as count
    `;

    let count;
    await session.run(query)
      .then(result => {
        count = result.records[0].get('count').low;
      })
      .catch(error => {
        console.error('Error executing query:', error);
      })
      .finally(() => {
        session.close();
      });

    return count;
  }

  async getEdgeLabelCount() {
    const session = this.driver.session();
    const label = this.getEdgeLabel();
    const query = `
      MATCH ()-[r:\`${label}\`]->()
      RETURN count(r) as count
    `;

    let count;
    await session.run(query)
      .then(result => {
        count = result.records[0].get('count').low;
      })
      .catch(error => {
        console.error('Error executing query:', error);
      })
      .finally(() => {
        session.close();
      });

    return count;
  }

  async verifyGraph() {
    const {expectedAmountOfVertices, expectedAmountOfEdges} = await this.getVertexAndEdgeCountsToInsert();
    const vCount = await this.getNodeLabelCount();
    const eCount = await this.getEdgeLabelCount();

    if (vCount !== parseInt(expectedAmountOfVertices)) {
      throw new Error(`Expected amount of vertices (${expectedAmountOfVertices}) does not match actual amount of vertices (${vCount})`);
    }
    if (eCount !== parseInt(expectedAmountOfEdges)) {
      throw new Error(`Expected amount of edges (${expectedAmountOfEdges}) does not match actual amount of edges (${eCount})`);
    }
    console.log(`Graph ${this.graphName} verified. Expected amount of vertices: ${expectedAmountOfVertices}, actual amount of vertices: ${vCount}, expected amount of edges: ${expectedAmountOfEdges}, actual amount of edges: ${eCount}`)
  }
}
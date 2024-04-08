const {Database, aql} = require('arangojs');
const fs = require('fs');
const path = require('path');
const util = require('util');
const exec = util.promisify(require('child_process').exec);
const axios = require('axios');

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

class GraphImporter {
  constructor(arangoConfig, graphName, dropGraph = false) {
    this.arangoEndpoint = arangoConfig.endpoint;
    this.arangoUser = arangoConfig.username;
    this.arangoPassword = arangoConfig.password;
    this.graphName = graphName;
    this.dropGraph = dropGraph;
    this.databaseName = arangoConfig.databaseName;
    this.db = new Database({
      url: this.arangoEndpoint,
      auth: {username: this.arangoUser, password: this.arangoPassword},
    });
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
          let d = maker(counter);
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
          if (documentCount >= limit) {
            done = true;
          }
        }
      }

      if ((done && l.length > 0) || l.length >= batchSize) {
        let response = await axios.post(
          `${this.arangoEndpoint}/_db/${encodeURIComponent(db)}/_api/document/${encodeURIComponent(coll)}`,
          JSON.stringify(l),
          {
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

  async insertEdges() {
    const filePath = path.join(__dirname, '..', 'data', this.graphName, `${this.graphName}.e`);
    

    console.log(`Will now insert edges into collection ${this.graphName}_e. This will take a while...`)
    await this.insertManyDocumentsIntoCollection(this.databaseName, this.graphName + '_e',
      function (i) {
        const line = fs.readFileSync(filePath, 'utf8').split('\n')[i];
        const [from, to] = line.split(' ');
        return {
          _from: `${this.graphName}_v/${from}`,
          _to: `${this.graphName}_v/${to}`,
        };
      },
      lineCount, 10000);
  }

  async insertVertices() {
    const filePath = path.join(__dirname, '..', 'data', this.graphName, `${this.graphName}.v`);
    const lineCount = await this.countLinesUsingWc(filePath);

    //const query = `
    //  FOR i IN 1..${lineCount}
    //  INSERT {_key: TO_STRING(i)} INTO @@vertexCollection
    //`;
    //await this.db.query(query, {
    //  "@vertexCollection": this.graphName + '_v'
    //});

    console.log(`Will now insert vertices into collection ${this.graphName}_v. This will take a while...`)
    await this.insertManyDocumentsIntoCollection(this.databaseName, this.graphName + '_v',
      function (i) {
        return {_key: JSON.stringify(i)};
      },
      lineCount, 10000);
    console.log(`-> Inserted ${lineCount} vertices into collection ${this.graphName}_v`);
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
      await graph.create();
      const edgeCollectionName = `${this.graphName}_e`;
      const vertexCollectionName = `${this.graphName}_v`;
      await this.db.edgeCollection(edgeCollectionName).create();
      await this.db.collection(vertexCollectionName).create();
      await graph.addEdgeDefinition({
        collection: edgeCollectionName,
        from: [vertexCollectionName],
        to: [vertexCollectionName],
      });
      console.log(`Graph ${this.graphName} created with edge collection ${edgeCollectionName} and vertex collection ${vertexCollectionName}`);
    } else {
      throw new Error(`Graph ${this.graphName} already exists`);
    }
  }
}

module.exports = GraphImporter;
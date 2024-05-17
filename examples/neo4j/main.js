import {parseArgs} from './modules/argumentParser.js';
import {GraphImporter} from './modules/import.js';

const argv = parseArgs();

const main = async () => {
  const graphName = argv.graphName;
  console.log(`Starting insert of neo4j graph: ${graphName}`);
  const neo4jConfig = {
    endpoint: argv.neo4jEndpoint,
    username: argv.neo4jUser,
    password: argv.neo4jPassword,
    databaseName: argv.databaseName
  };

  const importOptions = {
    concurrency: argv.concurrency,
    maxQueueSize: argv.maxQueueSize,
  };

  let graphImporter = new GraphImporter(neo4jConfig, graphName, argv.dropGraph, importOptions);

  await graphImporter.cleanGraphData();

  if (!argv.skipVertices) {
    await graphImporter.insertVertices();
  }
  if (!argv.skipEdges) {
    await graphImporter.insertEdges();
  }
  if (argv.verifyGraph) {
    await graphImporter.verifyGraph();
  }

  // this is the "gds" utility. It can only be created after we have inserted all the data
  await graphImporter.prepareGraph();

}
main().then(() => console.log('Everything is done')).catch(e => console.error(e));
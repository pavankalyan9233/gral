import {parseArgs} from './modules/argumentParser.js';
import {GraphImporter} from './modules/graphImporter.js';


const argv = parseArgs();

const main = async () => {
  const graphName = argv.graphName;
  console.log(`Starting insert of graph: ${graphName}`);
  const arangoConfig = {
    endpoint: argv.arangoEndpoint,
    username: argv.arangoUser,
    password: argv.arangoPassword,
    databaseName: argv.databaseName,
    ca: argv.ca
  };

  const importOptions = {
    concurrency: argv.concurrency,
    maxQueueSize: argv.maxQueueSize,
  };
  
  let graphImporter = new GraphImporter(arangoConfig, graphName, argv.dropGraph, importOptions);
  await graphImporter.createGraph();

  if (!argv.skipVertices) {
    await graphImporter.insertVertices();
  }
  if (!argv.skipEdges) {
    await graphImporter.insertEdges();
  }
}
main().then(r => console.log('Everything is done')).catch(e => console.error(e));
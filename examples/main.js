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
    databaseName: argv.databaseName
  };

  const skipVertices = argv.skipVertices;
  const skipEdges = argv.skipEdges;

  let graphImporter = new GraphImporter(arangoConfig, graphName, argv.dropGraph);
  await graphImporter.createGraph();

  if (!skipVertices) {
    await graphImporter.insertVertices();
  }
  if (!skipEdges) {
    await graphImporter.insertEdges();
  }
}
main().then(r => console.log('Everything is done')).catch(e => console.error(e));
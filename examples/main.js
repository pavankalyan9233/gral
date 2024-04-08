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

  let graphImporter = new GraphImporter(arangoConfig, graphName, argv.dropGraph);
  await graphImporter.createGraph();
  await graphImporter.insertVertices();
  await graphImporter.insertEdges();
}
main().then(r => console.log('Everything is done')).catch(e => console.error(e));
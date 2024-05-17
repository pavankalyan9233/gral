const arangodb = require("./helpers/arangodb.js");
const c = require("./environment.config.js");
const gral = require("./helpers/gral.js");
const config = c.config;

const main = async () => {
  const jwt = await arangodb.getArangoJWT();
  console.log(config)
  const gralEndpoint = config.gral_instances.arangodb_auth;
  let graphNameToGralIdMap = {};

  // load wiki-Talk into gral
  const graphName = 'wiki-Talk';
  const vertexAttributes = ["_id", "@collectionname"]
  const response = await gral.loadGraph(jwt, gralEndpoint, graphName, [], [], vertexAttributes);
  graphNameToGralIdMap[graphName] = response.result.graph_id;

  // write the graphNameToGralIdMap to a file in JSON format
  const fs = require('fs');
  fs.writeFileSync('graphNameToGralIdMap.json', JSON.stringify(graphNameToGralIdMap));
}

main().then(r => console.log('Everything is done')).catch(e => console.error(e));
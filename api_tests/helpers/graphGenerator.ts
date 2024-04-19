import {config} from '../environment.config';
import ngGenerator = require('ngraph.generators');
import {GraphImporter} from "../../examples/modules/GraphImporter";

function getGraphConfig() {
  const arangoConfig = {
    url: config.arangodb.endpoint,
    database: config.arangodb.database,
    username: config.arangodb.username,
    password: config.arangodb.password
  };
  const dropGraph = true;
  const importOptions = {
    concurrency: null,
    max_queue_size: null,
  };
  return {arangoConfig, dropGraph, importOptions};
}

async function writeGraphToArangoDB(graph: any, graphName: string) {
  const {arangoConfig, dropGraph, importOptions} = getGraphConfig();
  let graphImporter = new GraphImporter(arangoConfig, graphName, dropGraph, importOptions);
  const vertexCollectionName = graphImporter.getVertexCollectionName();
  await graphImporter.createGraph();

  const nodes = [];
  graph.forEachNode((node) => {
    nodes.push({
      _key: `${node.id.toString()}`
    });
  });
  await graphImporter.insertVerticesArray(nodes)

  const edges = [];
  graph.forEachLink((edge) => {
    edges.push({
      _from: `${vertexCollectionName}/${edge.fromId.toString()}`,
      _to: `${vertexCollectionName}/${edge.toId.toString()}`
    });
  });

  await graphImporter.insertEdgesArray(edges);

  return graph;
}

async function generateCompleteGraph(k: number = 5, graphName: string = 'test_graph') {
  const graph = ngGenerator.complete(k);
  return await writeGraphToArangoDB(graph, graphName);
}

export const graphGenerator = {
  generateCompleteGraph
};


module.exports = graphGenerator;
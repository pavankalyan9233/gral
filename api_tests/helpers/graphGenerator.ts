import {config} from '../environment.config';
import GraphGenerator from 'ngraph.generators';
import {Graph} from "ngraph.graph";
import {GraphImporter} from "../../examples/modules/graphImporter.js";

function getArangoConfig() {
  return {
    url: config.arangodb.endpoint,
    database: config.arangodb.database,
    username: config.arangodb.username,
    password: config.arangodb.password
  };
}

function getImportOptions() {
  return {
    concurrency: null,
    max_queue_size: null,
  };
}

async function writeGraphToArangoDB(graph: Graph, graphName: string) {
  const arangoConfig = getArangoConfig();
  const importOptions = getImportOptions();
  const graphImporter = new GraphImporter(arangoConfig, graphName, true, importOptions);
  const vertexCollectionName = graphImporter.getVertexCollectionName();
  await graphImporter.createGraph();

  const vertices = [];
  graph.forEachNode((node) => {
    vertices.push({
      _key: `${node.id.toString()}`
    });
  });
  const edges = [];
  graph.forEachLink((edge) => {
    edges.push({
      _from: `${vertexCollectionName}/${edge.fromId.toString()}`,
      _to: `${vertexCollectionName}/${edge.toId.toString()}`
    });
  });

  await graphImporter.createGraphWithVerticesAndEdges(vertices, edges);

  return graph;
}

async function generateCompleteGraph(amountOfNodes: number = 5, graphName: string = 'test_graph') {
  const graph = GraphGenerator.complete(amountOfNodes);
  return await writeGraphToArangoDB(graph, graphName);
}

export const graphGenerator = {
  generateCompleteGraph
};


module.exports = graphGenerator;
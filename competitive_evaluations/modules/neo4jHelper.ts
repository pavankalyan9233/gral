import neo4j from "neo4j-driver";
import {config as environmentConfig} from "../../examples/config/environment";

const createGraph = async (graphName: string, node_properties: Array<string> = []) => {
  // creates neo4j in memory graph projection
  const driver = neo4j.driver(environmentConfig.neo4j.endpoint, neo4j.auth.basic(
    environmentConfig.neo4j.username, environmentConfig.neo4j.password
  ), {});

  const nodeLabelsList = [
    `${graphName}_v`
  ];

  const relationShipList = [
    `${graphName}_e`
  ];

  const cypherQuery = `
      CALL gds.graph.project(
        "${graphName}",
        ${JSON.stringify(nodeLabelsList)},
        ${JSON.stringify(relationShipList)},
        {
          nodeProperties: ${JSON.stringify(node_properties)}
        }
      )`
  ;

  const session = driver.session();
  await session.run(cypherQuery);
  await session.close();
}

const dropGraph = async (graphName: string) => {
  const driver = neo4j.driver(environmentConfig.neo4j.endpoint, neo4j.auth.basic(
    environmentConfig.neo4j.username, environmentConfig.neo4j.password
  ), {});

  const dropCypherQuery = `
    CALL gds.graph.drop('${graphName}')
  `;

  const session = driver.session();
  await session.run(dropCypherQuery)
    .then(() => {
      // currently we do not want to do anything with the result
    })
    .catch(error => {
      console.error('Error during deletion:', error);
    })
    .finally(() => {
      session.close();
    });
}

const runPageRank = async (graphName: string, maxIterations: number = 10, dampingFactor: number = 0.85) => {
  const driver = neo4j.driver(environmentConfig.neo4j.endpoint, neo4j.auth.basic(
    environmentConfig.neo4j.username, environmentConfig.neo4j.password
  ), {});
  const pageRankCypherQuery = `
    CALL gds.pageRank.stream(
      '${graphName}',
      {
        maxIterations: ${maxIterations}, 
        dampingFactor: ${dampingFactor},
        concurrency: 1
      }
    )
    YIELD nodeId, score
    `;

  const session = driver.session();
  await session.run(pageRankCypherQuery)
    .then(() => {
      // currently we do not want to do anything with the result
    })
    .catch(error => {
      console.error('Error during pagerank:', error);
    })
    .finally(() => {
      session.close();
    });
}

const runWCC = async (graphName: string) => {
  const driver = neo4j.driver(environmentConfig.neo4j.endpoint, neo4j.auth.basic(
    environmentConfig.neo4j.username, environmentConfig.neo4j.password
  ), {});
  const wccCypherQuery = `
    CALL gds.wcc.stream("${graphName}", {
      concurrency: 1
    })
    YIELD nodeId, componentId
  `;

  const session = driver.session();
  await session.run(wccCypherQuery)
    .then(() => {
      // currently we do not want to do anything with the result
    })
    .catch(error => {
      console.error('Error during wcc:', error);
    })
    .finally(() => {
      session.close();
    });
}

const runSCC = async (graphName: string) => {
  const driver = neo4j.driver(environmentConfig.neo4j.endpoint, neo4j.auth.basic(
    environmentConfig.neo4j.username, environmentConfig.neo4j.password
  ), {});
  const sccCypherQuery = `
    CALL gds.scc.stream("${graphName}", {
      concurrency: 1
    })
    YIELD nodeId, componentId
  `;

  const session = driver.session();
  await session.run(sccCypherQuery)
    .then(() => {
      // currently we do not want to do anything with the result
    })
    .catch(error => {
      console.error('Error during scc:', error);
    })
    .finally(() => {
      session.close();
    });
}

const runCDLP = async (graphName: string, startLabelAttribute: string = "") => {
  const driver = neo4j.driver(environmentConfig.neo4j.endpoint, neo4j.auth.basic(
    environmentConfig.neo4j.username, environmentConfig.neo4j.password
  ), {});
  const labelPropagationCypherQuery = `
    CALL gds.labelPropagation.stream(
      "${graphName}",
      {
        maxIterations: 10,
        concurrency: 1,
        seedProperty: '${startLabelAttribute}'
      }
    )
  `;

  const session = driver.session();
  await session.run(labelPropagationCypherQuery)
    .then(() => {
      // currently we do not want to do anything with the result
    })
    .catch(error => {
      console.error('Error during label propagation:', error);
    })
    .finally(() => {
      session.close();
    });
}

export const neo4jHelper = {
  createGraph, dropGraph, runPageRank, runWCC, runSCC, runCDLP
};


module.exports = neo4jHelper;

import neo4j from "neo4j-driver";
import {config as environmentConfig} from "../../examples/config/environment";

const runPageRank = async (graphName: string, maxIterations: number = 10, dampingFactor: number = 0.85) => {
  const driver = neo4j.driver(environmentConfig.neo4j.endpoint, neo4j.auth.basic(
    environmentConfig.neo4j.username, environmentConfig.neo4j.password
  ), {});
  const pageRankCypherQuery = `
    CALL gds.pageRank.stream(
      '${graphName}',
      {
        maxIterations: ${maxIterations}, 
        dampingFactor: ${dampingFactor}
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

export const neo4jHelper = {
  runPageRank
};


module.exports = neo4jHelper;

import {bench, describe, expect} from 'vitest';
import {config} from '../../api_tests/environment.config';
import {arangodb} from '../../api_tests/helpers/arangodb';
import {gral} from "../../api_tests/helpers/gral";
import {common} from "../modules/common";
import neo4j from 'neo4j-driver';

import {config as environmentConfig} from '../../examples/config/environment.js';

const ITERATIONS = 5;
const WARNUP_ITERATIONS = 1;

const gralEndpoint = config.gral_instances.arangodb_auth;
const neoEndpoint = environmentConfig.neo4j.endpoint;

describe.sequential('PageRank all services (Dataset: wiki-Talk)', () => {
  bench('GRAL', async () => {
    const jwt = await arangodb.getArangoJWT();
    const wikiTalkGraphId = common.getGralGraphId('wiki-Talk');
    expect(wikiTalkGraphId).toBeTypeOf('number');
    await gral.runPagerank(jwt, gralEndpoint, wikiTalkGraphId, 10, 0.85);
    // 1x warmupIteration as for the first run indices need to be created in-memory.
  }, {iterations: ITERATIONS, warmupIterations: WARNUP_ITERATIONS});

  bench('Neo4j', async () => {
    const driver = neo4j.driver(neoEndpoint, neo4j.auth.basic(
      environmentConfig.neo4j.username, environmentConfig.neo4j.password
    ), {});
    const pageRankCypherQuery = `
    CALL gds.pageRank.stream(
      'test',
      {
        maxIterations: 10, 
        dampingFactor: 0.85
      }
    )
    YIELD nodeId, score
    `;
    // RETURN gds.util.asNode(nodeId).customId AS id, score ORDER BY score DESC

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

  });

});
import {bench, describe, expect} from 'vitest';
import {config} from '../../api_tests/environment.config';
import {arangodb} from '../../api_tests/helpers/arangodb';
import {gral} from "../../api_tests/helpers/gral";
import {neo4jHelper} from "../../competitive_evaluations/modules/neo4jHelper";

const ITERATIONS = 5;
const WARMUP_ITERATIONS = 1;
const gralEndpoint = config.gral_instances.arangodb_auth;

describe.sequential('PageRank (Dataset: wiki-Talk)', () => {
  bench('GRAL', async () => {
    const jwt = await arangodb.getArangoJWT();
    const response = await gral.loadGraph(jwt, gralEndpoint, "wiki-Talk", [], [], [], 50);
    const wikiTalkGraphId = response.result.graph_id;
    expect(wikiTalkGraphId).toBeTypeOf('number');

    await gral.runPagerank(jwt, gralEndpoint, wikiTalkGraphId, 10, 0.85);
    // 1x warmupIteration as for the first run indices need to be created in-memory.
  }, {iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS});

  bench('Neo4j', async () => {
    await neo4jHelper.runPageRank('wiki-Talk', 10, 0.85);
  }, {iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS});

});
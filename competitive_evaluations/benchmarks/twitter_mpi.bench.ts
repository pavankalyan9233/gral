import {bench, describe} from 'vitest';
import {config} from '../../api_tests/environment.config';
import {arangodb} from '../../api_tests/helpers/arangodb';
import {gral} from "../../api_tests/helpers/gral";
import {neo4jHelper} from "../modules/neo4jHelper";

const ITERATIONS = 3;
const WARMUP_ITERATIONS = 0;

const gralEndpoint = config.gral_instances.arangodb_auth;
const graphName = 'twitter_mpi';

describe.sequential(`PageRank, Graph: ${graphName}`, () => {
  bench('GRAL', async () => {
    const jwt = await arangodb.getArangoJWT();
    const twitterMpiGraphId = await gral.loadGraph(jwt, gralEndpoint, graphName, [], [], [], 50);
    await gral.runPagerank(jwt, gralEndpoint, twitterMpiGraphId, 10, 0.85);
    await gral.dropGraph(jwt, gralEndpoint, twitterMpiGraphId);
  }, {iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS});

  bench('Neo4j', async () => {
    await neo4jHelper.createGraph(graphName);
    await neo4jHelper.runPageRank(graphName, 10, 0.85);
    await neo4jHelper.dropGraph(graphName);
  }, {iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS});
});

describe.sequential(`WCC, Graph: ${graphName}`, () => {
  bench('GRAL', async () => {
    const jwt = await arangodb.getArangoJWT();
    const twitterMpiGraphId = await gral.loadGraph(jwt, gralEndpoint, graphName, [], [], [], 50);
    await gral.runWCC(jwt, gralEndpoint, twitterMpiGraphId, {});
    await gral.dropGraph(jwt, gralEndpoint, twitterMpiGraphId);
  }, {iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS});

  bench('Neo4j', async () => {
    await neo4jHelper.createGraph(graphName);
    await neo4jHelper.runWCC(graphName);
    await neo4jHelper.dropGraph(graphName);
  }, {iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS});
});

describe.sequential(`SCC, Graph: ${graphName}`, () => {
  bench('GRAL', async () => {
    const jwt = await arangodb.getArangoJWT();
    const twitterMpiGraphId = await gral.loadGraph(jwt, gralEndpoint, graphName, [], [], [], 50);
    await gral.runSCC(jwt, gralEndpoint, twitterMpiGraphId, {});
    await gral.dropGraph(jwt, gralEndpoint, twitterMpiGraphId);
  }, {iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS});

  bench('Neo4j', async () => {
    await neo4jHelper.createGraph(graphName);
    await neo4jHelper.runSCC(graphName);
    await neo4jHelper.dropGraph(graphName);
  }, {iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS});
});

describe.sequential.skip(`Label Propagation, Graph: ${graphName}`, () => {
  // skipped because of too high algorithim complexity in relation to the graph size

  bench('GRAL', async () => {
    const jwt = await arangodb.getArangoJWT();
    const twitterMpiGraphId = await gral.loadGraph(jwt, gralEndpoint, graphName, [], [], ['_key'], 50);
    await gral.runCDLP(jwt, gralEndpoint, twitterMpiGraphId, "_key");
    // _key equals the data original source id
    await gral.dropGraph(jwt, gralEndpoint, twitterMpiGraphId);
  }, {iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS});

  bench('Neo4j', async () => {
    await neo4jHelper.createGraph(graphName, ["customId"]);
    // customId equals the data original source id
    // cannot be set to the original neo4j's id as this value cannot be set from the outside
    await neo4jHelper.runCDLP(graphName, "customId");
    await neo4jHelper.dropGraph(graphName);
  }, {iterations: ITERATIONS, warmupIterations: WARMUP_ITERATIONS});
});
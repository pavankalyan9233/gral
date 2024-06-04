import {bench, describe} from 'vitest';
import {config} from '../../api_tests/environment.config';
import {arangodb} from '../../api_tests/helpers/arangodb';
import {gral} from "../../api_tests/helpers/gral";
import {neo4jHelper} from "../modules/neo4jHelper";

const gralEndpoint = config.gral_instances.arangodb_auth;
const graphName = 'dota-league';
const benchmarkOptions = {
  time: 0,
  iterations: 3,
  warmupTime: 0,
  warmupIterations: 0
};

describe.sequential(`PageRank, Graph: ${graphName}`, () => {
  bench('GRAL', async () => {
    const jwt = await arangodb.getArangoJWT();
    const dotaLeagueGraphId = await gral.loadGraph(jwt, gralEndpoint, graphName, [], [], [], 50);
    await gral.runPagerank(jwt, gralEndpoint, dotaLeagueGraphId, 10, 0.85);
    await gral.dropGraph(jwt, gralEndpoint, dotaLeagueGraphId);
  }, benchmarkOptions);

  bench('Neo4j', async () => {
    await neo4jHelper.createGraph(graphName);
    await neo4jHelper.runPageRank(graphName, 10, 0.85);
    await neo4jHelper.dropGraph(graphName);
  }, benchmarkOptions);
});

describe.sequential(`WCC, Graph: ${graphName}`, () => {
  bench('GRAL', async () => {
    const jwt = await arangodb.getArangoJWT();
    const dotaLeagueGraphId = await gral.loadGraph(jwt, gralEndpoint, graphName, [], [], [], 50);
    await gral.runWCC(jwt, gralEndpoint, dotaLeagueGraphId, {});
    await gral.dropGraph(jwt, gralEndpoint, dotaLeagueGraphId);
  }, benchmarkOptions);

  bench('Neo4j', async () => {
    await neo4jHelper.createGraph(graphName);
    await neo4jHelper.runWCC(graphName);
    await neo4jHelper.dropGraph(graphName);
  }, benchmarkOptions);
});

describe.sequential(`SCC, Graph: ${graphName}`, () => {
  bench('GRAL', async () => {
    const jwt = await arangodb.getArangoJWT();
    const dotaLeagueGraphId = await gral.loadGraph(jwt, gralEndpoint, graphName, [], [], [], 50);
    await gral.runSCC(jwt, gralEndpoint, dotaLeagueGraphId, {});
    await gral.dropGraph(jwt, gralEndpoint, dotaLeagueGraphId);
  }, benchmarkOptions);

  bench('Neo4j', async () => {
    await neo4jHelper.createGraph(graphName);
    await neo4jHelper.runSCC(graphName);
    await neo4jHelper.dropGraph(graphName);
  }, benchmarkOptions);
});

describe.sequential(`Label Propagation, Graph: ${graphName}`, () => {
  bench('GRAL', async () => {
    const jwt = await arangodb.getArangoJWT();
    const dotaLeagueGraphId = await gral.loadGraph(jwt, gralEndpoint, graphName, [], [], ['_key'], 50);
    await gral.runCDLP(jwt, gralEndpoint, dotaLeagueGraphId, "_key");
    // _key equals the data original source id
    await gral.dropGraph(jwt, gralEndpoint, dotaLeagueGraphId);
  }, benchmarkOptions);

  bench('Neo4j', async () => {
    await neo4jHelper.createGraph(graphName, ["customId"]);
    // customId equals the data original source id
    // cannot be set to the original neo4j's id as this value cannot be set from the outside
    await neo4jHelper.runCDLP(graphName, "customId");
    await neo4jHelper.dropGraph(graphName);
  }, benchmarkOptions);
});
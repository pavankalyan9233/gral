import {bench, describe} from 'vitest';
import {config} from '../../api_tests/environment.config';
import {arangodb} from '../../api_tests/helpers/arangodb';
import {gral} from "../../api_tests/helpers/gral";
import {neo4jHelper} from "../modules/neo4jHelper";

const gralEndpoint = config.gral_instances.arangodb_auth;
const graphName = 'dota-league';

const loadDropGraphOptions = {
  time: 0,
  iterations: 1,
  warmupTime: 0,
  warmupIterations: 0
};

const benchmarkOptions = {
  time: 0,
  iterations: 3,
  warmupTime: 0,
  warmupIterations: 0
};

let wikiTalkGraphId = -1;

describe.sequential(`Load Graph: ${graphName}`, () => {
  bench('GRAL', async () => {
    const jwt = await arangodb.getArangoJWT();
    wikiTalkGraphId = await gral.loadGraph(jwt, gralEndpoint, graphName, [], [], [], 200);
  }, loadDropGraphOptions);

  bench('Neo4j', async () => {
    await neo4jHelper.createGraph(graphName);
  }, loadDropGraphOptions);
});

describe.sequential(`PageRank, Graph: ${graphName}`, () => {
  bench('GRAL', async () => {
    const jwt = await arangodb.getArangoJWT();
    await gral.runPagerank(jwt, gralEndpoint, wikiTalkGraphId, 10, 0.85);
  }, benchmarkOptions);

  bench('Neo4j', async () => {
    await neo4jHelper.runPageRank(graphName, 10, 0.85);
  }, benchmarkOptions);
});

describe.sequential(`WCC, Graph: ${graphName}`, () => {
  bench('GRAL', async () => {
    const jwt = await arangodb.getArangoJWT();
    await gral.runWCC(jwt, gralEndpoint, wikiTalkGraphId, {});
  }, benchmarkOptions);

  bench('Neo4j', async () => {
    await neo4jHelper.runWCC(graphName);
  }, benchmarkOptions);
});

describe.sequential(`SCC, Graph: ${graphName}`, () => {
  bench('GRAL', async () => {
    const jwt = await arangodb.getArangoJWT();
    await gral.runSCC(jwt, gralEndpoint, wikiTalkGraphId, {});
  }, benchmarkOptions);

  bench('Neo4j', async () => {
    await neo4jHelper.runSCC(graphName);
  }, benchmarkOptions);
});

describe.sequential(`Drop Graph: ${graphName}`, () => {
  bench('GRAL', async () => {
    const jwt = await arangodb.getArangoJWT();
    await gral.dropGraph(jwt, gralEndpoint, wikiTalkGraphId);
  }, loadDropGraphOptions);

  bench('Neo4j', async () => {
    await neo4jHelper.dropGraph(graphName);
  }, loadDropGraphOptions);
});

describe.sequential(`Load Graph with Attributes: ${graphName}`, () => {
  bench('GRAL', async () => {
    const jwt = await arangodb.getArangoJWT();
    wikiTalkGraphId = await gral.loadGraph(jwt, gralEndpoint, graphName, [], [], ['_key'], 50);
  }, loadDropGraphOptions);

  bench('Neo4j', async () => {
    await neo4jHelper.createGraph(graphName, ["customId"]);
  }, loadDropGraphOptions);
});

describe.sequential(`Label Propagation, Graph: ${graphName}`, () => {
  bench('GRAL', async () => {
    const jwt = await arangodb.getArangoJWT();
    await gral.runCDLP(jwt, gralEndpoint, wikiTalkGraphId, "_key");
  }, benchmarkOptions);

  bench('Neo4j', async () => {
    // customId equals the data original source id
    // cannot be set to the original neo4j's id as this value cannot be set from the outside
    await neo4jHelper.runCDLP(graphName, "customId");
  }, benchmarkOptions);
});

describe.sequential(`Drop Graph: ${graphName}`, () => {
  bench('GRAL', async () => {
    const jwt = await arangodb.getArangoJWT();
    await gral.dropGraph(jwt, gralEndpoint, wikiTalkGraphId);
  }, loadDropGraphOptions);

  bench('Neo4j', async () => {
    await neo4jHelper.dropGraph(graphName);
  }, loadDropGraphOptions);
});
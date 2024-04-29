import {beforeAll, bench, describe, expect} from 'vitest';
import {config} from '../../environment.config';
import {arangodb} from '../../helpers/arangodb';
import {benchmarkHelper} from "../../helpers/benchmark";
import {gral} from "../../helpers/gral";

const gralEndpoint = config.gral_instances.arangodb_auth;
let pageRankGraphID = 0;

describe.sequential('PageRank Benchmarks', () => {

  // First, load all graphs into gral
  bench('Load Graph: wiki-Talk', async () => {
    const jwt = await arangodb.getArangoJWT();
    const graphName = 'wiki-Talk';
    const response = await gral.loadGraph(jwt, gralEndpoint, graphName);
    pageRankGraphID = response.result.graph_id;
  }, {iterations: 3});

  // Then, execute all algorithms we want to run on it
  bench('PageRank Native', async () => {
    const jwt = await arangodb.getArangoJWT();
    await gral.runPagerank(jwt, gralEndpoint, 59, 10, 0.85);
  }, {iterations: 5});

  bench('PageRank Python', async () => {
    const jwt = await arangodb.getArangoJWT();
    //await gral.runPythonPagerank(jwt, gralEndpoint, 'pagerank', pageRankGraphID);
  }, {iterations: 5});


});
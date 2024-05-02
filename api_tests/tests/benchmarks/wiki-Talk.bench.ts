import {beforeAll, bench, describe, expect} from 'vitest';
import {config} from '../../environment.config';
import {arangodb} from '../../helpers/arangodb';
import {benchmarkHelper} from "../../helpers/benchmark";
import {gral} from "../../helpers/gral";

const gralEndpoint = config.gral_instances.arangodb_auth;
let wikiTalkGraphID = 0;

describe.sequential('PageRank Benchmarks', () => {

  // First, load all graphs into gral
  bench('Load Graph: wiki-Talk', async () => {
    // TODO: As soon as we have named graphs in GRAL, we will pull out graph creation out of here so we
    //  only get algorithm related benchmark results here.
    const jwt = await arangodb.getArangoJWT();
    const graphName = 'wiki-Talk';
    const response = await gral.loadGraph(jwt, gralEndpoint, graphName);
    wikiTalkGraphID = response.result.graph_id;
  }, {iterations: 1, warmupIterations: 0});

  // Then, execute all algorithms we want to run on it

  // Currently skipped, as usage is not clear.
  bench.skip('iRank', async () => {
    const jwt = await arangodb.getArangoJWT();
    await gral.runIRank(jwt, gralEndpoint, wikiTalkGraphID, 10, 0.85);
    // 1x warmupIteration as for the first run indices need to be created in-memory.
  }, {iterations: 3, warmupIterations: 1});

  bench('PageRank', async () => {
    const jwt = await arangodb.getArangoJWT();
    await gral.runPagerank(jwt, gralEndpoint, wikiTalkGraphID, 10, 0.85);
    // 1x warmupIteration as for the first run indices need to be created in-memory.
  }, {iterations: 3, warmupIterations: 1});

  bench('PageRank Python', async () => {
    const jwt = await arangodb.getArangoJWT();
    await gral.runPythonPagerank(jwt, gralEndpoint, wikiTalkGraphID, 10, 0.85);
    // no warmup iterations required. Only choosing 1 iteration as this execution is pretty slow.
  }, {iterations: 1, warmupIterations: 0});

  bench('WCC', async () => {
    const jwt = await arangodb.getArangoJWT();
    const customFields = {};
    await gral.runWCC(jwt, gralEndpoint, wikiTalkGraphID, customFields);
    // 1x warmupIteration as for the first run indices need to be created in-memory.
  }, {iterations: 3, warmupIterations: 1});

  bench('SCC', async () => {
    const jwt = await arangodb.getArangoJWT();
    const customFields = {};
    await gral.runSCC(jwt, gralEndpoint, wikiTalkGraphID, customFields);
    // 1x warmupIteration as for the first run indices need to be created in-memory.
  }, {iterations: 3, warmupIterations: 1});

});
import {beforeAll, describe, expect, expectTypeOf, test} from 'vitest';
import {config} from '../environment.config';
import {arangodb} from '../helpers/arangodb';
import {gral} from '../helpers/gral';
import axios from 'axios';
import {validator} from "../helpers/validator";

const gralEndpoint = config.gral_instances.arangodb_auth;

function isClose(a: number, b: number, relativeTolerance: number = 1e-5) {
  // relativeTolerance: "floating point number value for percentage"
  return Math.abs(a - b) <= Math.max(Math.abs(a), Math.abs(b)) * relativeTolerance;
}

const verifyGraphStatus = async (graphId: string, jwt: string) => {
  // precondition
  expect(graphId).not.toBeUndefined();
  expect(graphId).not.toBe('');

  const url = gral.buildUrl(gralEndpoint, `/v1/graphs/${graphId}`);
  const response = await axios.get(url, gral.buildHeaders(jwt));
  expect(response.status).toBe(200);
  expect(response.data).toBeInstanceOf(Object);
  const body = response.data;
  expect(body).toHaveProperty('graph');
  expectTypeOf(body.graph).toBeObject();
  expect(body.graph).toHaveProperty('graph_id');
  expectTypeOf(body.graph.graph_id).toBeString();
  const graph = body.graph;
  expect(graph.graph_id).toBe(graphId);

  expect(graph).toHaveProperty('number_of_vertices');
  expectTypeOf(graph.number_of_vertices).toBeString();
  const numberOfVertices = parseInt(graph.number_of_vertices, 10);
  expect(numberOfVertices).toBeGreaterThan(0);
  expect(numberOfVertices).toBe(2394385);

  expect(graph).toHaveProperty('number_of_edges');
  expectTypeOf(graph.number_of_edges).toBeString();
  const numberOfEdges = parseInt(graph.number_of_edges, 10);
  expect(numberOfEdges).toBeGreaterThan(0);
  expect(numberOfEdges).toBe(5021410);

  expect(graph).toHaveProperty('memory_per_vertex');
  expectTypeOf(graph.memory_per_vertex).toBeString();
  const memoryPerVertex = parseInt(graph.memory_per_vertex, 10);
  expect(memoryPerVertex).toBeGreaterThan(0);

  expect(graph).toHaveProperty('memory_per_edge');
  expectTypeOf(graph.memory_per_edge).toBeString();
  const memoryPerEdge = parseInt(graph.memory_per_edge, 10);
  expect(memoryPerEdge).toBeGreaterThan(0);

  expect(graph).toHaveProperty('memory_usage');
  expectTypeOf(graph.memory_usage).toBeString();
  const memoryUsage = parseInt(graph.memory_usage, 10);
  expect(memoryUsage).toBeGreaterThan(0);
  const expectedMemoryUsage = (numberOfVertices * memoryPerVertex) + (numberOfEdges * memoryPerEdge);
  // we cannot expect the exact value, as the amount of memory per vertex or edge might is divided by the number of
  // vertices or edges, respectively, and the division might not be exact
  const closenessResult = isClose(memoryUsage, expectedMemoryUsage, 0.05);
  expect(closenessResult).toBeTruthy();
};

describe.sequential('API tests based on wiki-Talk graph dataset', () => {

  let jwt: string;
  let graphIdForComputation: number;
  let resultIdPagerank: string;
  let resultIdWcc: string;
  let resultIdCdlp: string;

  beforeAll(async () => {
    jwt = await arangodb.getArangoJWT();
    expect(jwt).not.toBe('');
    expect(jwt).not.toBeUndefined();

    await arangodb.executeQuery(`
      LET totalDocuments = LENGTH(@@collectionName)
      FOR doc IN @@collectionName
        LET keyAsNumber = TO_NUMBER(doc._key)
        LET lexicographicValue = CONCAT("000000", TO_STRING(keyAsNumber))
        LET formattedLexicographicKey = RIGHT(lexicographicValue, 7)
      UPDATE doc WITH { lexicographicKey: formattedLexicographicKey } IN @@collectionName
    `, {"@collectionName": "wiki-Talk_v"});
  }, config.test_configuration.xtra_long_timeout * 2);

  test('load the wiki-Talk graph with graph_name and vertex and edge collections given', async () => {
    const url = gral.buildUrl(gralEndpoint, '/v1/loaddata');
    const graphAnalyticsEngineLoadDataRequest = {
      "database": "_system",
      "graph_name": "wiki-Talk",
      "vertex_collections": ["wiki-Talk_v"],
      "edge_collections": ["wiki-Talk_e"]
    };

    const response = await axios.post(
      url, graphAnalyticsEngineLoadDataRequest, gral.buildHeaders(jwt)
    );
    const body = response.data;

    try {
      await gral.waitForJobToBeFinished(gralEndpoint, jwt, body.job_id);
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect(error.message).toContain('Either specify the graph_name or ensure that vertex_collections and edge_collections are not empty.');
    }
  });

  test('load a graph graph with empty vertex and edge collections given', async () => {
    const url = gral.buildUrl(gralEndpoint, '/v1/loaddata');
    const graphAnalyticsEngineLoadDataRequest = {
      "database": "_system",
      "vertex_collections": [],
      "edge_collections": []
    };

    const response = await axios.post(
      url, graphAnalyticsEngineLoadDataRequest, gral.buildHeaders(jwt)
    );
    const body = response.data;

    try {
      await gral.waitForJobToBeFinished(gralEndpoint, jwt, body.job_id);
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect(error.message).toContain('Either specify the graph_name or ensure that vertex_collections and edge_collections are not empty.');
    }
  });

  test('load the wiki-Talk graph into memory, via provided graph name', async () => {
    const url = gral.buildUrl(gralEndpoint, '/v1/loaddata');
    const graphAnalyticsEngineLoadDataRequest = {
      "database": "_system",
      "graph_name": "wiki-Talk"
    };

    const response = await axios.post(
      url, graphAnalyticsEngineLoadDataRequest, gral.buildHeaders(jwt)
    );

    expect(response.status).toBe(200);
    expect(response.data).toBeInstanceOf(Object);
    const body = response.data;
    expect(body).toHaveProperty('job_id');
    expectTypeOf(body.job_id).toBeString();
    expect(body).toHaveProperty('graph_id');
    expectTypeOf(body.graph_id).toBeString();

    // check that both are numeric values (but strings...)
    expect(parseInt(body.job_id, 10)).not.toBeNaN();
    expect(parseInt(body.graph_id, 10)).not.toBeNaN();
    const graph_id = body.graph_id;

    // wait for the job to be finished
    const jobResponse = await gral.waitForJobToBeFinished(gralEndpoint, jwt, body.job_id);
    expect(jobResponse).toBeInstanceOf(Object);
    expect(jobResponse).toHaveProperty('result');
    expect(jobResponse.result).toBeInstanceOf(Object);
    expect(jobResponse.result).toHaveProperty('progress');
    expect(jobResponse.result).toHaveProperty('total');
    expect(jobResponse.result).toHaveProperty('job_id');
    expect(jobResponse.result).toHaveProperty('graph_id');
    expect(jobResponse.result.progress).toBe(jobResponse.result.total);
    expect(jobResponse.result.job_id).toBe(body.job_id);
    expect(jobResponse.result.graph_id).toBe(body.graph_id);
    await verifyGraphStatus(graph_id, jwt);
  }, config.test_configuration.long_timeout);

  test('load the wiki-Talk graph into memory, via provided edge and vertex names', async () => {
    // Note: This graph is being used for algorithm validation later.
    const url = gral.buildUrl(gralEndpoint, '/v1/loaddata');
    const vertexAttributes = ["lexicographicKey"];
    const graphAnalyticsEngineLoadDataRequest = {
      "database": "_system",
      "vertex_collections": ["wiki-Talk_v"],
      "edge_collections": ["wiki-Talk_e"],
      "vertex_attributes": vertexAttributes
    };

    const response = await axios.post(
      url, graphAnalyticsEngineLoadDataRequest, gral.buildHeaders(jwt)
    );

    expect(response.status).toBe(200);
    expect(response.data).toBeInstanceOf(Object);
    const body = response.data;
    expect(body).toHaveProperty('job_id');
    expectTypeOf(body.job_id).toBeString();
    expect(body).toHaveProperty('graph_id');
    expectTypeOf(body.graph_id).toBeString();

    // check that both are numeric values (but strings...)
    expect(parseInt(body.job_id, 10)).not.toBeNaN();
    expect(parseInt(body.graph_id, 10)).not.toBeNaN();
    const graph_id = body.graph_id;

    // wait for the job to be finished
    const jobResponse = await gral.waitForJobToBeFinished(gralEndpoint, jwt, body.job_id);
    expect(jobResponse).toBeInstanceOf(Object);
    expect(jobResponse).toHaveProperty('result');
    expect(jobResponse.result).toBeInstanceOf(Object);
    expect(jobResponse.result).toHaveProperty('progress');
    expect(jobResponse.result).toHaveProperty('total');
    expect(jobResponse.result).toHaveProperty('job_id');
    expect(jobResponse.result).toHaveProperty('graph_id');
    expect(jobResponse.result.progress).toBe(jobResponse.result.total);
    expect(jobResponse.result.job_id).toBe(body.job_id);
    expect(jobResponse.result.graph_id).toBe(body.graph_id);
    graphIdForComputation = graph_id;
    await verifyGraphStatus(graph_id, jwt);
  }, config.test_configuration.long_timeout);

  test('the list of graphs should now contain at least one graph', async () => {
    const url = gral.buildUrl(gralEndpoint, `/v1/graphs`);
    const response = await axios.get(url, gral.buildHeaders(jwt));
    expect(response.status).toBe(200);
    expect(response.data).toBeInstanceOf(Object);
    const body = response.data;
    expect(body).toBeInstanceOf(Array);
    expect(body.length).toBeGreaterThan(0);
  });

  test('run the pagerank algorithm on one of the created graphs', async () => {
    const url = gral.buildUrl(gralEndpoint, '/v1/pagerank');
    const graphAnalyticsEngineRunPageRank = {
      "graph_id": graphIdForComputation,
      "maximum_supersteps": 10,
      "damping_factor": 0.85
    };
    const response = await axios.post(
      url, graphAnalyticsEngineRunPageRank, gral.buildHeaders(jwt)
    );
    expect(response.data).toBeInstanceOf(Object);
    expect(response.data).toHaveProperty('job_id');
    const job_id = response.data.job_id;
    expectTypeOf(job_id).toBeString();
    expect(job_id).not.toBe('');
    expect(job_id).not.toBeUndefined();
    const jobResponse = await gral.waitForJobToBeFinished(gralEndpoint, jwt, job_id);
    expect(jobResponse).toBeInstanceOf(Object);
    expect(jobResponse).toHaveProperty('result');
    expect(jobResponse.result).toBeInstanceOf(Object);
    const expectedProperties = ['progress', 'total', 'job_id', 'graph_id', 'comp_type', 'progress'];
    expectedProperties.forEach((property) => {
      expect(jobResponse.result).toHaveProperty(property);
    });
    expect(jobResponse.result.progress).toBe(jobResponse.result.total);
    expect(jobResponse.result.progress).toBe(100);
    expect(jobResponse.result.job_id).toBe(job_id);
    expect(jobResponse.result.graph_id).toBe(graphIdForComputation);
    expect(jobResponse.result.comp_type).toBe('pagerank');
    expectTypeOf(jobResponse.result.memory_usage).toBeString();
    const memoryUsage = parseInt(jobResponse.result.memory_usage, 10);
    expect(memoryUsage).toBeGreaterThan(0);
    resultIdPagerank = jobResponse.result.job_id;
  }, config.test_configuration.medium_timeout);

  test('run the wcc algorithm on one of the created graphs', async () => {
    const wccJobResponse = await gral.runWCC(jwt, gralEndpoint, graphIdForComputation);
    resultIdWcc = wccJobResponse.result.job_id;
  }, config.test_configuration.medium_timeout);

  test('run the label propagation (sync) algorithm on one of the created graphs', async () => {
    const cdlpJobResponse = await gral.runCDLP(jwt, gralEndpoint, graphIdForComputation, "lexicographicKey");
    resultIdCdlp = cdlpJobResponse.result.job_id;
  }, config.test_configuration.xtra_long_timeout * 3);

  test('Verify pagerank result', async () => {
    const resultAttrName = 'iResult';
    const resultCollection = await arangodb.createDocumentCollection('results');
    await gral.storeComputationResult(
      resultIdPagerank, config.arangodb.database, resultCollection.name, resultAttrName, jwt, gralEndpoint
    );
    const count = await resultCollection.count();
    expect(count.count).toBe(2394385);

    const computedDocs = await arangodb.executeQuery(`
      FOR doc IN ${resultCollection.name}
      RETURN [TO_NUMBER(SPLIT(doc.id, "/")[1]), doc.${resultAttrName}]
    `);

    await validator.verifyPageRankDocuments('wiki-Talk', computedDocs);
  }, config.test_configuration.xtra_long_timeout);

  test('Verify wcc result', async () => {
    const resultAttrName = 'iResult';
    const resultCollection = await arangodb.createDocumentCollection('results');
    await gral.storeComputationResult(
      resultIdWcc, config.arangodb.database, resultCollection.name, resultAttrName, jwt, gralEndpoint
    );
    const count = await resultCollection.count();
    expect(count.count).toBe(2394385);

    const computedDocs = await arangodb.executeQuery(`
      FOR doc IN ${resultCollection.name}
      RETURN [TO_NUMBER(SPLIT(doc.id, "/")[1]), TO_NUMBER(SPLIT(doc.${resultAttrName}, "/")[1])]
    `);

    await validator.verifyWCCResults('wiki-Talk', computedDocs);
  }, config.test_configuration.xtra_long_timeout);

  test('Verify cdlp result', async () => {
    const resultAttrName = 'iResult';
    const resultCollection = await arangodb.createDocumentCollection('results');
    await gral.storeComputationResult(
      resultIdCdlp, config.arangodb.database, resultCollection.name, resultAttrName, jwt, gralEndpoint
    );
    const count = await resultCollection.count();
    expect(count.count).toBe(2394385);

    const computedDocs = await arangodb.executeQuery(`
      FOR doc IN ${resultCollection.name}
      LIMIT 10
      RETURN [TO_NUMBER(SPLIT(doc.id, "/")[1]), TO_NUMBER(doc.${resultAttrName})]
    `);

    await validator.verifyCDLPResults('wiki-Talk', computedDocs);
  }, config.test_configuration.xtra_long_timeout);


});

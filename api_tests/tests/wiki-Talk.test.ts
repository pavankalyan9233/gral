import {beforeAll, describe, expect, expectTypeOf, test} from 'vitest';
import {config} from '../environment.config';
import {arangodb} from '../helpers/arangodb';
import {gral} from '../helpers/gral';
import axios from 'axios';

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
  expect(body.graph).toHaveProperty('graphId');
  expectTypeOf(body.graph.graphId).toBeString();
  const graph = body.graph;
  expect(graph.graphId).toBe(graphId);

  expect(graph).toHaveProperty('numberOfVertices');
  expectTypeOf(graph.numberOfVertices).toBeString();
  const numberOfVertices = parseInt(graph.numberOfVertices);
  expect(numberOfVertices).toBeGreaterThan(0);
  expect(numberOfVertices).toBe(2394385);

  expect(graph).toHaveProperty('numberOfEdges');
  expectTypeOf(graph.numberOfEdges).toBeString();
  const numberOfEdges = parseInt(graph.numberOfEdges);
  expect(numberOfEdges).toBeGreaterThan(0);
  expect(numberOfEdges).toBe(5021410);

  expect(graph).toHaveProperty('memoryPerVertex');
  expectTypeOf(graph.memoryPerVertex).toBeString();
  const memoryPerVertex = parseInt(graph.memoryPerVertex);
  expect(memoryPerVertex).toBeGreaterThan(0);

  expect(graph).toHaveProperty('memoryPerEdge');
  expectTypeOf(graph.memoryPerEdge).toBeString();
  const memoryPerEdge = parseInt(graph.memoryPerEdge);
  expect(memoryPerEdge).toBeGreaterThan(0);

  expect(graph).toHaveProperty('memoryUsage');
  expectTypeOf(graph.memoryUsage).toBeString();
  const memoryUsage = parseInt(graph.memoryUsage);
  expect(memoryUsage).toBeGreaterThan(0);
  const expectedMemoryUsage = (numberOfVertices * memoryPerVertex) + (numberOfEdges * memoryPerEdge);
  // we cannot expect the exact value, as the amount of memory per vertex or edge might is divided by the number of
  // vertices or edges, respectively, and the division might not be exact
  const closenessResult = isClose(memoryUsage, expectedMemoryUsage, 0.05);
  expect(closenessResult).toBeTruthy();
};

describe.sequential('API tests based on wiki-Talk graph dataset', () => {

  let jwt: string;
  let graphIdForComputation: string;

  beforeAll(async () => {
    jwt = await arangodb.getArangoJWT();
    expect(jwt).not.toBe('');
    expect(jwt).not.toBeUndefined();
  }, config.test_configuration.medium_timeout);

  test('get information about a graph, before created', async () => {
    const url = gral.buildUrl(gralEndpoint, `/v1/graphs/1337`);
    await axios.get(url, gral.buildHeaders(jwt)).catch((error) => {
      expect(error.response.status).toBe(404);
      const body = error.response.data;
      expect(body).toBeInstanceOf(Object);
      expect(body).toHaveProperty('errorCode');
      expect(body).toHaveProperty('errorMessage');
      expectTypeOf(body.errorCode).toBeNumber();
      expectTypeOf(body.errorMessage).toBeString();
      expect(body.errorCode).toBe(404);
      expect(body.errorMessage).toBe('Graph 1337 not found!');
    });
  });

  test('load the wiki-Talk graph without graph_name and vertex and edge collections given', async () => {
    const url = gral.buildUrl(gralEndpoint, '/v1/loaddata');
    const graphAnalyticsEngineLoadDataRequest = {
      "database": "_system"
    };

    const response = await axios.post(
      url, graphAnalyticsEngineLoadDataRequest, gral.buildHeaders(jwt)
    );
    const body = response.data;

    try {
      await gral.waitForJobToBeFinished(gralEndpoint, jwt, body.jobId);
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect(error.message).toContain('Either specify the graph_name or ensure that vertex_collections and edge_collections are not empty.');
    }
  });

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
      await gral.waitForJobToBeFinished(gralEndpoint, jwt, body.jobId);
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect(error.message).toContain('Either specify the graph_name or ensure that vertex_collections and edge_collections are not empty.');
    }
  });

  test('load the wiki-Talk graph with empty vertex and edge collections given', async () => {
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
      await gral.waitForJobToBeFinished(gralEndpoint, jwt, body.jobId);
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
    expect(body).toHaveProperty('jobId');
    expectTypeOf(body.jobId).toBeString();
    expect(body).toHaveProperty('graphId');
    expectTypeOf(body.graphId).toBeString();

    // check that both are numeric values (but strings...)
    expect(parseInt(body.jobId)).not.toBeNaN();
    expect(parseInt(body.graphId)).not.toBeNaN();
    const graphId = body.graphId;

    // wait for the job to be finished
    const jobResponse = await gral.waitForJobToBeFinished(gralEndpoint, jwt, body.jobId);
    expect(jobResponse).toBeInstanceOf(Object);
    expect(jobResponse).toHaveProperty('result');
    expect(jobResponse.result).toBeInstanceOf(Object);
    expect(jobResponse.result).toHaveProperty('progress');
    expect(jobResponse.result).toHaveProperty('total');
    expect(jobResponse.result).toHaveProperty('jobId');
    expect(jobResponse.result).toHaveProperty('graphId');
    expect(jobResponse.result.progress).toBe(jobResponse.result.total);
    expect(jobResponse.result.jobId).toBe(body.jobId);
    expect(jobResponse.result.graphId).toBe(body.graphId);
    await verifyGraphStatus(graphId, jwt);
  }, config.test_configuration.long_timeout);

  test('load the wiki-Talk graph into memory, via provided edge and vertex names', async () => {
    const url = gral.buildUrl(gralEndpoint, '/v1/loaddata');
    const graphAnalyticsEngineLoadDataRequest = {
      "database": "_system",
      "vertex_collections": ["wiki-Talk_v"],
      "edge_collections": ["wiki-Talk_e"]
    };

    const response = await axios.post(
      url, graphAnalyticsEngineLoadDataRequest, gral.buildHeaders(jwt)
    );

    expect(response.status).toBe(200);
    expect(response.data).toBeInstanceOf(Object);
    const body = response.data;
    expect(body).toHaveProperty('jobId');
    expectTypeOf(body.jobId).toBeString();
    expect(body).toHaveProperty('graphId');
    expectTypeOf(body.graphId).toBeString();

    // check that both are numeric values (but strings...)
    expect(parseInt(body.jobId)).not.toBeNaN();
    expect(parseInt(body.graphId)).not.toBeNaN();
    const graphId = body.graphId;

    // wait for the job to be finished
    const jobResponse = await gral.waitForJobToBeFinished(gralEndpoint, jwt, body.jobId);
    expect(jobResponse).toBeInstanceOf(Object);
    expect(jobResponse).toHaveProperty('result');
    expect(jobResponse.result).toBeInstanceOf(Object);
    expect(jobResponse.result).toHaveProperty('progress');
    expect(jobResponse.result).toHaveProperty('total');
    expect(jobResponse.result).toHaveProperty('jobId');
    expect(jobResponse.result).toHaveProperty('graphId');
    expect(jobResponse.result.progress).toBe(jobResponse.result.total);
    expect(jobResponse.result.jobId).toBe(body.jobId);
    expect(jobResponse.result.graphId).toBe(body.graphId);
    graphIdForComputation = graphId;
    await verifyGraphStatus(graphId, jwt);
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

  test('run the pagerank algorithm one on of the created graphs', async () => {
    const url = gral.buildUrl(gralEndpoint, '/v1/pagerank');
    const graphAnalyticsEngineRunPageRank = {
      "graphId": graphIdForComputation,
      "maximum_supersteps": 5,
      "damping_factor": 0.85
    };
    const response = await axios.post(
      url, graphAnalyticsEngineRunPageRank, gral.buildHeaders(jwt)
    );
    expect(response.data).toBeInstanceOf(Object);
    expect(response.data).toHaveProperty('jobId');
    const jobId = response.data.jobId;
    expectTypeOf(jobId).toBeString();
    expect(jobId).not.toBe('');
    expect(jobId).not.toBeUndefined();
    const jobResponse = await gral.waitForJobToBeFinished(gralEndpoint, jwt, jobId);
    expect(jobResponse).toBeInstanceOf(Object);
    expect(jobResponse).toHaveProperty('result');
    expect(jobResponse.result).toBeInstanceOf(Object);
    const expectedProperties = ['progress', 'total', 'jobId', 'graphId', 'compType', 'progress'];
    expectedProperties.forEach((property) => {
      expect(jobResponse.result).toHaveProperty(property);
    });
    expect(jobResponse.result.progress).toBe(jobResponse.result.total);
    expect(jobResponse.result.progress).toBe(100);
    expect(jobResponse.result.jobId).toBe(jobId);
    expect(jobResponse.result.graphId).toBe(graphIdForComputation);
    expect(jobResponse.result.compType).toBe('pagerank');
    expectTypeOf(jobResponse.result.memoryUsage).toBeString();
    const memoryUsage = parseInt(jobResponse.result.memoryUsage);
    expect(memoryUsage).toBeGreaterThan(0);
  });

});
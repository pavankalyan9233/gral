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

const verifyGraphStatus = async (graph_id: string, jwt: string) => {
  // precondition
  expect(graph_id).not.toBeUndefined();
  expect(graph_id).not.toBe('');

  const url = gral.buildUrl(gralEndpoint, `/v1/graphs/${graph_id}`);
  const response = await axios.get(url, gral.buildHeaders(jwt));
  expect(response.status).toBe(200);
  expect(response.data).toBeInstanceOf(Object);
  const body = response.data;
  expect(body).toHaveProperty('graph');
  expectTypeOf(body.graph).toBeObject();
  expect(body.graph).toHaveProperty('graph_id');
  expectTypeOf(body.graph.graph_id).toBeString();
  const graph = body.graph;
  expect(graph.graph_id).toBe(graph_id);

  expect(graph).toHaveProperty('number_of_vertices');
  expectTypeOf(graph.number_of_vertices).toBeString();
  const number_of_vertices = parseInt(graph.number_of_vertices);
  expect(number_of_vertices).toBeGreaterThan(0);
  expect(number_of_vertices).toBe(2394385);

  expect(graph).toHaveProperty('number_of_edges');
  expectTypeOf(graph.number_of_edges).toBeString();
  const number_of_edges = parseInt(graph.number_of_edges);
  expect(number_of_edges).toBeGreaterThan(0);
  expect(number_of_edges).toBe(5021410);

  expect(graph).toHaveProperty('memory_per_vertex');
  expectTypeOf(graph.memory_per_vertex).toBeString();
  const memory_per_vertex = parseInt(graph.memory_per_vertex);
  expect(memory_per_vertex).toBeGreaterThan(0);

  expect(graph).toHaveProperty('memory_per_edge');
  expectTypeOf(graph.memory_per_edge).toBeString();
  const memory_per_edge = parseInt(graph.memory_per_edge);
  expect(memory_per_edge).toBeGreaterThan(0);

  expect(graph).toHaveProperty('memory_usage');
  expectTypeOf(graph.memory_usage).toBeString();
  const memory_usage = parseInt(graph.memory_usage);
  expect(memory_usage).toBeGreaterThan(0);
  const expected_memory_usage = (number_of_vertices * memory_per_vertex) + (number_of_edges * memory_per_edge);
  // we cannot expect the exact value, as the amount of memory per vertex or edge might is divided by the number of
  // vertices or edges, respectively, and the division might not be exact
  const closenessResult = isClose(memory_usage, expected_memory_usage, 0.05);
  expect(closenessResult).toBeTruthy();
};

describe.sequential('API tests based on wiki-Talk graph dataset', () => {

  let jwt: string;
  let graph_idForComputation: string;

  beforeAll(async () => {
    jwt = await arangodb.getArangoJWT();
    expect(jwt).not.toBe('');
    expect(jwt).not.toBeUndefined();
  }, config.test_configuration.medium_timeout);

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
    expect(parseInt(body.job_id)).not.toBeNaN();
    expect(parseInt(body.graph_id)).not.toBeNaN();
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
    expect(body).toHaveProperty('job_id');
    expectTypeOf(body.job_id).toBeString();
    expect(body).toHaveProperty('graph_id');
    expectTypeOf(body.graph_id).toBeString();

    // check that both are numeric values (but strings...)
    expect(parseInt(body.job_id)).not.toBeNaN();
    expect(parseInt(body.graph_id)).not.toBeNaN();
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
    graph_idForComputation = graph_id;
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
      "graph_id": graph_idForComputation,
      "maximum_supersteps": 5,
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
    expect(jobResponse.result.graph_id).toBe(graph_idForComputation);
    expect(jobResponse.result.comp_type).toBe('pagerank');
    expectTypeOf(jobResponse.result.memory_usage).toBeString();
    const memory_usage = parseInt(jobResponse.result.memory_usage);
    expect(memory_usage).toBeGreaterThan(0);
  });

});
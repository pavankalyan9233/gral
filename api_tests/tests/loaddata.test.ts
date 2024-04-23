import {beforeAll, describe, expect, expectTypeOf, test} from 'vitest';
import {config} from '../environment.config';
import {arangodb} from '../helpers/arangodb';
import {gral} from '../helpers/gral';
import axios from 'axios';

const gralEndpoint = config.gral_instances.arangodb_auth;

describe('API tests based on wiki-Talk graph dataset', () => {

  let jwt: string;

  beforeAll(async () => {
    jwt = await arangodb.getArangoJWT();
    expect(jwt).not.toBe('');
    expect(jwt).not.toBeUndefined();
  }, config.test_configuration.medium_timeout);

  // TODO: Add test for: a non-existing vertex collection and
  // TODO: Add test for: a non-existing edge collection


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

  test('load graph with graph_name that does not exist in the database', async () => {
    const url = gral.buildUrl(gralEndpoint, '/v1/loaddata');
    const graphAnalyticsEngineLoadDataRequest = {
      "database": "_system",
      "graph_name": "doesNotExist"
    };

    const response = await axios.post(
      url, graphAnalyticsEngineLoadDataRequest, gral.buildHeaders(jwt)
    );
    const body = response.data;

    try {
      await gral.waitForJobToBeFinished(gralEndpoint, jwt, body.jobId);
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect(error.message).toContain("graph 'doesNotExist' not found");
    }
  });

  test('load graph without any graph_name or vertex or edge collections given', async () => {
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
      await gral.waitForJobToBeFinished(gralEndpoint, jwt, body.jobId);
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect(error.message).toContain('Either specify the graph_name or ensure that vertex_collections and edge_collections are not empty.');
    }
  });

});
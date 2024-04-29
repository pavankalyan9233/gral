import {beforeAll, describe, expect, test} from 'vitest';
import {config} from '../environment.config';
import {arangodb} from '../helpers/arangodb';
import {gral} from '../helpers/gral';
import axios from 'axios';
import {graphGenerator} from "../helpers/graphGenerator";

const gralEndpoint = config.gral_instances.arangodb_auth;
describe('Python integration', () => {

  let jwt: string;

  beforeAll(async () => {
    jwt = await arangodb.getArangoJWT();
    expect(jwt).not.toBe('');
    expect(jwt).not.toBeUndefined();

    // generate a complete graph for testing
    await graphGenerator.generateCompleteGraph(5, 'complete_graph_5');
  }, config.test_configuration.medium_timeout);

  test('Full execution (custom pagerank) including results', async () => {
    const url = gral.buildUrl(gralEndpoint, '/v1/loaddata');
    const postBody = {
      database: '_system',
      vertex_collections: ['complete_graph_5_v'],
      edge_collections: ['complete_graph_5_e'],
    }
    const response = await axios.post(url, postBody, gral.buildHeaders(jwt));
    expect(response.status).toBe(200);
    const body = response.data;
    const graph_id = body.graph_id;
    const job_id = body.job_id;

    await gral.waitForJobToBeFinished(gralEndpoint, jwt, job_id);

    let pythonUrl = gral.buildUrl(gralEndpoint, '/v1/python');
    const pythonPostBody = {
      "graph_id": graph_id,
      "function": "def worker(graph): return nx.pagerank(graph, 0.85)"
    };
    const pythonResponse = await axios.post(pythonUrl, pythonPostBody, gral.buildHeaders(jwt));
    // Verify initial python request
    expect(pythonResponse.status).toBe(200);
    expect(pythonResponse.data.job_id).not.toBeUndefined();
    expect(pythonResponse.data.job_id).toBeTypeOf('number');
    expect(pythonResponse.data.job_id).toBeGreaterThan(0);
    expect(pythonResponse.data.error_code).toBeTypeOf('number');
    expect(pythonResponse.data.error_code).toBe(0);
    expect(pythonResponse.data.error_message).toBe('');

    const pythonResponseJobId = pythonResponse.data.job_id;
    const pythonJobResult = await gral.waitForJobToBeFinished(gralEndpoint, jwt, pythonResponseJobId);

    // Verify the jobs response of the python computation
    expect(pythonJobResult).toHaveProperty('result');
    const pythonComputationResult = pythonJobResult.result;
    const expectedProperties = [
      'job_id', 'graph_id', 'total', 'progress', 'error_code', 'error_message', 'memory_usage', 'comp_type'
    ];
    for (const property of expectedProperties) {
      expect(pythonComputationResult).toHaveProperty(property);
    }
    expect(pythonComputationResult.error_code).toBe(0);
    expect(pythonComputationResult.error_message).toBe('');
    expect(pythonComputationResult.total).toEqual(pythonComputationResult.progress);
    expect(pythonComputationResult.memory_usage).toBeTypeOf('number');
    expect(pythonComputationResult.memory_usage).toBeGreaterThan(0);
    expect(pythonComputationResult.comp_type).toBe('Custom');

    // Store the computation results into the database
    // create collection named results in arangodb
    const resultCollection = await arangodb.createDocumentCollection('results');
    const comp_id = pythonComputationResult.job_id;

    const storeResultRequestBody = {
      "job_ids": [comp_id],
      "attribute_names": ["iResult"],
      "database": "_system",
      "target_collection": "results",
    };
    const storeResultsUrl = gral.buildUrl(gralEndpoint, '/v1/storeresults');
    const storeResultsResponse = await axios.post(
      storeResultsUrl, storeResultRequestBody, gral.buildHeaders(jwt)
    );
    expect(storeResultsResponse.status).toBe(200);
    expect(storeResultsResponse.data.error_code).toBe(0);
    expect(storeResultsResponse.data.error_message).toBe('');
    expect(storeResultsResponse.data.job_id).toBeTypeOf('number');
    expect(storeResultsResponse.data.job_id).toBeGreaterThan(0);


    const storeResultsJobId = storeResultsResponse.data.job_id;
    const storeJobResponse = await gral.waitForJobToBeFinished(gralEndpoint, jwt, storeResultsJobId);
    const storeJobResult = storeJobResponse.result;
    expect(storeJobResult.error_code).toBe(0);
    expect(storeJobResult.error_message).toBe('');
    expect(storeJobResult.comp_type).toBe('Store Operation');

    const collectionProps = await resultCollection.count();
    expect(collectionProps.count).toBe(5);

    const db = arangodb.getArangoJSDatabaseInstance();
    const docs = await db.query(`
      FOR doc IN ${resultCollection.name}
      RETURN doc
    `);

    await docs.forEach((doc: any) => {
      expect(doc).toHaveProperty('iResult');
      expect(doc.iResult).toBeTypeOf('number');
      expect(doc.iResult).toBe(0.2);
    });
  }, config.test_configuration.medium_timeout);

  test('Invalid function input for script', async () => {
    const url = gral.buildUrl(gralEndpoint, '/v1/loaddata');
    const postBody = {
      database: '_system',
      vertex_collections: ['complete_graph_5_v'],
      edge_collections: ['complete_graph_5_e'],
    }
    const response = await axios.post(url, postBody, gral.buildHeaders(jwt));
    const body = response.data;
    const graph_id = body.graph_id;
    const job_id = body.job_id;

    await gral.waitForJobToBeFinished(gralEndpoint, jwt, job_id);

    let pythonUrl = gral.buildUrl(gralEndpoint, '/v1/python');

    const pythonFunctionsToTest = [
      ";'./,234180", // invalid python code
      "def worker(graph): return undefinedMethod()", // call an undefined method
      "def worker(graph): print(123)", // do not return anything (dict expected)
      "def worker(graph): return 123", // return a number (dict expected)
      "def worker(graph): return '123'", // return a string (dict expected)
      "def worker(graph): return true", // return true (dict expected)
      "def worker(graph): return false", // return false (dict expected)
      "def worker(graph): return None", // return None (dict expected)
    ];

    for (const pythonFunction of pythonFunctionsToTest) {
      const pythonPostBody = {
        "graph_id": graph_id,
        "function": pythonFunction
      };
      const pythonResponse = await axios.post(pythonUrl, pythonPostBody, gral.buildHeaders(jwt));
      expect(pythonResponse.status).toBe(200);
      expect(pythonResponse.data.job_id).toBeTypeOf('number');
      expect(pythonResponse.data.job_id).toBeGreaterThan(0);
      expect(pythonResponse.data.error_code).toBeTypeOf('number');
      expect(pythonResponse.data.error_code).toBe(0);
      expect(pythonResponse.data.error_message).toBe('');

      const pythonResponseJobId = pythonResponse.data.job_id;
      try {
        await gral.waitForJobToBeFinished(gralEndpoint, jwt, pythonResponseJobId);
      } catch (error) {
        expect(error.message).toContain('Failed to execute Python script');
      }
    }
  }, config.test_configuration.medium_timeout);
});
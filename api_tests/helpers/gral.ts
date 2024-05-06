import {config} from '../environment.config';
import axios from "axios";
import {expect} from "vitest";

function buildUrl(endpoint: string, path: string) {
  if (endpoint !== config.gral_instances.arangodb_auth && endpoint !== config.gral_instances.service_auth && endpoint !== config.gral_instances.service_auth_unreachable) {
    throw new Error('Endpoint must be one of the gral_instances defined in environment.config.ts');
  }

  if (path[0] !== '/') {
    throw new Error('Path must start with a "/" character.');
  }
  return `${endpoint}${path}`;
}

function buildHeaders(jwt: string) {
  return {
    headers: {
      'Authorization': `Bearer ${jwt}`
    }
  };
}

async function waitForJobToBeFinished(endpoint: string, jwt: string, jobId: string) {
  const url = buildUrl(endpoint, `/v1/jobs/${jobId}`);

  let retries = 0;

  // While this is a `while` loop, the test framework will forcefully stop
  // the test after a certain amount of time. The default timeout is 5 seconds.
  // For longer running tests, this needs to be adjusted inside the test() definition
  // itself
  while (true) {
    try {
      const response = await axios.get(url, buildHeaders(jwt));
      const body = response.data;
      if (body !== undefined) {
        if (body.error) {
          throw new Error(`Job <${jobId}> failed: ${body.error_message}`)
        } else if (body.progress >= body.total) {
          return {result: body, retriesNeeded: retries};
        } else {
          retries++;
          await new Promise(resolve => setTimeout(resolve, 250));
        }
      } else {
        retries++;
        await new Promise(resolve => setTimeout(resolve, 250));
      }
    } catch (error) {
      throw new Error(`Job <${jobId}> did not finish in time: ${error}`);
    }
  }
}

async function shutdownInstance(endpoint: string, jwt: string) {
  return new Promise((resolve, reject) => {
    const url = buildUrl(endpoint, '/v1/shutdown');
    axios.delete(url, buildHeaders(jwt)).then((response) => {
      resolve(response);
    }).catch((error) => {
      reject(error);
    });
  });
}

async function storeComputationResult(
  job_id: string, databaseName: string = '_system',
  targetCollectionName: string,
  attributeName: string,
  jwt: string,
  gralEndpoint: string) {
  const storeResultRequestBody = {
    "job_ids": [job_id],
    "attribute_names": [attributeName],
    "database": databaseName,
    "target_collection": targetCollectionName,
  };

  let storeResultsResponse;
  const storeResultsUrl = gral.buildUrl(gralEndpoint, '/v1/storeresults');
  try {
    storeResultsResponse = await axios.post(
      storeResultsUrl, storeResultRequestBody, gral.buildHeaders(jwt)
    );
  } catch (error) {
    console.log(error);
  }

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
}

async function loadGraph(
  jwt: string,
  gralEndpoint: string,
  graphName: string,
  vertexCollections: string[] = [],
  edgeCollections: string[] = []) {
  const url = buildUrl(gralEndpoint, '/v1/loaddata');
  const graphAnalyticsEngineLoadDataRequest = {
    "database": config.arangodb.database,
    "graph_name": graphName,
    "vertex_collections": vertexCollections,
    "edge_collections": edgeCollections
  };

  const response = await axios.post(
    url, graphAnalyticsEngineLoadDataRequest, buildHeaders(jwt)
  );
  const body = response.data;

  try {
    return await waitForJobToBeFinished(gralEndpoint, jwt, body.job_id);
  } catch (error) {
    throw error;
  }
}

async function runPagerank(jwt: string, gralEndpoint: string, graphId: number, maxSupersteps: number = 10, dampingFactor: number = 0.85) {
  const url = buildUrl(gralEndpoint, '/v1/pagerank');
  const pagerankRequest = {
    "graph_id": graphId,
    "maximum_supersteps": maxSupersteps,
    "damping_factor": dampingFactor
  };

  const response = await axios.post(
    url, pagerankRequest, buildHeaders(jwt)
  );
  const body = response.data;

  try {
    return await waitForJobToBeFinished(gralEndpoint, jwt, body.job_id);
  } catch (error) {
    throw error;
  }
}

async function runPythonPagerank(jwt: string, gralEndpoint: string, graphId: number,
                                 maxSupersteps: number = 10,
                                 dampingFactor: number = 0.85) {
  const url = buildUrl(gralEndpoint, '/v1/python');
  const algorithmRequest = {
    "graph_id": graphId,
    "function": `def worker(graph): return nx.pagerank(G = graph, alpha=${dampingFactor}, max_iter=${maxSupersteps})`
  };

  const response = await axios.post(
    url, algorithmRequest, buildHeaders(jwt)
  );
  const body = response.data;

  try {
    return await waitForJobToBeFinished(gralEndpoint, jwt, body.job_id);
  } catch (error) {
    throw error;
  }
}

async function runWCC(jwt: string, gralEndpoint: string, graphId: number, customFields: object = {}) {
  const url = buildUrl(gralEndpoint, '/v1/wcc');
  const wccRequest = {
    "graph_id": graphId,
    "custom_fields": customFields
  };

  const response = await axios.post(
    url, wccRequest, buildHeaders(jwt)
  );
  const body = response.data;

  try {
    return await waitForJobToBeFinished(gralEndpoint, jwt, body.job_id);
  } catch (error) {
    throw error;
  }
}

export const gral = {
  buildUrl,
  buildHeaders,
  storeComputationResult,
  shutdownInstance,
  waitForJobToBeFinished,
  loadGraph,
  runPagerank,
  runPythonPagerank,
  runWCC
};

module.exports = gral;
import {config} from '../environment.config';
import axios from "axios";
import {strict as assert} from 'assert';

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

async function sendDeleteJobRequest(jwt: string, gralEndpoint: string, jobId: string) {
  const url = buildUrl(gralEndpoint, `/v1/jobs/${jobId}`);
  axios.delete(url, buildHeaders(jwt));
}

async function waitForJobToBeFinished(endpoint: string, jwt: string, jobId: string, refetchInterval: number = 250) {
  const url = buildUrl(endpoint, `/v1/jobs/${jobId}`);

  let retries = 0;

  // While this is a `while` loop, the test framework will forcefully stop
  // the test after a certain amount of time. The default timeout is 5 seconds.
  // For longer running tests, this needs to be adjusted inside the test() definition
  // itself

  // eslint-disable-next-line no-constant-condition
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
          await new Promise(resolve => setTimeout(resolve, refetchInterval));
        }
      } else {
        retries++;
        await new Promise(resolve => setTimeout(resolve, refetchInterval));
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

  assert(storeResultsResponse.status === 200);
  assert(storeResultsResponse.data.error_code === 0);
  assert(storeResultsResponse.data.error_message === '');
  assert(typeof storeResultsResponse.data.job_id === 'number')
  assert(storeResultsResponse.data.job_id > 0);

  const storeResultsJobId = storeResultsResponse.data.job_id;
  const storeJobResponse = await gral.waitForJobToBeFinished(gralEndpoint, jwt, storeResultsJobId);
  const storeJobResult = storeJobResponse.result;
  assert(storeJobResult.error_code === 0);
  assert(storeJobResult.error_message === '');
  assert(storeJobResult.comp_type === 'Store Operation')
}

async function loadGraph(
  jwt: string,
  gralEndpoint: string,
  graphName: string,
  vertexCollections: string[] = [],
  edgeCollections: string[] = [], vertexAttributes: string[] = [], refetchInterval: number = 250) {
  const url = buildUrl(gralEndpoint, '/v1/loaddata');
  const graphAnalyticsEngineLoadDataRequest = {
    "database": config.arangodb.database,
    "graph_name": graphName,
    "vertex_collections": vertexCollections,
    "edge_collections": edgeCollections,
    "vertex_attributes": vertexAttributes
  };

  const response = await axios.post(
    url, graphAnalyticsEngineLoadDataRequest, buildHeaders(jwt)
  );
  const body = response.data;
  const graphId = body.graph_id;

  await waitForJobToBeFinished(gralEndpoint, jwt, body.job_id, refetchInterval);
  // HACK right now as the loaddata job returns a "faked" graph id and not the correct one.
  return graphId;
}

async function dropGraph(
  jwt: string,
  gralEndpoint: string,
  graphId: number,
  refetchInterval: number = 250
) {
  const url = buildUrl(gralEndpoint, `/v1/graphs/${graphId}`);
  const response = await axios.delete(
    url, buildHeaders(jwt)
  );
  assert(response.status === 200);

  // TODO: DELETE does not return a job id, but documented like this.
  // const body = response.data;
  // return await waitForJobToBeFinished(gralEndpoint, jwt, body.job_id, refetchInterval);
}

async function runIRank(jwt: string, gralEndpoint: string, graphId: number, maxSupersteps: number = 10, dampingFactor: number = 0.85) {
  const url = buildUrl(gralEndpoint, '/v1/irank');
  const iRankRequest = {
    "graph_id": graphId,
    "maximum_supersteps": maxSupersteps,
    "damping_factor": dampingFactor
  };

  let body;
  try {
    const response = await axios.post(
      url, iRankRequest, buildHeaders(jwt)
    );
    body = response.data;
  } catch (error) {
    console.log(error);
  }

  return await waitForJobToBeFinished(gralEndpoint, jwt, body.job_id);
}

async function runPagerank(jwt: string, gralEndpoint: string, graphId: number, maxSupersteps: number = 10, dampingFactor: number = 0.85, deleteJob: boolean = true) {
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

  let jobResponse = await waitForJobToBeFinished(gralEndpoint, jwt, body.job_id);
  if (deleteJob) {
    // required as job currently holds mutex to the instantiated graph itself
    // this will free used memory
    const job_id = jobResponse.result.job_id;
    sendDeleteJobRequest(jwt, gralEndpoint, job_id);
  }
  return jobResponse;
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

  return await waitForJobToBeFinished(gralEndpoint, jwt, body.job_id);
}

async function runWCC(jwt: string, gralEndpoint: string, graphId: number, customFields: object = {}, deleteJob: boolean = true) {
  const url = buildUrl(gralEndpoint, '/v1/wcc');
  const wccRequest = {
    "graph_id": graphId,
    "custom_fields": customFields
  };

  const response = await axios.post(
    url, wccRequest, buildHeaders(jwt)
  );
  const body = response.data;

  const jobResponse = await waitForJobToBeFinished(gralEndpoint, jwt, body.job_id);
  if (deleteJob) {
    // required as job currently holds mutex to the instantiated graph itself
    // this will free used memory
    const job_id = jobResponse.result.job_id;
    sendDeleteJobRequest(jwt, gralEndpoint, job_id);
  }
  return jobResponse;
}

async function runSCC(jwt: string, gralEndpoint: string, graphId: number, customFields: object = {}, deleteJob: boolean = true) {
  const url = buildUrl(gralEndpoint, '/v1/scc');
  const wccRequest = {
    "graph_id": graphId,
    "custom_fields": customFields
  };

  const response = await axios.post(
    url, wccRequest, buildHeaders(jwt)
  );
  const body = response.data;

  const jobResponse = await waitForJobToBeFinished(gralEndpoint, jwt, body.job_id);
  if (deleteJob) {
    // required as job currently holds mutex to the instantiated graph itself
    // this will free used memory
    const job_id = jobResponse.result.job_id;
    sendDeleteJobRequest(jwt, gralEndpoint, job_id);
  }
  return jobResponse;
}

async function runCDLP(jwt: string, gralEndpoint: string, graphId: number, startLabelAttribute: string = "", deleteJob: boolean = true) {
  const url = buildUrl(gralEndpoint, '/v1/labelpropagation');
  const cdlpRequest = {
    "graph_id": graphId,
    "start_label_attribute": startLabelAttribute,
    "synchronous": true,
    "maximum_supersteps": 10,
    "random_tiebreak": false,
  };

  const response = await axios.post(
    url, cdlpRequest, buildHeaders(jwt)
  );
  const body = response.data;

  const jobResponse = await waitForJobToBeFinished(gralEndpoint, jwt, body.job_id);
  if (deleteJob) {
    // required as job currently holds mutex to the instantiated graph itself
    // this will free used memory
    const job_id = jobResponse.result.job_id;
    sendDeleteJobRequest(jwt, gralEndpoint, job_id);
  }
  return jobResponse;
}

export const gral = {
  buildUrl,
  buildHeaders,
  storeComputationResult,
  shutdownInstance,
  waitForJobToBeFinished,
  loadGraph,
  dropGraph,
  runIRank,
  runPagerank,
  runPythonPagerank,
  runWCC,
  runSCC,
  runCDLP
};

module.exports = gral;

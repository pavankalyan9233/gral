import {config} from '../environment.config';
import axios from "axios";

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
          throw new Error(`Job <${jobId}> failed: ${body.errorMessage}`)
        } else if (body.progress >= body.total) {
          return {result: body, retriesNeeded: retries};
        } else {
          retries++;
          await new Promise(resolve => setTimeout(resolve, 1000));
        }
      } else {
        retries++;
        await new Promise(resolve => setTimeout(resolve, 1000));
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

export const gral = {
  buildUrl, buildHeaders, shutdownInstance, waitForJobToBeFinished
};

module.exports = gral;
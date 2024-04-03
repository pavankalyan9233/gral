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

async function shutdownInstance(endpoint: string, jwt: string) {
  return new Promise((resolve, reject) => {
    const url = buildUrl(endpoint, '/v1/shutdown');
    axios.delete(url, buildHeaders(jwt)).then((response) => {
      console.log(response);
      resolve(response);
    }).catch((error) => {
      console.log(error);
      reject(error);
    });
  });
}

export const gral = {
  buildUrl, buildHeaders, shutdownInstance
};

module.exports = gral;
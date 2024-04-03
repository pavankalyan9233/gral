import {config} from '../environment.config';

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

export const gral = {
  buildUrl, buildHeaders
};

module.exports = gral;
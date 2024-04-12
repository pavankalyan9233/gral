const axios = require('axios');
import {config} from '../environment.config';

function buildArangoDBUrl(path: string) {
  if (path[0] !== '/') {
    throw new Error('Path must start with a "/" character.');
  }
  return `${config.arangodb.endpoint}${path}`;

}

async function getArangoJWT(maxRetries: number = 1) {
  let retries = 0;

  while (retries < maxRetries) {
    try {
      const response = await axios.post(buildArangoDBUrl('/_open/auth'), {
        username: config.arangodb.username,
        password: config.arangodb.password
      });
      return response.data['jwt'];
    } catch (error) {
      retries++;
      console.log('ArangoDB not ready, retrying...');
      await new Promise(resolve => setTimeout(resolve, 1000));
      if (retries === maxRetries) {
        throw new Error(`Failed to get JWT after ${maxRetries} attempts.`);
      }
    }
  }
}

export const arangodb = {
  getArangoJWT
};


module.exports = arangodb;
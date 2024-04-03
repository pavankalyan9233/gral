const axios = require('axios');
import {config} from '../environment.config';

function buildArangoDBUrl(path: string) {
  if (path[0] !== '/') {
    throw new Error('Path must start with a "/" character.');
  }
  return `${config.arangodb.endpoint}${path}`;

}

async function getArangoJWT() {
  const response = await axios.post(buildArangoDBUrl('/_open/auth'), {
    username: config.arangodb.username,
    password: config.arangodb.password
  });

  return response.data['jwt'];
}

export const arangodb = {
  getArangoJWT
};


module.exports = arangodb;
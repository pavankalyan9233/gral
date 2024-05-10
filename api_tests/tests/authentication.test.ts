import {beforeAll, describe, expect, test} from 'vitest';
import {config} from '../environment.config';
import {arangodb} from '../helpers/arangodb';
import {gral} from '../helpers/gral';
import axios from 'axios';

const GRAL_VALID_AUTH_ENDPOINTS = [config.gral_instances.arangodb_auth, config.gral_instances.service_auth];
const GRAL_INVALID_AUTH_ENDPOINTS = [config.gral_instances.service_auth_unreachable];
describe.concurrent('Authentication tests', () => {
  describe.concurrent('With valid JWT token', () => {
    let jwt: string;

    beforeAll(async () => {
      jwt = await arangodb.getArangoJWT();
      expect(jwt).not.toBe('');
      expect(jwt).not.toBeUndefined();
    }, config.test_configuration.medium_timeout);

    test('JWT token should be generated via call to ArangoDB itself', () => {
      expect(jwt).not.toBe('');
    });

    test('GET /v1/graphs ', async () => {
      for (const endpoint of GRAL_VALID_AUTH_ENDPOINTS) {
        const url = gral.buildUrl(endpoint, '/v1/graphs');
        const response = await axios.get(url, gral.buildHeaders(jwt));
        expect(response.status).toBe(200);
        expect(response.data).toBeInstanceOf(Array);
      }
    });
  });

  describe.concurrent('With an invalid JWT token', () => {
    const jwt: string = 'invalid';

    test('GET /v1/graphs ', async () => {
      for (const endpoint of [...GRAL_VALID_AUTH_ENDPOINTS, ...GRAL_INVALID_AUTH_ENDPOINTS]) {
        const url = gral.buildUrl(endpoint, '/v1/graphs');
        await axios.get(url, gral.buildHeaders(jwt)).catch((error) => {
          expect(error.response.status).toBe(401);
        });
      }
    });
  });
});
import {beforeAll, describe, expect, expectTypeOf, test} from 'vitest';
import {config} from '../environment.config';
import {arangodb} from '../helpers/arangodb';
import {gral} from '../helpers/gral';
import axios from 'axios';

const gral_valid_auth_endpoints = [config.gral_instances.arangodb_auth, config.gral_instances.service_auth];
const gral_invalid_auth_endpoints = [config.gral_instances.service_auth_unreachable];

describe('Authentication tests with ArangoDB auth configured', () => {
  let jwt: String;

  beforeAll(async () => {
    jwt = await arangodb.getArangoJWT();
  }, config.test_configuration.timeout);

  test('JWT token should be generated via call to ArangoDB itself', () => {
    expect(jwt).not.toBe('');
  });

  describe('Requests with valid JWT token', () => {
    test('GET /v1/graphs ', () => {
      for (let endpoint of gral_valid_auth_endpoints) {
        let url = gral.buildUrl(endpoint, '/v1/graphs');
        axios.get(url, gral.buildHeaders(jwt)).then((response) => {
          expect(response.status).toBe(200);
          expect(response.data).toBeInstanceOf(Array);
        });
      }
    });

    describe('Requests with an invalid JWT token', () => {
      test('GET /v1/graphs ', () => {
        for (let endpoint of [...gral_valid_auth_endpoints, ...gral_invalid_auth_endpoints]) {
          let url = gral.buildUrl(endpoint, '/v1/graphs');
          axios.get(url, gral.buildHeaders('invalid')).catch((error) => {
            expect(error.response.status).toBe(401);
          });
        }
      });
    });

  });
});
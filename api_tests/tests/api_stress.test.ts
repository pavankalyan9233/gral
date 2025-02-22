import {expect, describe, test, beforeAll} from 'vitest';
import {loadSync} from "@grpc/proto-loader";
import {ChannelCredentials, loadPackageDefinition} from '@grpc/grpc-js';
import {gral} from "../helpers/gral";
import axios from "axios";
import {arangodb} from "../helpers/arangodb";
import {config} from "../environment.config";

const AMOUNT_OF_REQUESTS = 1000;
const TEST_TIMEOUT = 10000;

// Currently this Suite is skipped by default.
// Reason: It is not recommended to run this test in a CI/CD pipeline right now.
// I've struggled to find a way to select proper constants for the test to run in a reasonable time.
// I still want to keep this file as it has been helpful in the past to test the service under high load.
// Additionally, this test file demonstrates how to use GRPC to communicate with the service in JavaScript.
describe.skip('API Stress Test', () => {
  let jwt: string;

  beforeAll(async () => {
    jwt = await arangodb.getArangoJWT(3);
  }, config.test_configuration.medium_timeout);

  test('Test high requests against service api', async () => {
    const endpoint = 'localhost:9092';
    const packageDefinition = loadSync('../proto/definition.proto');
    const notesProto = loadPackageDefinition(packageDefinition);

    const createToken = async () => {
      const client = new notesProto.authentication.AuthenticationV1(endpoint, ChannelCredentials.createInsecure());
      const createTokenRequest = {
        user: "root"
      };

      return new Promise((resolve, reject) => {
        client.CreateToken(createTokenRequest, (error, response) => {
          if (error) {
            reject(error);
          } else {
            resolve(response);
          }
        });
      });

    };

    const tokenPromises = [];
    for (let i = 0; i < AMOUNT_OF_REQUESTS; i++) {
      tokenPromises.push(await createToken());
    }

    Promise.all(tokenPromises)
      .then((tokens) => {
        expect(tokens.length).toBe(AMOUNT_OF_REQUESTS);
      })
      .catch((error) => {
        // throw error
        throw error;
      });
  }, TEST_TIMEOUT);

  test('Test random chosen gral endpoint that will communicate with the auth service behind', () => {
    const endpoint = 'http://localhost:1337';
    const url = gral.buildUrl(endpoint, '/v1/graphs');
    const promises = [];

    for (let i = 0; i < AMOUNT_OF_REQUESTS; i++) {
      promises.push(
        axios.get(url, gral.buildHeaders(jwt)).then((response) => {
          expect(response.status).toBe(200);
          expect(response.data).toBeInstanceOf(Array);
        })
      );
    }

    Promise.all(promises)
      .then((responses) => {
        expect(responses.length).toBe(AMOUNT_OF_REQUESTS);
      })
      .catch((error) => {
        throw error;
      });
  }, TEST_TIMEOUT);
});

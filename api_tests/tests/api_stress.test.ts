import {expect, describe, test, beforeAll} from 'vitest';
import {loadSync} from "@grpc/proto-loader";
import {ChannelCredentials, loadPackageDefinition} from '@grpc/grpc-js';
import {gral} from "../helpers/gral";
import axios from "axios";
import {arangodb} from "../helpers/arangodb";
import {config} from "../environment.config";

const AMOUNT_OF_REQUESTS = 100;
const TEST_TIMEOUT = 15000;

describe('API Stress Test', () => {
  let jwt: String;

  beforeAll(async () => {
    jwt = await arangodb.getArangoJWT(3);
  }, config.test_configuration.timeout);

  test('Test high requests against service api', async () => {
    const endpoint = 'localhost:9092';
    const packageDefinition = loadSync('../proto/definition.proto');
    const notesProto = loadPackageDefinition(packageDefinition);

    const createToken = async () => {
      try {
        //const credentials
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
      } catch (error) {
        throw error;
      }
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
    let url = gral.buildUrl(endpoint, '/v1/graphs');

    let promises = [];

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
        console.error('An error occurred:', error);
      });
  }, TEST_TIMEOUT);
});

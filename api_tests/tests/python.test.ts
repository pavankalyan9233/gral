import {beforeAll, describe, expect, test} from 'vitest';
import {config} from '../environment.config';
import {arangodb} from '../helpers/arangodb';
import {gral} from '../helpers/gral';
import axios from 'axios';
import {graphGenerator} from "../helpers/graphGenerator";

const gral_endpoint = config.gral_instances.arangodb_auth;
describe('Python integration', () => {

  let jwt: string;

  beforeAll(async () => {
    jwt = await arangodb.getArangoJWT();
    expect(jwt).not.toBe('');
    expect(jwt).not.toBeUndefined();

    // generate a complete graph for testing
    await graphGenerator.generateCompleteGraph(5, 'complete_graph_5');
  }, config.test_configuration.timeout);

  test('WIP: Load a graph and do a python3 based pagerank computation on it', async () => {
    // TODO: This needs to be finalized in the upcoming PR which will add an API ENDPOINT for python3 based computation
    let url = gral.buildUrl(gral_endpoint, '/v1/loaddata');
    const postBody = {
      vertex_collections: ['complete_graph_5_v'],
      edge_collections: ['complete_graph_5_e'],
    }
    const response = await axios.post(url, postBody, gral.buildHeaders(jwt));
    expect(response.status).toBe(200);
  });


});
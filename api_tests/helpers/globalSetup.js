import {gral} from "./gral";
import {config} from "../environment.config.ts";
import {arangodb} from "./arangodb";

export async function setup() {
  console.log("Starting the Integration Test Framework... Waiting for all services to be ready...");
  // we'll try to wait up to 10s for arangodb to be ready
  await arangodb.getArangoJWT(10);
}

export async function teardown() {
  const gral_valid_auth_endpoints = [
    config.gral_instances.arangodb_auth, config.gral_instances.service_auth
  ];

  const jwt = await arangodb.getArangoJWT();

  // Note: Currently, we cannot shut down instances via API which are wrongly configured and cannot reach the auth service
  // TODO: Implement a way to not execute this particular method whenever we run only `npm run test`
  for (const endpoint of gral_valid_auth_endpoints) {
    await gral.shutdownInstance(endpoint, jwt)
      .then((response) => {
        console.log(`Instance ${endpoint} Shutdown successful`);
        // Handle the response here
      })
      .catch((error) => {
        console.error(`Instance ${endpoint} Error during shutdown:`, error);
        // Handle the error here
      });
  }
}
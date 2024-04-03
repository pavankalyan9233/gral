import {gral} from "./gral";
import {config} from "../environment.config.ts";
import {arangodb} from "./arangodb";

export function setup() {
  console.log("Starting the Integration Test Framework...");
}

export async function teardown() {
  console.log("Shutting down the Integration Test Framework...");
  console.log("TODO: Shutting down all valid gral instances...");
  return;
  const gral_valid_auth_endpoints = [
    config.gral_instances.arangodb_auth, config.gral_instances.service_auth
  ];

  const jwt = await arangodb.getArangoJWT();

  for (const endpoint of gral_valid_auth_endpoints) {
    gral.shutdownInstance(endpoint, jwt)
      .then((response) => {
        console.log('Shutdown successful:', response);
        // Handle the response here
      })
      .catch((error) => {
        console.error('Error during shutdown:', error);
        // Handle the error here
      });
  }
}
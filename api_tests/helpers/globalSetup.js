import {arangodb} from "./arangodb";

export async function setup() {
  console.log("Starting the Integration Test Framework... Waiting for all services to be ready...");
  // we'll try to wait up to 10s for arangodb to be ready
  await arangodb.getArangoJWT(10);
}
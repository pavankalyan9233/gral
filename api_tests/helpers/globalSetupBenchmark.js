import {arangodb} from "./arangodb";
import {gral} from "./gral";
import {config} from "../environment.config.ts";

// This function will insert load all required benchmark related datasets
// into one running gral instance.


export async function setup() {
  async function loadBenchmarkGraphs(jwt) {
    // Here we do not take care about timings. We just want to load the graphs.
    // We can create another bench.ts file for benchmarking the loading times
    // measured than in vertices/s or edges/s.

    const benchmarkGraphs = config.benchmark.graphs;
    const endpoint = config.gral_instances.arangodb_auth;

    let responses = [];
    for (const graphName of benchmarkGraphs) {
      console.log(`Loading benchmark graph ${graphName}...`);
      responses.push(await gral.loadGraph(jwt, endpoint, graphName));
    }

    return responses;
  }

  console.log("Starting the Benchmark Framework... Waiting for all services to be ready...");
  // we'll try to wait up to 10s for arangodb to be ready
  const jwt = await arangodb.getArangoJWT(10);
  await loadBenchmarkGraphs(jwt);
}
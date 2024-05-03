export const config = {
  gral_instances: {
    arangodb_auth: "http://localhost:9999",
    service_auth: "http://localhost:1337",
    service_auth_unreachable: "http://localhost:1336",
  },
  arangodb: {
    endpoint: "http://localhost:8529",
    username: "root",
    password: "",
    database: "_system"
  },
  // The default timeout is 5000 ms (vitest default).
  // This applies when no specific timeout is set.
  test_configuration: {
    short_timeout: 7500,
    medium_timeout: 15000,
    long_timeout: 30000,
    xtra_long_timeout: 60000
  },
  benchmark: {
    // Please keep this array in sync with /api_tests/scripts/import_benchmark_datasets
    graphs: {
      "wiki-Talk": {
        algos: ["pagerank", "wcc"]
      } // we could auto-generate those properties from the `*.properties` file
      // TODO: Found a bug in our import. The assumption I had about the first dataset is wrong. Vertex IDs do not
      //  begin either with 0 or 1 and increment. This needs to be fixed first. Then we can re-enable next two datasets.
      //"dota-league",
      //"graph500-23",
    }
  }
};
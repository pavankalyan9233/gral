export const config = {
  arangodb: {
    endpoint: "http://localhost:10000",
    username: "root",
    password: "",
  },
  test_configuration: {
    timeout: 10000,
  },
  import_configuration: {
    concurrency: 20,
    max_queue_size: 1000,
  },
  neo4j: {
    endpoint: "neo4j://localhost:7687",
    username: "admin",
    password: "",
    database: "neo4j",
  }
};

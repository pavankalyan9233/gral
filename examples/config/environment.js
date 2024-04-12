export const config = {
  arangodb: {
    endpoint: "http://localhost:8529",
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
};

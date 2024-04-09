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
  },
  test_configuration: {
    timeout: 10000,
  },
  import_configuration: {
    concurrency: 20,
    max_queue_size: 1000,
  },
};

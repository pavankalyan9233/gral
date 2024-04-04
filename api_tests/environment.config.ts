export const config = {
  gral_instances: {
    arangodb_auth: "http://127.0.0.1:9999",
    service_auth: "http://127.0.0.1:1337",
    service_auth_unreachable: "http://127.0.0.1:1336",
  },
  arangodb: {
    endpoint: "http://localhost:8529",
    username: "root",
    password: "",
  },
  test_configuration: {
    timeout: 10000,
  }
};
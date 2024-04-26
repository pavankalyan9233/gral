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
    long_timeout: 30000
  }
};
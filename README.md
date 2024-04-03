# GRAL - A GRaph AnaLytics engine

This is a prototype. It strives to implement a server process `gral`
(single process, RAM only) for a graph analytics engine and implements
the API described in `design/GraphAnaphAnalyticsEngine.md` in this
repository.

TO BE CONTINUED...

## Testing

Currently, we do have unit tests and integration tests. The unit tests
are implemented in the same file as the code they test. The integration
tests are implemented in separate files in the `tests` directory.

### All Tests

To execute all tests, run the following command:

```bash
$ cargo test
```

Note: ArangoDB must be running for the integration tests to pass.
An ArangoDB Cluster is expected to be reachable via Coordinator at `http://localhost:8529`.
Also, it must be started with the same secret token as the one in the `secrets.jwt/token` file.
To make life simpler, you can just use the `docker-compose.yml` file in the root directory to start an ArangoDB Cluster.
This will also start the authentication service in a separate container.

```bash

### Unit Testing
To execute only the unit tests, run the following command:

```bash
$ cargo test --lib
```

### Integration Testing

To execute only the integration tests, run the following command:

```bash
$ cargo test --test '*'
```

Note: ArangoDB must be running for the integration tests to pass.

### API Integration Testing (Node & TypeScript based)

Tests only the accessible API endpoints of the GRAL server.

#### Preconditions

Use docker compose to start the required services:

```bash
docker compose up -d
```

#### Run the tests

To execute only the API integration tests, run the following command:

```bash
$ cd api_tests
$ npm install
$ npm test
```

> npm install only needs to be executed once. It installs the required node modules.

Please see [package.json](api_tests%2Fpackage.json) for more details.
There you find the required node version to be available on your system.
At the time of writing this README, the required node version is `>= 21`.
The required npm version is `>= 10.5.1`


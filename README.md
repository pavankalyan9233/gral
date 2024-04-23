# GRAL - A GRaph AnaLytics engine

This is a prototype. It strives to implement a server process `gral`
(single process, RAM only) for a graph analytics engine and implements
the API described in `design/GraphAnaphAnalyticsEngine.md` in this
repository.

TO BE CONTINUED...

## Testing

Currently, we do have unit tests and integration tests. The unit tests
are implemented in the same file as the code they test. The integration
tests are implemented in separate files in the `api_tests` directory.

### Unit Tests

To execute only the unit tests, run the following command:

```bash
$ cargo test
```

### Integration Tests

To execute only the integration tests, run the following command:

```bash
$ cd api_tests
$ npm install
$ npm test
```

Important: Execute tests like this, requires you to start the gral binary and an ArangoDB instance on your own.

`npm install` only needs to be executed once.

`npm run test_full` will start multiple gral instances as binaries. Also, it will use  docker-compose.yml
to start a docker container with an ArangoDB Cluster and the additional authentication service.

Additional:

In case you want to see how the integration tests are being executed, see the included `package.json` file
in the `api_tests` directory.

#### Prerequisites

Applications you need on your machine to run the integration tests:
* Docker
* Node
* NPM

> Please read [package.json](api_tests%2Fpackage.json) for more version details.
  There you find the required node version to be available on your system.
  At the time of writing this README, the required node version is `>= 21`.
  The required npm version is `>= 10.5.1`


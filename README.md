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
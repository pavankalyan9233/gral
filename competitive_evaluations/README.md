# Competitor Benchmarks

This directory / sub-project is maintaining benchmarks helpers and utilities for gral vs. competitors:
Currently we do have the following competitors:
* Gral/GAE & ArangoDB (our product)
* Neo4j (with Graph Data Science (GDS))
* networkx (Python)
* cugraph (RAPIDS)

## How to run the benchmarks

This project is not automated.
You need to run the benchmarks manually and prepare the data for the evaluation.

### Prerequisites

First, install all required modules:
```bash
$ npm install
```

Also, you need to install typescript support globally:
```bash
$ npm install -g typescript
````

Then prepare all environments you want to test, this means:
Start all services and import the data.

* Manuals:
  * ArangoDB: See `examples/README.md`
  * Neo4j: See `examples/neo4j/README.md`

### Run benchmarks

Run the benchmarks for all competitors:
```bash
$ npm run benchmark
```

Execute single file example: 
```bash
vitest bench --run --bail 1 --maxConcurrency 1 --outputJson twitter_only_three_iterations.json twitter_mpi.bench.ts
```

Execute all to custom file:
```bash
vitest bench --run --bail 1 --maxConcurrency 1 --outputJson myfilename.json
```


### Helpers

There are some helper scripts in the `scripts` directory:
* `download_examples` - downloads the data we later want to import into the services
* `start_services` - starts required services
* `import_examples` - imports the data into the services

Please feel free to either use them directly or use them as a base to deploy your desired environment.
# Benchmark 

This document summarizes the benchmark results of the Graph Analytics Engine (GAE)
(with ArangoDB as the source database) compared to Neo4j.

## Datasets (LDBC)

The dataset we used is the LDBC Social Network Benchmark (SNB) dataset.
* Source: https://ldbcouncil.org/benchmarks/graphalytics/

In detail, those are the graphs we have chosen:

| Graphs used   | Vertices   | Edges           |
| ------------- | ---------- | --------------- |
| datagen-8_0-fb| 1,706,561  | 107,507,376     |
| dota-league   | 61,170     | 50,870,313      |
| kgs           | 832,247    | 17,891,698      |
| wiki-Talk     | 2,394,385  | 5,021,410       |
| twitter_mpi   | 52,579,678 | 1,963,263,508   |

## Hardware

Machine specifications which has been used for the tests. Same for all listed executions.

* OS: Ubuntu 23.10 (mantic, 64 bit)
* MEM: 192 GB Memory - DIMM Synchronous Unbuffered (Unregistered) 4800 MHz (0.2 ns)
* CPU: Ryzen 9 7950X3D - 16 Cores, 32 Threads

## Database Configuration

This chapter describes the configuration for each database system we've configured.

### Neo4j

* Version:	5.19.0
* Edition:	Community
* Name:	    neo4j
* Deployment: On-Premise

Started with Docker:

```
docker run -d \
	--publish=7474:7474 --publish=7687:7687 \
	--user="$(id -u):$(id -g)" \
	-e NEO4J_AUTH=none --name neo4jbenchos \
	--env NEO4J_PLUGINS='["graph-data-science", "apoc"]' \
	neo4j:latest
```

### ArangoDB 

* Version: 3.12.0-NIGHTLY.20240305
* Edition: Community
* Name: arangodb
* Type: cluster
* Deployment: On-Premise
* Used shard count per collection: 3

Started with Docker via docker-compose.yml:

```
version: '3'
services:
  arangodb_cluster:
    image: arangodb/arangodb-preview:devel-nightly
    ports:
      - "8529:8529"
    environment:
      - ARANGO_ROOT_PASSWORD=""
      - ARANGO_NO_AUTH=false
    volumes:
      - ./data:/var/lib/arangodb3
      - ./secrets.jwt:/secrets
    command: arangodb --mode=cluster --local=true --auth.jwt-secret=./secrets/token
```

## Graph Analytics Engine (GAE / GRAL)

* Version: Latest (we do not have versioning here in place yet)
* Name: gral
* Type: single process (RUST based, currently no multithreaded-algorithms)
* Deployment: On-Premise

## Benchmark Configuration

We've chosen the best possible and fair setup we could come up with.
We'll explain the process for both environments in the upcoming chapter.

There are two separate workflows. 

Workflow A:
* Create the in-memory representation and to the computation once per algorithm.
* Included tested algorithms:
  * Pagerank
  * WCC
  * SCC
  * Label Propagation  
* Measure the whole process once.

Workflow B: 
* Create the in-memory representation
* Measure graph creation time
* Execute the algorithms:
  * Pagerank
  * WCC
  * SCC
  * Label Propagation
* Measure the computation time

### Used technologies

* JavaScript Framework Vitest with tinybench
* All communication to Neo4j has been established using the official Neo4j JS driver
* All communication to GAE has been established via direct requests using the node
  module axios (as we do not have an official driver here yet).

### General

All used algorithms are using the same equal parameters for execution.

Pagerank example, used properties:
```javascript
// execution ArangoDB with GAE
await gral.runPagerank(jwt, gralEndpoint, wikiTalkGraphId, 10, 0.85);

// execution Neo4J with GDS
await neo4jHelper.runPageRank(graphName, 10, 0.85);
```

### Neo4j

Cypher query to create the in-memory representation:
```
    CALL gds.graph.project(
        "${graphName}",
        ${JSON.stringify(nodeLabelsList)},
        ${JSON.stringify(relationShipList)},
        {
          nodeProperties: ${JSON.stringify(node_properties)}
        }
      )`
```

Cypher query to do the actual computation:

```
CALL gds.pageRank.stream(
      '${graphName}',
      {
        maxIterations: ${maxIterations}, 
        dampingFactor: ${dampingFactor},
        concurrency: ${AMOUNT_OF_THREADS}
      }
    )
    YIELD nodeId, score
```

Cypher query to drop an in-memory graph:
```
CALL gds.graph.drop('${graphName}')
```

Note: We only could use a value of `4` for `concurrency`, as using more is not
allowed in the community edition.

Other used cypher methods for the other algorithms:  
* `gds.wcc`
* `gds.scc`
* `gds.labelPropagation`

### ArangoDB

Used endpoints of our Graph Analytics Engine. 
The HTTP documentation is available here: 
- https://arangodb.github.io/graph-analytics/

Example of graph creation:
```javascript
const response = await axios.post(
    url, graphAnalyticsEngineLoadDataRequest, buildHeaders(jwt)
);
```

Example of pagerank execution:
```javascript
async function runPagerank(jwt: string, gralEndpoint: string, graphId: number, maxSupersteps: number = 10, dampingFactor: number = 0.85, deleteJob: boolean = true) {
    const url = buildUrl(gralEndpoint, '/v1/pagerank');
    const pagerankRequest = {
        "graph_id": graphId,
        "maximum_supersteps": maxSupersteps,
        "damping_factor": dampingFactor
    };

    const response = await axios.post(
        url, pagerankRequest, buildHeaders(jwt)
    );
    const body = response.data;

    return await waitForJobToBeFinished(gralEndpoint, jwt, body.job_id);
}
```

Example of graph deletion:
```javascript
const url = buildUrl(gralEndpoint, `/v1/graphs/${graphId}`);
const response = await axios.delete(
    url, buildHeaders(jwt)
);
```


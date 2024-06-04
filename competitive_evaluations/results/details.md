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

Same machine for Neo4j and ArangoDB incl. GAE has been used:

* OS: Ubuntu 23.10 (mantic, 64 bit)
* MEM: 192 GB Memory - DIMM Synchronous Unbuffered (Unregistered) 4800 MHz (0.2 ns)
* CPU: Ryzen 9 7950X3D - 16 Cores, 32 Threads

## Neo4j

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

## ArangoDB 

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

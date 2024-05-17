# Neo4j - Example Datasets for Testing and Benchmarking GAE

Start a neo4j instance, e.g. via docker: 
`docker run -d --publish=7474:7474 --publish=7687:7687 --user="$(id -u):$(id -g)" -e NEO4J_AUTH=none --env NEO4J_PLUGINS='["graph-data-science", "apoc"]' neo4j:latest --name neo4jbench`

Execute the data you want to import, e.g. `wiki-Talk` graph:
```bash
$ node main.js --graphName wiki-Talk --dropGraph true --mqs 20 --concurrent 10
```


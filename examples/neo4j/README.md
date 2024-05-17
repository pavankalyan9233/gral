# Neo4j - Example Datasets for Testing and Benchmarking GAE

## Start Neo4j

Start a neo4j instance, e.g. via docker: 
`docker run -d --name neo4jbench --publish=7474:7474 --publish=7687:7687 --user="$(id -u):$(id -g)" -e NEO4J_AUTH=none --env NEO4J_PLUGINS='["graph-data-science", "apoc"]' neo4j:latest`

## Importing Data into Neo4j

Execute the data you want to import, e.g. `wiki-Talk` graph:
```bash
$ node main.js --graphName wiki-Talk --dropGraph true --mqs 20 --concurrent 10
```

## Note
This code here inside this dir could be way better. But as we do not need this for CI etc. 
we should not lose too much developer time here to get it perfect. 
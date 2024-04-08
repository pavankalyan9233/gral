# Example Datasets for Testing and Benchmarking GAE

This directory contains example datasets for testing and benchmarking GAE. The datasets need to be stored
in `data` manually. Please take a look at `scripts/download-graphanalytics-data-sets-r2.sh`. If you run it
completely, you'll need approx 1.5TB of disk space. Therefore, I advise you to download the datasets manually
and choose the datasets you want to use.

The datasets are stored in the following directory structure:

```
data
├── twitter_mpi <graph name>
│      ├── twitter_mpi-BFS
│      ├── twitter_mpi-CDLP
│      ├── twitter_mpi-LCC
│      ├── twitter_mpi-PR
│      ├── twitter_mpi-WCC
│      ├── twitter_mpi.e
│      ├── twitter_mpi.properties
│      └── twitter_mpi.v
``` 

The datasets are stored in the following format:
* `*.e` - Edge list file
  *  Only two numerical values here. The first value is the source vertex and the second value is the target vertex.
  * Example row: `1 2`
* `*.v` - Vertex list file
  * Only numerical vertex IDs inside here. We'll use those numbers as a `_key` in ArangoDB.
  * Example row: `1`
* `*.properties` - Properties file
* `*-*` - Algorithm output files
  * e.g. `twitter_mpi-BFS` - This graphs supports the BFS use case algorithm.
  * e.g. `twitter_mpi-PR` - This graphs supports the PageRank use case algorithm.

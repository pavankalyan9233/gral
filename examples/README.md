# Example Datasets for Testing and Benchmarking GAE

## Usage

Download the dataset you want to use, see the [Important Data Usage Info](#important-data-usage-info) section below.

Get help text with all available options:

``` bash
npm run help
```

With `npm` you can execute all commands which are predefined in the `package.json` file.

Or alternatively, without the use of `npm`, run:

``` bash
node main.js --help
```

Example importing the graph `wiki-Talk`, drop if exists, with 500 max queue size (edges) and 50 concurrent connections:

```bash
node main.js --graphName wiki-Talk -d true --mqs 500 --con 50
```

## Recommended Parameters

Currently, the vertices insert does not have any parameters. The import for edges (edge amount usually is way bigger
than vertices), can be configured. On my test machine I've used the following parameters:
Queue size and concurrent connections: `--mqs 200 --con 20` for the `twitter_mpi` dataset.
On Hardware with 192GiB RAM and CPU: AMD Ryzen 9 7950X3D 16-Core Processor

## Important Data Usage Info

This directory contains example datasets for testing and benchmarking GAE.

For convenience, I've added another bash script which can be executed to download a single dataset and extract it
to the proper location automatically. The only thing you need to supply by yourself is the datasets name.

```bash
./scripts/downloadSingleDataset <dataset-name>
```

Example using smaller graph:

```bash
./scripts/downloadSingleDataset wiki-Talk
```

To get the full list of available datasets:
Please take a look at `scripts/download-graphanalytics-data-sets-r2.sh` or alternatively visit the website:

- https://ldbcouncil.org/benchmarks/graphalytics/

The datasets can be stored in `data` manually.
If you run it completely, you'll need approx 1.5TB of disk space. Therefore, I advise you to download the datasets
manually
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
    * Only two numerical values here. The first value is the source vertex and the second value is the target vertex.
    * Example row: `1 2`
* `*.v` - Vertex list file
    * Only numerical vertex IDs inside here. We'll use those numbers as a `_key` in ArangoDB.
    * Example row: `1`
* `*.properties` - Properties file
* `*-*` - Algorithm output files
    * e.g. `twitter_mpi-BFS` - This graphs supports the BFS use case algorithm.
    * e.g. `twitter_mpi-PR` - This graphs supports the PageRank use case algorithm.

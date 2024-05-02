#!/usr/bin/env python3
import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import networkx as nx

graph_file_path = "<Placeholder for graph_file_path>"
result_file_path = "<Placeholder for result file path>"


def read_graph_pandas(graph_file_path):
    try:
        df_edges = pd.read_parquet(graph_file_path, engine='pyarrow')
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {graph_file_path}")

    graph = nx.Graph()
    for _, row in df_edges.iterrows():
        # TODO: Currently the column names are hard-coded. Make it dynamic.
        graph.add_edge(row['_from'], row['_to'])

    return graph

def read_graph_pyarrow(graph_file_path):
    try:
        df_edges = pq.read_table(graph_file_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {graph_file_path}")

    fr = df_edges["_from"]
    to = df_edges["_to"]
    edges = [(fr[i].as_py(), to[i].as_py()) for i in range(len(fr))];
    graph = nx.Graph()
    graph.add_edges_from(edges)
    return graph

read_graph = read_graph_pyarrow

def store_computation_flexible(result):
    if isinstance(result, dict):
        df = pd.DataFrame(list(result.items()), columns=['Node', 'Result'])
        df.to_parquet(result_file_path)
    elif isinstance(result, pa.Table):
        pq.write_table(result, result_file_path)
    else:
        raise TypeError("Computation result must be a dictionary. Exiting...")

store_computation = store_computation_flexible

def worker(graph): print("<Placeholder for user injected script>");

def main():
    date_start = datetime.datetime.now()
    graph = read_graph(graph_file_path)
    date_read_graph = datetime.datetime.now()
    result = worker(graph)
    date_executed_computation = datetime.datetime.now()
    store_computation(result)
    date_stored_result = datetime.datetime.now()

    # Create tmp file under /tmp to store timings only
    with open("/tmp/timings.txt", "w") as f:
        f.write(f"Begin Python Execution: {date_start}\n")
        f.write(f"Graph Read: {date_read_graph}\n")
        f.write(f"Computation Executed: {date_executed_computation}\n")
        f.write(f"Result Stored: {date_stored_result}\n")
        f.write(f"Total Time: {date_stored_result - date_start}")


if __name__ == "__main__":
    main()

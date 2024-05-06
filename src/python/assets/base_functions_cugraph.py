#!/usr/bin/env python3
import datetime

import pandas as pd
import cudf
import cugraph

graph_file_path = "<Placeholder for graph_file_path>"
result_file_path = "<Placeholder for result file path>"


def read_graph_cugraph(graph_file_path):
    try:
        df_edges = cudf.read_parquet(graph_file_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {graph_file_path}")
    graph = cugraph.from_cudf_edgelist(df_edges, source="_from", destination="_to")
    return graph

read_graph = read_graph_cugraph

def store_computation_flexible(result):
    if isinstance(result, dict):
        df = pd.DataFrame(list(result.items()), columns=['Node', 'Result'])
        df.to_parquet(result_file_path)
    elif isinstance(result, pd.DataFrame):
        result.to_parquet(result_file_path)
    elif isinstance(result, cudf.core.dataframe.DataFrame):
        result.to_parquet(result_file_path)
    else:
        raise TypeError("Computation result must be a dictionary or data frame. Exiting...")

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

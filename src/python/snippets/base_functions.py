#!/usr/bin/env python3
import pandas as pd
import networkx as nx
graph_name = "<Placeholder graph_name>"


def worker(graph): print("<Placeholder for user injected script>");


def read_graph(graph_name):
    df_edges = pd.read_parquet(f"/tmp/{graph_name}.parquet", engine='pyarrow')
    graph = nx.Graph()
    for _, row in df_edges.iterrows():
        graph.add_edge(row['Source'], row['Target'])

    return graph


def store_computation(graph_name, result):
    if isinstance(result, dict):
        df = pd.DataFrame(result, index=[0])
        df.to_parquet(f"data/{graph_name}_result.parquet")

    else:
        raise TypeError("Computation result must be a dictionary. Exiting...")


def main():
    graph = read_graph(graph_name)
    result = worker(graph)
    store_computation(graph_name, result)


if __name__ == "__main__":
    main()

#![allow(dead_code)]
// Functions in here are only used in tests.
// TODO: Find a proper place for this.

use super::graph::Graph;

pub fn make_cyclic_graph(n: u32) -> Graph {
    let vertices: Vec<String> = (0..n).map(|i| format!("V/K{i}")).collect();
    let edges: Vec<(String, String)> = (0..n)
        .map(|i| {
            let from = format!("V/K{}", i);
            let to = format!("V/K{}", (i + 1) % 10);
            (from, to)
        })
        .collect();
    let mut g = Graph::create(vertices, edges);
    g.index_edges(true, false);
    g
}

pub fn make_star_graph(n: u32) -> Graph {
    let vertices: Vec<String> = (0..n).map(|i| format!("V/K{i}")).collect();
    let edges: Vec<(String, String)> = (0..n - 1)
        .map(|i| {
            let to = format!("V/K{}", n - 1);
            let from = format!("V/K{}", i);
            (from, to)
        })
        .collect();
    let mut g = Graph::create(vertices, edges);
    g.index_edges(true, false);
    g
}

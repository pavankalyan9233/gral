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

pub fn make_btree_graph(d: u32) -> Graph {
    // This produces a binary tree of depth d.
    // We will have 2^d-1 vertices numbered from 1 to 2^d-1. Each vertex i
    // has two outgoing edges from i to 2*i and 2*i+1, if i < 2^(d-1):
    let n = 2u64.pow(d) - 1;
    let vertices: Vec<String> = (1..=n).map(|i| format!("V/K{i}")).collect();
    let mut edges: Vec<(String, String)> = Vec::with_capacity(n as usize);
    for i in 1..2u64.pow(d - 1) {
        let from = format!("V/K{}", i);
        let to = format!("V/K{}", 2 * i);
        edges.push((from.clone(), to));
        let to = format!("V/K{}", 2 * i + 1);
        edges.push((from, to));
    }
    let mut g = Graph::create(vertices, edges);
    g.index_edges(true, false);
    g
}
